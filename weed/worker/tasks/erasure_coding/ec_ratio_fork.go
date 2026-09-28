package erasure_coding

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	ecstorage "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/grpc"
)

// Fork-local addition (not present in upstream SeaweedFS). The configurable EC
// ratio plumbing lives in this file so upstream config.go and plugin_handler.go
// keep only the small hooks that call into it.

// syncECPolicyFromCluster loads the cluster's EC ratio policy (ec.config) from a
// filer in the job's cluster context. A worker process has no filer configured,
// but the admin sends the filer addresses with every job; that is how the policy
// reaches a worker-encoded volume without the worker needing its own config.
// Called where a detection or execution pass starts, so the value is as fresh as
// the pass itself. Absent filers or an absent document leave the registry empty
// and the resolvers fall back to the build default.
func syncECPolicyFromCluster(clusterContext *plugin_pb.ClusterContext, grpcDialOption grpc.DialOption) {
	if clusterContext == nil || len(clusterContext.FilerAddresses) == 0 {
		return
	}
	for _, filer := range clusterContext.FilerAddresses {
		if strings.TrimSpace(filer) == "" {
			continue
		}
		if err := ec.LoadECConfigFromFiler(grpcDialOption, pb.ServerAddress(filer)); err != nil {
			// A missing document is not an error (LoadECConfigFromFiler clears the
			// policy), so reaching here means a filer that exists but could not be
			// read: the encode would silently fall back to the build default, which
			// is worth a warning rather than a debug line.
			glog.Warningf("erasure_coding: could not load the EC policy from filer %s: %v", filer, err)
			continue
		}
		return
	}
}

// ResolveDataShards returns the configured data-shard count for new encodes,
// falling back to the build default when unset (<= 0).
func (c *Config) ResolveDataShards() int {
	return c.ResolveDataShardsFor("")
}

// ResolveParityShards returns the configured parity-shard count for new encodes,
// falling back to the build default when unset (<= 0).
func (c *Config) ResolveParityShards() int {
	return c.ResolveParityShardsFor("")
}

// ResolveDataShardsFor resolves the data-shard count for one collection: an
// explicit job-type config value wins, else the cluster's EC ratio policy for
// that collection (ec.config), else the build default. Matching the enterprise
// edition, the policy is what an unset config means.
func (c *Config) ResolveDataShardsFor(collection string) int {
	if c != nil && c.DataShards > 0 {
		return c.DataShards
	}
	return policyDataShards(collection)
}

// ResolveParityShardsFor is ResolveDataShardsFor for the parity count.
func (c *Config) ResolveParityShardsFor(collection string) int {
	if c != nil && c.ParityShards > 0 {
		return c.ParityShards
	}
	return policyParityShards(collection)
}

// policyDataShards resolves a collection's data-shard count from the cluster EC
// ratio policy (loaded by syncECPolicyFromCluster), else the build default. It
// is the collection-less path's counterpart for callers that only know a
// collection name, such as a job's parameters.
func policyDataShards(collection string) int {
	if policy, found := ecstorage.ResolveECConfig(collection); found {
		return policy.DataShards
	}
	return ecstorage.DataShardsCount
}

// policyParityShards is policyDataShards for the parity count.
func policyParityShards(collection string) int {
	if policy, found := ecstorage.ResolveECConfig(collection); found {
		return policy.ParityShards
	}
	return ecstorage.ParityShardsCount
}

// applyConfiguredEcRatio resolves data_shards/parity_shards from the handler
// config values into taskConfig. A missing or out-of-range value falls back to
// the task defaults rather than failing detection: the pair is handed to
// placement and the Reed-Solomon encoder, so it must stay within the shard-id
// budget.
func applyConfiguredEcRatio(values map[string]*plugin_pb.ConfigValue, taskConfig *Config) {
	dataShards := pluginworker.ReadIntConfig(values, "data_shards", taskConfig.DataShards)
	if dataShards < 1 {
		dataShards = taskConfig.DataShards
	}
	parityShards := pluginworker.ReadIntConfig(values, "parity_shards", taskConfig.ParityShards)
	if parityShards < 1 {
		parityShards = taskConfig.ParityShards
	}
	if dataShards+parityShards > ecstorage.MaxShardCount {
		glog.Warningf("erasure_coding: configured ratio %d+%d exceeds %d total shards, using %d+%d",
			dataShards, parityShards, ecstorage.MaxShardCount, taskConfig.DataShards, taskConfig.ParityShards)
		dataShards, parityShards = taskConfig.DataShards, taskConfig.ParityShards
	}
	taskConfig.DataShards = dataShards
	taskConfig.ParityShards = parityShards
}
