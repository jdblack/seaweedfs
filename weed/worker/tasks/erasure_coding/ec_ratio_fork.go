package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	ecstorage "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// Fork-local addition (not present in upstream SeaweedFS). The configurable EC
// ratio plumbing lives in this file so upstream config.go and plugin_handler.go
// keep only the small hooks that call into it.

// ResolveDataShards returns the configured data-shard count for new encodes,
// falling back to the build default when unset (<= 0).
func (c *Config) ResolveDataShards() int {
	if c == nil || c.DataShards <= 0 {
		return ecstorage.DataShardsCount
	}
	return c.DataShards
}

// ResolveParityShards returns the configured parity-shard count for new encodes,
// falling back to the build default when unset (<= 0).
func (c *Config) ResolveParityShards() int {
	if c == nil || c.ParityShards <= 0 {
		return ecstorage.ParityShardsCount
	}
	return c.ParityShards
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
