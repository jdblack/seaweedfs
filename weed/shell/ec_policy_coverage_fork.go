package shell

import (
	"fmt"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// Fork-local addition (not present in upstream SeaweedFS). Whether the volumes
// about to be encoded are covered by the cluster EC ratio policy (ec.config).
//
// The shell already discovers a filer from the master when -filer is empty
// (RunShell, shell_liner.go), so the policy normally loads without the operator
// naming a filer. Discovery can still come up empty — no filer registered with
// the master, or a -filerGroup that matches none of them — and then the run
// carries no policy at all: each volume falls back to the layout its own .vif
// records and finally to the build default. That fallback is legitimate (a
// cluster without a filer behaves exactly as it did before the policy existed),
// but it is also how a volume ends up at 10+4 by accident, so ec.encode says so
// before encoding rather than after.

// ecPolicyCoverageWarning returns the line ec.encode prints when some of the
// volumes it is about to encode have no policy covering them, and "" when every
// one of them is covered (or when there is nothing to encode).
func ecPolicyCoverageWarning(filerAddress pb.ServerAddress, topologyInfo *master_pb.TopologyInfo, volumeIds []needle.VolumeId) string {
	if topologyInfo == nil || len(volumeIds) == 0 {
		return ""
	}

	uncovered := map[string]int{}
	uncoveredTotal := 0
	idToCollection := ec.CollectVolumeIdToCollection(topologyInfo, volumeIds)
	for _, vid := range volumeIds {
		collection := idToCollection[vid]
		if _, found := erasure_coding.ResolveECConfig(collection); found {
			continue
		}
		uncovered[collection]++
		uncoveredTotal++
	}
	if uncoveredTotal == 0 {
		return ""
	}

	names := make([]string, 0, len(uncovered))
	for collection, count := range uncovered {
		if collection == "" {
			collection = "(no collection)"
		}
		names = append(names, fmt.Sprintf("%s (%d volumes)", collection, count))
	}
	sort.Strings(names)

	where := fmt.Sprintf("no EC ratio policy covers these volumes (filer %s)", filerAddress)
	if filerAddress == "" {
		where = "no filer discovered (-filer empty, master lists none): no EC ratio policy was loaded"
	}

	return fmt.Sprintf(
		"warning: %s\n"+
			"  %d of %d volume(s) have no policy: %s\n"+
			"  each is encoded with the layout in its own .vif, else the build default %d+%d\n"+
			"  set a policy with: ec.config -set -dataShards=<n> -parityShards=<m>\n",
		where, uncoveredTotal, len(volumeIds), strings.Join(names, ", "),
		erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
}
