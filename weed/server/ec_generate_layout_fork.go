package weed_server

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// Fork-local addition (not present in upstream SeaweedFS). The layout a
// VolumeEcShardsGenerate call encodes with, resolved in one place so the
// precedence is testable:
//
//  1. the caller's EcShardConfig — the cluster's EC ratio policy (ec.config),
//     which is how a volume that has no layout recorded yet gets the policy's;
//  2. the layout an existing .vif records — a volume encoded before the policy
//     existed, or a caller that predates this field;
//  3. the build default (10+4).
//
// A request naming an impossible layout is an error rather than a silent fall
// through to a different one: encoding a volume at a ratio nobody chose is the
// failure this whole path exists to prevent.
const (
	ecLayoutFromRequest = "requested"
	ecLayoutFromVif     = "existing .vif"
	ecLayoutFromDefault = "build default"
)

func resolveGenerateEcLayout(volumeId needle.VolumeId, collection string, requested *volume_server_pb.EcShardConfig, vifPath string) (*erasure_coding.ECContext, string, error) {
	ecCtx := erasure_coding.NewDefaultECContext(collection, volumeId)

	if requested != nil && (requested.DataShards != 0 || requested.ParityShards != 0) {
		if !erasure_coding.ValidEcShardCounts(requested.DataShards, requested.ParityShards) {
			return nil, "", fmt.Errorf("volume %d: invalid requested EC config %d+%d (both counts must be positive and the total must not exceed %d)",
				volumeId, requested.DataShards, requested.ParityShards, erasure_coding.MaxShardCount)
		}
		ecCtx.DataShards = int(requested.DataShards)
		ecCtx.ParityShards = int(requested.ParityShards)
		return ecCtx, ecLayoutFromRequest, nil
	}

	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(vifPath); found && volumeInfo.EcShardConfig != nil {
		ds := int(volumeInfo.EcShardConfig.DataShards)
		ps := int(volumeInfo.EcShardConfig.ParityShards)
		if ds > 0 && ps > 0 && ds+ps <= erasure_coding.MaxShardCount {
			ecCtx.DataShards = ds
			ecCtx.ParityShards = ps
			return ecCtx, ecLayoutFromVif, nil
		}
		glog.Warningf("Invalid EC config in .vif for volume %d (data=%d, parity=%d), using defaults", volumeId, ds, ps)
	}

	return ecCtx, ecLayoutFromDefault, nil
}
