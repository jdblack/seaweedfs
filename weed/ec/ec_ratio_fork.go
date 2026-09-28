package ec

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

// Fork-local addition (not present in upstream SeaweedFS). Per-volume EC ratio
// lookups used by the encode/decode/rebuild paths, which would otherwise assume
// the build's 10+4 and mis-handle a volume encoded with a custom ratio.

// ecVolumeShardRatio returns the (data, parity) shard counts recorded for a
// volume on its EC shard-information messages in the topology. It defaults to
// the build ratio when no holder reports one, so a pre-upgrade volume stays
// correct.
func ecVolumeShardRatio(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId) (dataShards, parityShards int) {
	EachDataNode(topoInfo, func(_ DataCenterId, _ RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			if diskInfo == nil {
				continue
			}
			for _, v := range diskInfo.EcShardInfos {
				if v.Id == uint32(vid) && dataShards == 0 {
					dataShards = erasure_coding.EcShardsVolumeDataShards(v)
					parityShards = erasure_coding.EcShardsVolumeParityShards(v)
				}
			}
		}
	})
	if dataShards == 0 || parityShards == 0 {
		return erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount
	}
	return
}

// volumeShardRatio returns the (data, parity) shard counts recorded for a volume
// on any of the rebuilder's nodes, defaulting to the build ratio when none is
// reported. Lets the rebuilder judge completeness against the volume's real
// ratio instead of assuming 10+4.
func (erb *ecRebuilder) volumeShardRatio(volumeId needle.VolumeId) (dataShards, parityShards int) {
	for _, node := range erb.ecNodes {
		for _, diskInfo := range node.Info.DiskInfos {
			if diskInfo == nil {
				continue
			}
			for _, v := range diskInfo.EcShardInfos {
				if v.Id == uint32(volumeId) {
					return erasure_coding.EcShardsVolumeDataShards(v), erasure_coding.EcShardsVolumeParityShards(v)
				}
			}
		}
	}
	return erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount
}

// generatedLayoutShardIds returns the shard ids of the layout a holder serves
// for this volume — 0 .. data+parity-1 from the .vif it mounted with — or the
// build's 10+4 list when the holder reports no layout (a binary predating the
// field) or cannot be asked, which leaves a default-ratio cluster on the path
// it has always taken.
func generatedLayoutShardIds(dialOption grpc.DialOption, volumeId needle.VolumeId, target pb.ServerAddress) []erasure_coding.ShardId {
	return shardIdsFromHeldLayout(heldEcLayout(dialOption, volumeId, target))
}

// shardIdsFromHeldLayout turns a holder's reported layout into the shard ids to
// mount, or the build's list when the holder reported none.
func shardIdsFromHeldLayout(held *volume_server_pb.EcShardConfig) []erasure_coding.ShardId {
	if held == nil {
		return erasure_coding.AllShardIds()
	}
	total := int(held.DataShards) + int(held.ParityShards)
	shardIds := make([]erasure_coding.ShardId, total)
	for i := range shardIds {
		shardIds[i] = erasure_coding.ShardId(i)
	}
	return shardIds
}

// heldEcLayout asks a holder which layout it serves for this volume, after at
// least one shard is mounted (that is what makes the holder answer). It returns
// nil when the holder reports nothing usable: a binary predating the field, a
// volume not mounted, or an unreachable node — all of which keep the caller on
// the build default instead of guessing a layout.
func heldEcLayout(dialOption grpc.DialOption, volumeId needle.VolumeId, target pb.ServerAddress) *volume_server_pb.EcShardConfig {
	var held *volume_server_pb.EcShardConfig
	err := operation.WithVolumeServerClient(false, target, dialOption, func(client volume_server_pb.VolumeServerClient) error {
		resp, infoErr := client.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{
			VolumeId: uint32(volumeId),
		})
		if infoErr != nil {
			return infoErr
		}
		cfg := resp.GetEcShardConfig()
		if cfg == nil || cfg.GetDataShards() == 0 || cfg.GetParityShards() == 0 {
			return nil
		}
		if int(cfg.GetDataShards())+int(cfg.GetParityShards()) > erasure_coding.MaxShardCount {
			return nil
		}
		held = cfg
		return nil
	})
	if err != nil {
		glog.V(1).Infof("ec volume %d on %s: cannot read the generated layout (%v); assuming the default %d+%d",
			volumeId, target, err, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
		return nil
	}
	return held
}

// mountGeneratedEcShards brings the EC layout a volume server has just generated
// online, on the server that generated it. The build's 0..13 list cannot be
// used: a volume whose .vif records a custom ratio (e.g. 3+2) generated only
// that many shards, so mounting 0..13 fails at the first id past it with
// "MountEcShards <vid>.<n> not found on disk" and the encode rolls back.
// Shard 0 exists in every layout (there is at least one data shard) and
// mounting it is what makes the holder answer VolumeEcShardsInfo with the ratio
// it generated with, so mount that first, read the layout, mount the rest.
//
// When an EC ratio policy covers this collection, the layout the holder reports
// must be the policy's. A holder that ignored the requested layout — a volume
// server predating this fork's generate field — would otherwise be mounted,
// rebalanced and have its source deleted while sitting at a ratio nobody chose.
func mountGeneratedEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, target pb.ServerAddress) error {
	if err := MountEcShards(grpcDialOption, collection, volumeId, target, []erasure_coding.ShardId{0}); err != nil {
		return err
	}
	held := heldEcLayout(grpcDialOption, volumeId, target)
	if policy, found := erasure_coding.ResolveECConfig(collection); found {
		if held != nil && (int(held.DataShards) != policy.DataShards || int(held.ParityShards) != policy.ParityShards) {
			return fmt.Errorf("volume %d on %s generated %d+%d but the EC policy for collection %q is %d+%d; refusing to bring it online at a ratio nobody chose (upgrade the volume server so it honors the requested layout, or align the policy)",
				volumeId, target, held.DataShards, held.ParityShards, collection, policy.DataShards, policy.ParityShards)
		}
	}
	shardIds := shardIdsFromHeldLayout(held)
	if len(shardIds) <= 1 {
		return nil
	}
	return MountEcShards(grpcDialOption, collection, volumeId, target, shardIds[1:])
}

// ecLayoutTotalShards returns how many shards a volume of this collection will
// have: the EC policy's total when one covers the collection, else the build
// default. The capacity pre-flight uses it so a 3+2 cluster is not charged 14
// slots per volume (which used to print a capacity warning that could not be
// acted on).
func ecLayoutTotalShards(collection string) int {
	if policy, found := erasure_coding.ResolveECConfig(collection); found {
		return policy.Total()
	}
	return erasure_coding.TotalShardsCount
}
