package ec

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
