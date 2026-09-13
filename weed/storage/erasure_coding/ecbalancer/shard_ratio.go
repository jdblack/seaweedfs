package ecbalancer

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// shardDataShards returns the data-shard count of the volume an EC shard belongs
// to, used to size the shard's disk footprint. It reads the volume's own ratio
// (data_shards, field 20 of the EC shard heartbeat) and falls back to the build
// default when the volume predates ratio tracking.
func shardDataShards(eci *master_pb.VolumeEcShardInformationMessage) int {
	return erasure_coding.EcShardsVolumeDataShards(eci)
}

// VolumeShardRatio returns the RAW per-volume (dataShards, parityShards) reported
// on an EC shard's heartbeat, with 0 meaning "not reported" so the balancer falls
// back to the collection ratio (the standard scheme). The proto now carries
// data_shards/parity_shards at fields 20/21 (matching the enterprise fork),
// populated from the .vif; a pre-upgrade volume reports 0, 0 and keeps the
// collection-keyed behavior.
func VolumeShardRatio(eci *master_pb.VolumeEcShardInformationMessage) (dataShards, parityShards int) {
	if eci == nil {
		return 0, 0
	}
	return int(eci.GetDataShards()), int(eci.GetParityShards())
}
