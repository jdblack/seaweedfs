package dash

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// Fork-local addition (not present in upstream SeaweedFS). Kept in its own file
// so upstream edits to ec_shard_management.go do not conflict with it.

// ecVolumeRatio is a volume's EC shard split. total is data+parity — the count a
// complete volume should present. Unset ratios resolve to the build default
// (10+4) through the erasure_coding accessors.
type ecVolumeRatio struct {
	dataShards   int
	parityShards int
	total        int
}

// ecShardRatio reads a volume's data/parity/total shard counts from its EC
// shard-information message, defaulting to the build ratio when it carries none.
func ecShardRatio(vi *master_pb.VolumeEcShardInformationMessage) ecVolumeRatio {
	dataShards := erasure_coding.EcShardsVolumeDataShards(vi)
	parityShards := erasure_coding.EcShardsVolumeParityShards(vi)
	return ecVolumeRatio{
		dataShards:   dataShards,
		parityShards: parityShards,
		total:        dataShards + parityShards,
	}
}
