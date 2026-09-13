package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// Fork-local addition (not present in upstream SeaweedFS). Per-volume EC ratio
// accessors, kept in their own file so upstream edits to the ec_volume_* core do
// not conflict with them.

// ECDataShards returns the number of data shards this volume was encoded with,
// or 0 when the ratio is not yet known (ECContext unset). Callers treat 0 as
// "use the build default".
func (ev *EcVolume) ECDataShards() int {
	if ev == nil || ev.ECContext == nil {
		return 0
	}
	return ev.ECContext.DataShards
}

// ECParityShards returns the number of parity shards this volume was encoded
// with, or 0 when unknown.
func (ev *EcVolume) ECParityShards() int {
	if ev == nil || ev.ECContext == nil {
		return 0
	}
	return ev.ECContext.ParityShards
}

// ShardRatioOrDefault returns the volume's (data, parity) shard counts. A
// recorded ratio is used only when it is non-zero and internally consistent
// (ValidEcShardCounts); otherwise the build default is returned, so a 0/0 pair
// from an older volume reads as the standard 10+4 layout.
func (ecInfo *EcVolumeInfo) ShardRatioOrDefault() (dataShards, parityShards int) {
	if ecInfo != nil && ecInfo.DataShards > 0 && ecInfo.ParityShards > 0 &&
		ValidEcShardCounts(uint32(ecInfo.DataShards), uint32(ecInfo.ParityShards)) {
		return ecInfo.DataShards, ecInfo.ParityShards
	}
	return DataShardsCount, ParityShardsCount
}

// ParityShardsOrDefault returns the volume's parity-shard count, falling back to
// the build default when the recorded ratio is unset or invalid.
func (ecInfo *EcVolumeInfo) ParityShardsOrDefault() int {
	_, parityShards := ecInfo.ShardRatioOrDefault()
	return parityShards
}

// TotalShardsOrDefault returns the volume's data+parity shard count.
func (ecInfo *EcVolumeInfo) TotalShardsOrDefault() int {
	dataShards, parityShards := ecInfo.ShardRatioOrDefault()
	return dataShards + parityShards
}

// ecShardRatiosFromMessage returns the volume's (data, parity) shard counts from
// the message. A recorded ratio is honored only when both counts are non-zero and
// internally consistent (ValidEcShardCounts); otherwise the build default is
// returned, so a message from a volume that predates ratio tracking reads as the
// standard 10+4 layout.
func ecShardRatiosFromMessage(vi *master_pb.VolumeEcShardInformationMessage) (dataShards, parityShards int) {
	if vi != nil && vi.GetDataShards() > 0 && vi.GetParityShards() > 0 &&
		ValidEcShardCounts(vi.GetDataShards(), vi.GetParityShards()) {
		return int(vi.GetDataShards()), int(vi.GetParityShards())
	}
	return DataShardsCount, ParityShardsCount
}

// EcShardsVolumeTotalShards returns the number of data+parity shards for the EC
// volume described by vi (the count a complete volume should present).
func EcShardsVolumeTotalShards(vi *master_pb.VolumeEcShardInformationMessage) int {
	dataShards, parityShards := ecShardRatiosFromMessage(vi)
	return dataShards + parityShards
}

// ShardsPerVolumeSlot returns how many EC shards of the volumes described by
// infos occupy one volume slot — i.e. their data-shard count. Volumes in a
// cluster normally share one ratio, so it returns the first valid ratio found;
// when none is reported (an empty disk, or pre-upgrade volumes) it returns the
// build default. Callers that only see a set of shards use it to convert between
// EC shard counts and volume slots, so a 3+2 disk is not sized as a 10+4 one.
func ShardsPerVolumeSlot(infos []*master_pb.VolumeEcShardInformationMessage) int {
	for _, vi := range infos {
		if vi != nil && vi.GetDataShards() > 0 && vi.GetParityShards() > 0 &&
			ValidEcShardCounts(vi.GetDataShards(), vi.GetParityShards()) {
			return int(vi.GetDataShards())
		}
	}
	return DataShardsCount
}
