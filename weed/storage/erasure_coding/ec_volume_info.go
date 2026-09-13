package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// data structure used in master
type EcVolumeInfo struct {
	VolumeId    needle.VolumeId
	Collection  string
	DiskType    string
	DiskId      uint32 // ID of the disk this EC volume is on
	ExpireAtSec uint64 // ec volume destroy time, calculated from the ec volume was created
	ShardsInfo  *ShardsInfo
	FileCount   uint64 // live needle count for this EC volume (same on every node holding shards)
	DeleteCount uint64 // tombstoned needle count for this EC volume
	EncodeTsNs  int64  // encode-run identity (unix nanos); one value per (volume, disk)
	// DataShards/ParityShards are this volume's Reed-Solomon ratio as recorded in
	// the .vif EcShardConfig. 0 means the volume predates the ratio being tracked;
	// readers then fall back to the build default (10+4), which keeps pre-upgrade
	// volumes correct. Carried per (volume, disk) entry like the other fields.
	DataShards   int
	ParityShards int
}

// DataShardsOrDefault returns how many of this volume's shards hold data; shard
// ids below it are data, the rest are parity. It is a per-volume accessor, like
// EcShardsVolumeDataShards, so callers stay correct when the ratio is derived
// per volume rather than from the global constant.
func (ecInfo *EcVolumeInfo) DataShardsOrDefault() int {
	dataShards, _ := ecInfo.ShardRatioOrDefault()
	return dataShards
}

func (ecInfo *EcVolumeInfo) Minus(other *EcVolumeInfo) *EcVolumeInfo {
	return &EcVolumeInfo{
		VolumeId:     ecInfo.VolumeId,
		Collection:   ecInfo.Collection,
		ShardsInfo:   ecInfo.ShardsInfo.Minus(other.ShardsInfo),
		DiskType:     ecInfo.DiskType,
		DiskId:       ecInfo.DiskId,
		ExpireAtSec:  ecInfo.ExpireAtSec,
		FileCount:    ecInfo.FileCount,
		DeleteCount:  ecInfo.DeleteCount,
		EncodeTsNs:   ecInfo.EncodeTsNs,
		DataShards:   ecInfo.DataShards,
		ParityShards: ecInfo.ParityShards,
	}
}

func (evi *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret *master_pb.VolumeEcShardInformationMessage) {
	return &master_pb.VolumeEcShardInformationMessage{
		Id:           uint32(evi.VolumeId),
		EcIndexBits:  evi.ShardsInfo.Bitmap(),
		ShardSizes:   evi.ShardsInfo.SizesInt64(),
		Collection:   evi.Collection,
		DiskType:     evi.DiskType,
		ExpireAtSec:  evi.ExpireAtSec,
		DiskId:       evi.DiskId,
		FileCount:    evi.FileCount,
		DeleteCount:  evi.DeleteCount,
		EncodeTsNs:   evi.EncodeTsNs,
		DataShards:   uint32(evi.DataShards),
		ParityShards: uint32(evi.ParityShards),
	}
}

func (evi *EcVolumeInfo) GetCollection() string {
	return evi.Collection
}

func (evi *EcVolumeInfo) GetVolumeId() needle.VolumeId {
	return evi.VolumeId
}

func (evi *EcVolumeInfo) GetRemoteStorageName() string {
	return ""
}
