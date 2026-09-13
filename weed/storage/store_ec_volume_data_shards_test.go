package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestEcShardVolumeDataShardsResolvesFromVif covers the free-slot maths EC shard
// placement uses: the volume server holds no cluster EC config, so a volume's
// data-shard count must come from its .vif (a mounted EcVolume would win, but
// there is none here), falling back to the build default when there is no .vif.
func TestEcShardVolumeDataShardsResolvesFromVif(t *testing.T) {
	const dataShards, parityShards = 3, 2
	const collection = "ectest"
	const vid = needle.VolumeId(42)

	dir := t.TempDir()
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	if err := volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version: uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   dataShards,
			ParityShards: parityShards,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}

	store := &Store{Locations: []*DiskLocation{
		NewDiskLocation(dir, 100, util.MinFreeSpace{}, "", types.HardDriveType, nil, stats.DefaultDiskIOProbeConfig()),
	}}

	if got := store.EcShardVolumeDataShards(collection, vid); got != dataShards {
		t.Errorf("EcShardVolumeDataShards from .vif = %d, want %d", got, dataShards)
	}
	if got := store.EcShardVolumeDataShards(collection, needle.VolumeId(999)); got != erasure_coding.DataShardsCount {
		t.Errorf("EcShardVolumeDataShards with no .vif = %d, want build default %d", got, erasure_coding.DataShardsCount)
	}
}
