package ecvacuum

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// loadOptsFromVif records one EC layout in a .vif and loads it back the way the
// vacuum does.
func loadOptsFromVif(t *testing.T, cfg *volume_server_pb.EcShardConfig) (Options, error) {
	t.Helper()
	dir := t.TempDir()
	base := ShardBaseName("c1", 7)
	require.NoError(t, volume_info.SaveVolumeInfo(filepath.Join(dir, base+".vif"), &volume_server_pb.VolumeInfo{
		EcShardConfig: cfg,
	}))
	return LoadVacuumOptions(dir, base, VacuumTarget{
		VolumeID: 7, Collection: "c1", DataShards: 10, ParityShards: 4, DiskType: "hdd",
	})
}

// TestLoadVacuumOptionsUsesAValidRecordedLayout verifies the .vif stays the
// authority for the ratio and the block layout on the happy path.
func TestLoadVacuumOptionsUsesAValidRecordedLayout(t *testing.T) {
	opts, err := loadOptsFromVif(t, &volume_server_pb.EcShardConfig{
		DataShards: 3, ParityShards: 2, BlockSize: erasure_coding.ErasureCodingSmallBlockSize,
	})
	require.NoError(t, err)
	require.Equal(t, 3, opts.DataShards)
	require.Equal(t, 2, opts.ParityShards)
	require.EqualValues(t, erasure_coding.ErasureCodingSmallBlockSize, opts.BlockSize)
}

// TestLoadVacuumOptionsRejectsAnImpossibleRecordedRatio verifies a .vif that
// records a ratio no encoder could have produced is refused rather than replaced.
// The shards were laid out by the recorded matrix, so reading them back through a
// different one returns wrong bytes.
func TestLoadVacuumOptionsRejectsAnImpossibleRecordedRatio(t *testing.T) {
	_, err := loadOptsFromVif(t, &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 0})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid shard counts 10+0")
}

// TestLoadVacuumOptionsRejectsAnUnalignedBlockSize verifies the recorded block size
// is validated, as the mount and rebuild paths do. An unaligned size maps every
// read to the wrong shard offset, and the damage is self-consistent — the
// re-encode verify decodes back through the same layout — so it would otherwise be
// published to the holders.
func TestLoadVacuumOptionsRejectsAnUnalignedBlockSize(t *testing.T) {
	_, err := loadOptsFromVif(t, &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4, BlockSize: 3})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid shard block size 3")
}

// TestLoadVacuumOptionsKeepsTheTopologyRatioWhenNoneIsRecorded verifies a .vif that
// carries an EC config without counts still falls back to the topology's ratio:
// "nothing recorded" is not the same as "recorded something impossible".
func TestLoadVacuumOptionsKeepsTheTopologyRatioWhenNoneIsRecorded(t *testing.T) {
	opts, err := loadOptsFromVif(t, &volume_server_pb.EcShardConfig{})
	require.NoError(t, err)
	require.Equal(t, 10, opts.DataShards)
	require.Equal(t, 4, opts.ParityShards)
	require.Zero(t, opts.BlockSize)
}
