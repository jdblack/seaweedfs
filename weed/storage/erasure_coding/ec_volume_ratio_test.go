package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestVolumeEcShardInformationMessageFieldNumbers pins the wire numbers the
// OSS/enterprise ratio contract depends on. Renumbering these silently rewrites
// the on-the-wire layout and can collide with the enterprise fork, so this fails
// loudly instead. Numbers confirmed against the enterprise fork's compiled
// descriptor: data_shards=20, parity_shards=21.
func TestVolumeEcShardInformationMessageFieldNumbers(t *testing.T) {
	fields := (&master_pb.VolumeEcShardInformationMessage{}).ProtoReflect().Descriptor().Fields()
	for _, want := range []struct {
		name   protoreflect.Name
		number int
	}{
		{"encode_ts_ns", 14},
		{"data_shards", 20},
		{"parity_shards", 21},
	} {
		fd := fields.ByName(want.name)
		if fd == nil {
			t.Fatalf("field %q is missing from VolumeEcShardInformationMessage", want.name)
		}
		if int(fd.Number()) != want.number {
			t.Errorf("field %q is number %d, want %d (changing it rewrites the wire format / breaks enterprise interop)",
				want.name, fd.Number(), want.number)
		}
	}
}

// TestEcShardsVolumeRatioAccessors covers the per-volume ratio accessors that
// replaced the hardcoded 10+4 assumption. An unset (0/0) or invalid ratio must
// still read as the build default so pre-upgrade volumes stay correct.
func TestEcShardsVolumeRatioAccessors(t *testing.T) {
	cases := []struct {
		name         string
		vi           *master_pb.VolumeEcShardInformationMessage
		data, parity int
	}{
		{"nil uses build default", nil, DataShardsCount, ParityShardsCount},
		{"unset uses build default", &master_pb.VolumeEcShardInformationMessage{}, DataShardsCount, ParityShardsCount},
		{"3+2 honored", &master_pb.VolumeEcShardInformationMessage{DataShards: 3, ParityShards: 2}, 3, 2},
		{"16+6 honored", &master_pb.VolumeEcShardInformationMessage{DataShards: 16, ParityShards: 6}, 16, 6},
		{"partial ratio falls back", &master_pb.VolumeEcShardInformationMessage{DataShards: 3}, DataShardsCount, ParityShardsCount},
		{"over budget falls back", &master_pb.VolumeEcShardInformationMessage{DataShards: 30, ParityShards: 6}, DataShardsCount, ParityShardsCount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.data, EcShardsVolumeDataShards(c.vi))
			assert.Equal(t, c.parity, EcShardsVolumeParityShards(c.vi))
			assert.Equal(t, c.data+c.parity, EcShardsVolumeTotalShards(c.vi))
		})
	}
}

func TestEcVolumeInfoShardRatioOrDefault(t *testing.T) {
	assert.Equal(t, DataShardsCount, (&EcVolumeInfo{}).DataShardsOrDefault())
	assert.Equal(t, ParityShardsCount, (*EcVolumeInfo)(nil).ParityShardsOrDefault())
	assert.Equal(t, TotalShardsCount, (&EcVolumeInfo{DataShards: 0, ParityShards: 0}).TotalShardsOrDefault())

	custom := &EcVolumeInfo{DataShards: 3, ParityShards: 2}
	assert.Equal(t, 3, custom.DataShardsOrDefault())
	assert.Equal(t, 2, custom.ParityShardsOrDefault())
	assert.Equal(t, 5, custom.TotalShardsOrDefault())
}

// TestEcVolumeInfoToMessageCarriesRatio locks the wire path the admin depends
// on: the per-volume ratio must survive the master-side -> message conversion.
func TestEcVolumeInfoToMessageCarriesRatio(t *testing.T) {
	info := &EcVolumeInfo{VolumeId: 9, Collection: "c", DataShards: 3, ParityShards: 2, ShardsInfo: NewShardsInfo()}
	msg := info.ToVolumeEcShardInformationMessage()
	assert.Equal(t, uint32(3), msg.GetDataShards())
	assert.Equal(t, uint32(2), msg.GetParityShards())
	assert.Equal(t, 5, EcShardsVolumeTotalShards(msg))
}

// TestShardsPerVolumeSlot covers the shard<->volume-slot helper: a set of
// volumes' own ratio is honored, an unset or invalid set falls back to the build
// default (so an empty disk or pre-upgrade volumes keep the standard layout).
func TestShardsPerVolumeSlot(t *testing.T) {
	assert.Equal(t, DataShardsCount, ShardsPerVolumeSlot(nil))
	assert.Equal(t, DataShardsCount, ShardsPerVolumeSlot([]*master_pb.VolumeEcShardInformationMessage{{}}))
	assert.Equal(t, 3, ShardsPerVolumeSlot([]*master_pb.VolumeEcShardInformationMessage{
		{DataShards: 3, ParityShards: 2},
		{DataShards: 10, ParityShards: 4},
	}))
	// An invalid entry is skipped in favour of a later valid one.
	assert.Equal(t, 3, ShardsPerVolumeSlot([]*master_pb.VolumeEcShardInformationMessage{
		{DataShards: 0, ParityShards: 2},
		{DataShards: 3, ParityShards: 2},
	}))
}

// TestEcVolumeInfoMinusKeepsRatio ensures the shard-diff used by heartbeats does
// not drop the ratio from the resulting entry.
func TestEcVolumeInfoMinusKeepsRatio(t *testing.T) {
	a := &EcVolumeInfo{DataShards: 3, ParityShards: 2, ShardsInfo: NewShardsInfo()}
	a.ShardsInfo.Set(NewShardInfo(0, 10))
	b := &EcVolumeInfo{DataShards: 3, ParityShards: 2, ShardsInfo: NewShardsInfo()}
	got := a.Minus(b)
	assert.Equal(t, 3, got.DataShards)
	assert.Equal(t, 2, got.ParityShards)
}
