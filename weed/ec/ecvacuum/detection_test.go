package ecvacuum

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// ecShardEntry builds one holder's shard entry for an EC volume. Every shard of a
// volume is the same length, as the encoder guarantees, and all 14 (10+4) are
// present, so the entry clears the validity gates and a test can focus on size.
func ecShardEntry(vid uint32, shardSize int64, fileCount, deleteCount uint64) *master_pb.VolumeEcShardInformationMessage {
	sizes := make([]int64, 14)
	for i := range sizes {
		sizes[i] = shardSize
	}
	return &master_pb.VolumeEcShardInformationMessage{
		Id:           vid,
		Collection:   "c1",
		EcIndexBits:  0x3FFF,
		DataShards:   10,
		ParityShards: 4,
		FileCount:    fileCount,
		DeleteCount:  deleteCount,
		EncodeTsNs:   1,
		ShardSizes:   sizes,
	}
}

func oneVolumeTopo(entries ...*master_pb.VolumeEcShardInformationMessage) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "r1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id:      "n1",
					Address: "127.0.0.1:8080",
					DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {EcShardInfos: entries},
					},
				}},
			}},
		}},
	}
}

// TestBuildVacuumTargetRefusesDuplicateShardCopy verifies a volume whose shard id
// lives on two nodes is refused rather than vacuumed. shardHolders keeps only the
// first holder for an id, so the second copy's delete journal would never be folded
// in and never cleared — its deletes would silently vanish from the new generation.
func TestBuildVacuumTargetRefusesDuplicateShardCopy(t *testing.T) {
	topo := twoNodeTopo(
		ecShardEntry(7, 1<<20, 100, 10), // node 1: every shard, with 10 deletes
		ecShardEntry(7, 1<<20, 100, 0),  // node 2: a surplus copy of every shard
	)

	_, err := BuildVacuumTarget(topo, 7, "c1", "hdd")
	require.Error(t, err)
	require.Contains(t, err.Error(), "more than one copy")
}

// TestBuildVacuumTargetAcceptsSplitShards is the guard on the guard: the normal
// case — one copy of each shard, split across nodes — must still build a target.
func TestBuildVacuumTargetAcceptsSplitShards(t *testing.T) {
	first, second := ecShardEntry(7, 1<<20, 100, 10), ecShardEntry(7, 1<<20, 100, 0)
	first.EcIndexBits = 0x000F // shards 0-3
	second.EcIndexBits = 0x3FF0

	target, err := BuildVacuumTarget(twoNodeTopo(first, second), 7, "c1", "hdd")
	require.NoError(t, err)
	require.Len(t, target.Holders, 14, "every shard id has exactly one holder")
}

// twoNodeTopo puts one volume's shard entries on two nodes.
func twoNodeTopo(first, second *master_pb.VolumeEcShardInformationMessage) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "r1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{Id: "n1", Address: "127.0.0.1:8080", DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{first}},
					}},
					{Id: "n2", Address: "127.0.0.1:8081", DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{second}},
					}},
				},
			}},
		}},
	}
}

// TestCandidateReportsEncodedAndLiveBytes verifies a candidate carries both the
// volume's encode-time size and that size with the delete journals folded in — the
// pair the decode threshold is measured against.
func TestCandidateReportsEncodedAndLiveBytes(t *testing.T) {
	// 1 MB per shard * 10 data shards = 10 MB encoded, half the needles deleted.
	c, ok := candidateForGroup(7, "c1", "hdd", []*master_pb.VolumeEcShardInformationMessage{
		ecShardEntry(7, 1<<20, 100, 50),
	})
	require.True(t, ok)
	require.True(t, c.SizeReported)
	require.Equal(t, int64(10<<20), c.DataBytes)
	require.Equal(t, int64(5<<20), c.LiveBytes, "half the needles are gone")
}

// TestCandidateWithoutShardSizesDoesNotClaimEmpty verifies that a holder reporting
// no shard size leaves the candidate unsized rather than reporting zero bytes:
// shard_sizes is an optional heartbeat field, so zero from a silent holder says
// nothing about whether the volume still holds data.
func TestCandidateWithoutShardSizesDoesNotClaimEmpty(t *testing.T) {
	entry := ecShardEntry(7, 1<<20, 100, 100)
	entry.ShardSizes = nil

	c, ok := candidateForGroup(7, "c1", "hdd", []*master_pb.VolumeEcShardInformationMessage{entry})
	require.True(t, ok, "an unsized volume is still a candidate; only its size is unknown")
	require.False(t, c.SizeReported)
	require.Zero(t, c.DataBytes)
	require.Zero(t, c.LiveBytes)
}

// TestEnumerateDecodeCandidatesSelectsBelowThreshold verifies the fullness gate, and
// that the emptiest volume is proposed first.
func TestEnumerateDecodeCandidatesSelectsBelowThreshold(t *testing.T) {
	topo := oneVolumeTopo(
		ecShardEntry(7, 1<<20, 100, 50), // 10 MB encoded, 5 MB live -> 12.5%
		ecShardEntry(8, 3<<20, 100, 30), // 30 MB encoded, 21 MB live -> 52.5%
	)

	// 40 MB limit at 50% -> 20 MB.
	got, err := EnumerateDecodeCandidates(topo, "", 0.50, 40)
	require.NoError(t, err)
	require.Len(t, got, 1, "only the volume under the threshold")
	require.Equal(t, uint32(7), got[0].VolumeID)
	require.Equal(t, int64(5<<20), got[0].LiveBytes)
}

// TestEnumerateDecodeCandidatesSkipsUnreportedSize verifies that an unsized volume
// is not read as empty, and that the skip is counted: without the counter, a cluster
// whose volume servers report no shard sizes looks like a cluster with nothing to
// decode.
func TestEnumerateDecodeCandidatesSkipsUnreportedSize(t *testing.T) {
	entry := ecShardEntry(7, 1<<20, 100, 100) // fully deleted, but no sizes reported
	entry.ShardSizes = nil

	// The counters are process-wide, so snapshot instead of assuming a value.
	unsized := stats.ECVacuumJobsSkippedCounter.WithLabelValues("unsized")
	before := testutil.ToFloat64(unsized)

	got, err := EnumerateDecodeCandidates(oneVolumeTopo(entry), "", 0.50, 40)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, before+1, testutil.ToFloat64(unsized))
}

// TestEnumerateDecodeCandidatesNeedsVolumeSizeLimit verifies the volume size limit is
// required: fullness is a fraction of it, so there is no honest answer without it.
func TestEnumerateDecodeCandidatesNeedsVolumeSizeLimit(t *testing.T) {
	_, err := EnumerateDecodeCandidates(oneVolumeTopo(ecShardEntry(7, 1<<20, 100, 50)), "", 0.50, 0)
	require.Error(t, err)
}

// TestEnumerateDecodeCandidatesDisabled verifies a zero threshold disables the route.
func TestEnumerateDecodeCandidatesDisabled(t *testing.T) {
	got, err := EnumerateDecodeCandidates(oneVolumeTopo(ecShardEntry(7, 1<<20, 100, 100)), "", 0, 40)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestEnumerateGarbageEcVolumesSkipsFullyDeletedVolume verifies a volume with no
// live needles is not a vacuum candidate. Nothing can be compacted — the executor
// could only answer "no live entries" after collecting every shard — and
// reclaiming it is the decode route's job, which purges it.
func TestEnumerateGarbageEcVolumesSkipsFullyDeletedVolume(t *testing.T) {
	before := testutil.ToFloat64(stats.ECVacuumJobsSkippedCounter.WithLabelValues("no_live_entries"))

	got, err := EnumerateGarbageEcVolumes(oneVolumeTopo(ecShardEntry(9, 1<<20, 100, 100)), "", 0.30)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, before+1, testutil.ToFloat64(stats.ECVacuumJobsSkippedCounter.WithLabelValues("no_live_entries")),
		"the skip is counted, so it does not read as a volume that was never considered")
}

// TestEnumerateGarbageEcVolumesKeepsANearlyEmptyVolume is the guard on that guard:
// one surviving needle still leaves something to compact.
func TestEnumerateGarbageEcVolumesKeepsANearlyEmptyVolume(t *testing.T) {
	got, err := EnumerateGarbageEcVolumes(oneVolumeTopo(ecShardEntry(9, 1<<20, 100, 99)), "", 0.30)
	require.NoError(t, err)
	require.Len(t, got, 1)
}
