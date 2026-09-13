package ec_vacuum

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestEnumerateGarbageEcVolumes verifies the detector aggregates per-holder
// delete counts, ignores volumes below the threshold, and refuses volumes that
// are missing a data shard.
func TestEnumerateGarbageEcVolumes(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "r1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{
						Id:      "n1",
						Address: "127.0.0.1:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
								// 60% deleted across holders -> candidate.
								{Id: 7, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 40, EncodeTsNs: 111},
								// 2% deleted -> below threshold.
								{Id: 8, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 2, EncodeTsNs: 111},
								// 90% deleted but data shard 0 is missing -> not decodable.
								{Id: 9, Collection: "c1", EcIndexBits: 0x3FFE, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 90, EncodeTsNs: 111},
							}},
						},
					},
					{
						Id:      "n2",
						Address: "127.0.0.1:8081",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
								{Id: 7, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 20, EncodeTsNs: 111},
							}},
						},
					},
				},
			}},
		}},
	}

	candidates, err := enumerateGarbageEcVolumes(topo, "", 0.30)
	require.NoError(t, err)
	require.Len(t, candidates, 1, "only volume 7 qualifies")

	c := candidates[0]
	require.Equal(t, uint32(7), c.VolumeID)
	require.Equal(t, uint64(60), c.DeleteCount, "delete counts are summed across holders")
	require.Equal(t, uint64(100), c.FileCount)
	require.Equal(t, 14, c.PresentShardCount())
	require.InDelta(t, 0.60, c.DeletedRatio(), 0.0001)
}

// TestEnumerateGarbageEcVolumesIgnoresStaleGeneration verifies delete counts
// from a superseded encode generation are not attributed to the live volume.
func TestEnumerateGarbageEcVolumesIgnoresStaleGeneration(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "r1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id:      "n1",
					Address: "127.0.0.1:8080",
					DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							// Old generation with a huge delete count must be ignored.
							{Id: 12, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 95, EncodeTsNs: 100},
							// New generation with almost no deletes.
							{Id: 12, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 1, EncodeTsNs: 200},
						}},
					},
				}},
			}},
		}},
	}

	candidates, err := enumerateGarbageEcVolumes(topo, "", 0.30)
	require.NoError(t, err)
	require.Empty(t, candidates, "the newest generation's low delete count wins")
}

// TestBuildProposalsCapsResults verifies the detection cap and hasMore flag.
func TestBuildProposalsCapsResults(t *testing.T) {
	candidates := []garbageCandidate{
		{VolumeID: 1, Collection: "c1", FileCount: 10, DeleteCount: 5},
		{VolumeID: 2, Collection: "c1", FileCount: 10, DeleteCount: 5},
		{VolumeID: 3, Collection: "c1", FileCount: 10, DeleteCount: 5},
	}

	proposals, hasMore := buildProposals(candidates, 2)
	require.Len(t, proposals, 2)
	require.True(t, hasMore)
	require.Equal(t, "ec_vacuum:1:c1:", proposals[0].GetDedupeKey())

	all, hasMore := buildProposals(candidates, 0)
	require.Len(t, all, 3)
	require.False(t, hasMore)
}

// TestHasAllDataShards guards the decode precondition.
func TestHasAllDataShards(t *testing.T) {
	require.True(t, hasAllDataShards(0x3FFF, 10))
	require.False(t, hasAllDataShards(0x3FFE, 10), "missing shard 0")
	require.False(t, hasAllDataShards(0x0001, 10), "only shard 0 present")
}
