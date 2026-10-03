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

// TestBuildProposalsModes verifies each proposal carries the action it was built
// for, that a decode renders its fullness rather than a deleted ratio, and that a
// proposal whose mode is absent reads back as a vacuum so queued jobs from before
// the decode route still execute.
func TestBuildProposalsModes(t *testing.T) {
	candidates := []garbageCandidate{
		{VolumeID: 1, Collection: "c1", FileCount: 10, DeleteCount: 5, SizeReported: true, LiveBytes: 5 << 20},
	}

	vacuum := buildProposals(candidates, modeVacuum, 40)
	require.Len(t, vacuum, 1)
	require.Equal(t, "vacuum:ec_vacuum:1:c1:", vacuum[0].GetDedupeKey())
	require.Equal(t, modeVacuum, vacuum[0].GetParameters()[fieldMode].GetStringValue())
	require.Equal(t, "vacuum", vacuum[0].GetLabels()["action"])
	require.Contains(t, vacuum[0].GetSummary(), "deleted")
	require.NotContains(t, vacuum[0].GetDetail(), "fullness")

	decode := buildProposals(candidates, modeDecode, 40)
	require.Len(t, decode, 1)
	require.NotEqual(t, vacuum[0].GetDedupeKey(), decode[0].GetDedupeKey(),
		"the two actions of one volume must not collide in the admin's dedupe")
	require.Equal(t, modeDecode, decode[0].GetParameters()[fieldMode].GetStringValue())
	require.Equal(t, "decode", decode[0].GetLabels()["action"])
	require.Contains(t, decode[0].GetSummary(), "full")
	// 5 MB live against a 40 MB limit = 12.5% full.
	require.Contains(t, decode[0].GetDetail(), "fullness=12.5%")

	// Two modes concatenate, and the caller's cap covers both together.
	combined := append(buildProposals(candidates, modeDecode, 40), buildProposals(candidates, modeVacuum, 40)...)
	require.Len(t, combined, 2)
	require.Equal(t, modeDecode, combined[0].GetParameters()[fieldMode].GetStringValue(),
		"decode proposals lead, so a cap spends its budget on them first")
	require.Equal(t, modeVacuum, combined[1].GetParameters()[fieldMode].GetStringValue())
}

// TestHasAllDataShards guards the decode precondition.
func TestHasAllDataShards(t *testing.T) {
	require.True(t, hasAllDataShards(0x3FFF, 10))
	require.False(t, hasAllDataShards(0x3FFE, 10), "missing shard 0")
	require.False(t, hasAllDataShards(0x0001, 10), "only shard 0 present")
}
