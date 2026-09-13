package ec_vacuum

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// fakeDetectionSender captures a detection run's output.
type fakeDetectionSender struct {
	proposals  []*plugin_pb.JobProposal
	complete   *plugin_pb.DetectionComplete
	activities int
}

func (s *fakeDetectionSender) SendProposals(p *plugin_pb.DetectionProposals) error {
	s.proposals = append(s.proposals, p.GetProposals()...)
	return nil
}
func (s *fakeDetectionSender) SendComplete(c *plugin_pb.DetectionComplete) error {
	s.complete = c
	return nil
}
func (s *fakeDetectionSender) SendActivity(*plugin_pb.ActivityEvent) error {
	s.activities++
	return nil
}

// TestDetectSkipsWhenRecent verifies the min-interval gate short-circuits before
// any topology fetch when the last successful run is recent.
func TestDetectSkipsWhenRecent(t *testing.T) {
	handler := NewEcVacuumHandler(nil, t.TempDir())
	fetches := 0
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		fetches++
		return syntheticTopo("c1", 1, 0x3FFF, 10), nil
	}

	sender := &fakeDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:           jobType,
		LastSuccessfulRun: timestamppb.New(time.Now()),
		ClusterContext:    &plugin_pb.ClusterContext{MasterGrpcAddresses: []string{"127.0.0.1:1"}},
	}, sender)
	require.NoError(t, err)
	require.Equal(t, 0, fetches, "detection must skip before fetching topology")
	require.Empty(t, sender.proposals)
	require.NotNil(t, sender.complete)
	require.True(t, sender.complete.GetSuccess())
	require.Equal(t, int32(0), sender.complete.GetTotalProposals())
}

// TestDetectFiltersByDiskType verifies the disk_type filter narrows detection to
// one medium and exercises filterCandidatesByDiskType.
func TestDetectFiltersByDiskType(t *testing.T) {
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
							{Id: 7, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 40, EncodeTsNs: 1},
						}},
						"ssd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 8, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 40, EncodeTsNs: 1},
						}},
					},
				}},
			}},
		}},
	}

	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return topo, nil
	}

	sender := &fakeDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType: jobType,
		AdminConfigValues: map[string]*plugin_pb.ConfigValue{
			fieldDiskType: {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "ssd"}},
		},
	}, sender)
	require.NoError(t, err)
	require.Len(t, sender.proposals, 1)
	require.Equal(t, int64(8), sender.proposals[0].GetParameters()["volume_id"].GetInt64Value())
}

// TestExecuteNoLiveNeedlesIsSuccessfulNoOp verifies a fully-deleted volume ends
// as a successful no-op instead of producing an empty volume.
func TestExecuteNoLiveNeedlesIsSuccessfulNoOp(t *testing.T) {
	srcDir := t.TempDir()
	buildEncodedFixture(t, srcDir, "c1", 1, 5, 5) // every needle deleted

	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return syntheticTopo("c1", 1, 0x3FFF, 5), nil
	}
	fake := &fakeTransport{srcDir: srcDir}
	handler.transport = fake

	job := &plugin_pb.JobSpec{
		JobId:   "ec-vacuum-test",
		JobType: jobType,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
			"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "c1"}},
		},
	}
	sender := &recordingSender{}
	err := handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: job}, sender)
	require.NoError(t, err)
	require.NotNil(t, sender.completed)
	require.True(t, sender.completed.GetSuccess())
	require.False(t, fake.redistributed, "a volume with no live needles must not be redistributed")
	require.Contains(t, sender.completed.GetResult().GetSummary(), "no live entries")
}

// TestLoadVacuumOptionsWithoutVif verifies graceful fallback to the topology's
// ratio and the legacy block layout when no .vif was collected.
func TestLoadVacuumOptionsWithoutVif(t *testing.T) {
	target := vacuumTarget{VolumeID: 3, Collection: "c1", DataShards: 10, ParityShards: 4, DiskType: "hdd"}
	opts, err := loadVacuumOptions(t.TempDir(), "c1_3", target)
	require.NoError(t, err)
	require.Equal(t, 10, opts.DataShards)
	require.Equal(t, 4, opts.ParityShards)
	require.Zero(t, opts.BlockSize)
	require.Zero(t, opts.EncodedDatFileSize)
}

// TestJobPriority verifies the heavily-deleted volume is bumped to HIGH.
func TestJobPriority(t *testing.T) {
	require.Equal(t, plugin_pb.JobPriority_JOB_PRIORITY_HIGH, jobPriority(0.7))
	require.Equal(t, plugin_pb.JobPriority_JOB_PRIORITY_NORMAL, jobPriority(0.3))
}

// TestDeletedRatioZeroFileCount verifies the divide-by-zero guard.
func TestDeletedRatioZeroFileCount(t *testing.T) {
	require.Equal(t, 0.0, garbageCandidate{FileCount: 0, DeleteCount: 9}.DeletedRatio())
}

// TestMasterAddressesNilContext verifies the nil-context guard.
func TestMasterAddressesNilContext(t *testing.T) {
	require.Nil(t, masterAddresses(nil))
}

// TestEnumerateGarbageEcVolumesCollectionFilter verifies the collection matcher.
func TestEnumerateGarbageEcVolumesCollectionFilter(t *testing.T) {
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
							{Id: 1, Collection: "keep", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 50, EncodeTsNs: 1},
							{Id: 2, Collection: "skip", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 50, EncodeTsNs: 1},
						}},
					},
				}},
			}},
		}},
	}

	candidates, err := enumerateGarbageEcVolumes(topo, "keep", 0.30)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "keep", candidates[0].Collection)
}
