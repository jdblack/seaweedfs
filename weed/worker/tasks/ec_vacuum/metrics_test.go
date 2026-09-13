package ec_vacuum

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func vacuumJobSpec() *plugin_pb.JobSpec {
	return &plugin_pb.JobSpec{
		JobId:   "ec-vacuum-test",
		JobType: jobType,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
			"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "c1"}},
		},
	}
}

// TestMetricsOnSuccessfulVacuum verifies a completed vacuum bumps the executed
// counter and records reclaimed bytes.
func TestMetricsOnSuccessfulVacuum(t *testing.T) {
	srcDir := t.TempDir()
	buildEncodedFixture(t, srcDir, "c1", 1, 20, 10)

	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return syntheticTopo("c1", 1, 0x3FFF, 10), nil
	}
	handler.transport = &fakeTransport{srcDir: srcDir}

	executedBefore := testutil.ToFloat64(stats.ECVacuumJobsExecutedCounter)
	reclaimedBefore := testutil.ToFloat64(stats.ECVacuumBytesReclaimedCounter)

	require.NoError(t, handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: vacuumJobSpec()}, &recordingSender{}))

	require.Equal(t, executedBefore+1, testutil.ToFloat64(stats.ECVacuumJobsExecutedCounter))
	require.Greater(t, testutil.ToFloat64(stats.ECVacuumBytesReclaimedCounter), reclaimedBefore)
}

// TestMetricsOnFailedVacuum verifies a failed redistribution bumps both the
// failed and rolled-back counters.
func TestMetricsOnFailedVacuum(t *testing.T) {
	srcDir := t.TempDir()
	buildEncodedFixture(t, srcDir, "c1", 1, 20, 10)

	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return syntheticTopo("c1", 1, 0x3FFF, 10), nil
	}
	handler.transport = &fakeTransport{srcDir: srcDir, failRedistributes: 1}

	failedBefore := testutil.ToFloat64(stats.ECVacuumJobsFailedCounter)
	rolledBackBefore := testutil.ToFloat64(stats.ECVacuumRolledBackCounter)

	require.Error(t, handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: vacuumJobSpec()}, &recordingSender{}))

	require.Equal(t, failedBefore+1, testutil.ToFloat64(stats.ECVacuumJobsFailedCounter))
	require.Equal(t, rolledBackBefore+1, testutil.ToFloat64(stats.ECVacuumRolledBackCounter))
}

// TestMetricsOnNoLiveEntries verifies the all-deleted no-op is counted as a skip.
func TestMetricsOnNoLiveEntries(t *testing.T) {
	srcDir := t.TempDir()
	buildEncodedFixture(t, srcDir, "c1", 1, 5, 5)

	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return syntheticTopo("c1", 1, 0x3FFF, 5), nil
	}
	handler.transport = &fakeTransport{srcDir: srcDir}

	before := testutil.ToFloat64(stats.ECVacuumJobsSkippedCounter.WithLabelValues("no_live_entries"))
	require.NoError(t, handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: vacuumJobSpec()}, &recordingSender{}))
	require.Equal(t, before+1, testutil.ToFloat64(stats.ECVacuumJobsSkippedCounter.WithLabelValues("no_live_entries")))
}

// TestMetricsOnBelowThresholdSkip verifies volumes under the deleted-ratio
// threshold are counted as skipped.
func TestMetricsOnBelowThresholdSkip(t *testing.T) {
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
							{Id: 7, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 1, EncodeTsNs: 1},
						}},
					},
				}},
			}},
		}},
	}

	before := testutil.ToFloat64(stats.ECVacuumJobsSkippedCounter.WithLabelValues("below_threshold"))
	candidates, err := enumerateGarbageEcVolumes(topo, "", 0.30)
	require.NoError(t, err)
	require.Empty(t, candidates)
	require.Equal(t, before+1, testutil.ToFloat64(stats.ECVacuumJobsSkippedCounter.WithLabelValues("below_threshold")))
}
