package ec_vacuum_test

import (
	"context"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/ec_vacuum"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestEcVacuumDetectsHighDeleteRatio drives detection through the real plugin
// worker/admin protocol: the handler registers with the worker, dials the (stub)
// master for topology, and proposes a vacuum only for the volume whose deleted
// ratio is over the 30% default threshold.
func TestEcVacuumDetectsHighDeleteRatio(t *testing.T) {
	response := &master_pb.VolumeListResponse{
		VolumeSizeLimitMb: 100,
		TopologyInfo: &master_pb.TopologyInfo{
			DataCenterInfos: []*master_pb.DataCenterInfo{{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{{
					Id: "rack1",
					DataNodeInfos: []*master_pb.DataNodeInfo{{
						Id:      "n1",
						Address: "127.0.0.1:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
								{Id: 7, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 40, EncodeTsNs: 1},
								{Id: 8, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 1, EncodeTsNs: 1},
							}},
						},
					}},
				}},
			}},
		},
	}
	master := pluginworkers.NewMasterServer(t, response)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := ec_vacuum.NewEcVacuumHandler(dialOption, t.TempDir())
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{GrpcDialOption: dialOption},
		Handlers:      []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("ec_vacuum")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	proposals, err := harness.Plugin().RunDetection(ctx, "ec_vacuum", &plugin_pb.ClusterContext{
		MasterGrpcAddresses: []string{master.Address()},
	}, 10)
	require.NoError(t, err)
	require.Len(t, proposals, 1, "only the volume over the delete threshold is proposed")
	require.Equal(t, "ec_vacuum", proposals[0].GetJobType())
	require.Equal(t, int64(7), proposals[0].GetParameters()["volume_id"].GetInt64Value())
	require.Equal(t, "c1", proposals[0].GetParameters()["collection"].GetStringValue())
}
