package ec_bitrot_scan_test

import (
	"context"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/ec_bitrot_scan"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestEcBitrotScanDetectsEcVolumes drives detection through the real plugin
// worker/admin protocol: the handler registers with the worker, dials the
// (stub) master for topology, and proposes one scan per EC volume.
func TestEcBitrotScanDetectsEcVolumes(t *testing.T) {
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
							"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
								Id:           7,
								Collection:   "c1",
								EcIndexBits:  0x3FFF,
								DataShards:   10,
								ParityShards: 4,
							}}},
						},
					}},
				}},
			}},
		},
	}
	master := pluginworkers.NewMasterServer(t, response)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := ec_bitrot_scan.NewBitrotScanHandler(dialOption)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{GrpcDialOption: dialOption},
		Handlers:      []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("ec_bitrot_scan")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	proposals, err := harness.Plugin().RunDetection(ctx, "ec_bitrot_scan", &plugin_pb.ClusterContext{
		MasterGrpcAddresses: []string{master.Address()},
	}, 10)
	require.NoError(t, err)
	require.Len(t, proposals, 1)
	require.Equal(t, "ec_bitrot_scan", proposals[0].GetJobType())
	require.Equal(t, int64(7), proposals[0].GetParameters()["volume_id"].GetInt64Value())
	require.Equal(t, "c1", proposals[0].GetParameters()["collection"].GetStringValue())
}
