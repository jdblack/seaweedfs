package ec_vacuum

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// TestBuildVacuumTargetRejectsMixedGeneration verifies a volume whose shards span
// more than one encode generation is refused rather than compacted.
func TestBuildVacuumTargetRejectsMixedGeneration(t *testing.T) {
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
							{Id: 7, Collection: "c1", EcIndexBits: 0x3FFF, DataShards: 10, ParityShards: 4, FileCount: 100, DeleteCount: 40, EncodeTsNs: 2},
						}},
					},
				}},
			}},
		}},
	}

	_, err := buildVacuumTarget(topo, 7, "c1", "hdd")
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode generations")
}

// TestVerifyEncodedShardsRejectsTruncatedShard verifies the decode-back check
// rejects a shard set that no longer decodes to the encoded data.
func TestVerifyEncodedShardsRejectsTruncatedShard(t *testing.T) {
	dir := t.TempDir()
	opts := buildEncodedFixture(t, dir, "c1", 1, 20, 0)
	require.Equal(t, 10, opts.DataShards)
	fullBase := filepath.Join(dir, shardBaseName("c1", 1))

	require.NoError(t, verifyEncodedShards(fullBase), "an intact shard set verifies")

	require.NoError(t, os.Truncate(fullBase+".ec00", 16))
	require.Error(t, verifyEncodedShards(fullBase), "a truncated data shard must fail verification")
}

// TestExecuteRollsBackOnRedistributeFailure verifies a failed redistribution
// restores the collected originals so the holders are not left mixed-generation.
func TestExecuteRollsBackOnRedistributeFailure(t *testing.T) {
	srcDir := t.TempDir()
	buildEncodedFixture(t, srcDir, "c1", 1, 20, 10)

	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return syntheticTopo("c1", 1, 0x3FFF, 10), nil
	}
	fake := &fakeTransport{srcDir: srcDir, failRedistributes: 1}
	handler.transport = fake

	job := &plugin_pb.JobSpec{
		JobId:   "ec-vacuum-test",
		JobType: jobType,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
			"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "c1"}},
		},
	}
	err := handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: job}, &recordingSender{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rolled back")

	require.Len(t, fake.redistributeDirs, 2, "push then rollback")
	require.Equal(t, "work", filepath.Base(fake.redistributeDirs[0]))
	require.Equal(t, "orig", filepath.Base(fake.redistributeDirs[1]))
	require.Equal(t, []bool{true, false}, fake.redistributeClear,
		"the success push clears the delete journals; the rollback keeps them")
}
