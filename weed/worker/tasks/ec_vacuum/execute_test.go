package ec_vacuum

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// fakeTransport simulates shard collection/redistribution without a cluster: it
// copies a prebuilt shard set into the worker working dir and records the
// redistribution.
type fakeTransport struct {
	srcDir        string
	collected     []uint32
	redistributed bool
	// redistributeDirs records the dir each Redistribute call pushed from, so a
	// test can assert the push-then-rollback sequence.
	redistributeDirs []string
	// redistributeClear records the clearJournal flag of each Redistribute call,
	// so a test can assert the delete journals are cleared only on success.
	redistributeClear []bool
	// failRedistributes makes that many leading Redistribute calls fail, to
	// exercise the rollback path.
	failRedistributes int
}

func (f *fakeTransport) Collect(_ context.Context, target vacuumTarget, dir, base string) ([]uint32, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(f.srcDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		// Only the EC artifact set is collected; the .dat/.idx fixture files are
		// not part of a real holder's shard inventory.
		switch filepath.Ext(e.Name()) {
		case ".dat", ".idx":
			continue
		}
		data, err := os.ReadFile(filepath.Join(f.srcDir, e.Name()))
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), data, 0o644); err != nil {
			return nil, err
		}
	}
	ids := make([]uint32, 0, len(target.Holders))
	for id := range target.Holders {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	f.collected = ids
	return ids, nil
}

func (f *fakeTransport) Redistribute(_ context.Context, _ vacuumTarget, dir, _ string, clearJournal bool) error {
	f.redistributeDirs = append(f.redistributeDirs, dir)
	f.redistributeClear = append(f.redistributeClear, clearJournal)
	if f.failRedistributes > 0 {
		f.failRedistributes--
		return fmt.Errorf("induced redistribute failure")
	}
	f.redistributed = true
	return nil
}

// recordingSender captures the terminal job completion.
type recordingSender struct {
	completed *plugin_pb.JobCompleted
}

func (s *recordingSender) SendProgress(*plugin_pb.JobProgressUpdate) error { return nil }
func (s *recordingSender) SendCompleted(c *plugin_pb.JobCompleted) error {
	s.completed = c
	return nil
}

func syntheticTopo(collection string, vid uint32, bits uint32, deleteCount uint64) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "r1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id:      "n1",
					Address: "127.0.0.1:8080",
					DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
							Id:           vid,
							Collection:   collection,
							EcIndexBits:  bits,
							DataShards:   10,
							ParityShards: 4,
							FileCount:    20,
							DeleteCount:  deleteCount,
							EncodeTsNs:   1,
						}}},
					},
				}},
			}},
		}},
	}
}

// TestExecuteVacuumEndToEnd drives the handler's execution path with a fake
// transport over a real encoded fixture and asserts the compacted shards are
// redistributed and the run reports reclaimed bytes.
func TestExecuteVacuumEndToEnd(t *testing.T) {
	const (
		collection = "c1"
		vid        = uint32(1)
	)

	srcDir := t.TempDir()
	buildEncodedFixture(t, srcDir, collection, vid, 20, 10)

	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return syntheticTopo(collection, vid, 0x3FFF, 10), nil
	}
	fake := &fakeTransport{srcDir: srcDir}
	handler.transport = fake

	job := &plugin_pb.JobSpec{
		JobId:   "ec-vacuum-test",
		JobType: jobType,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(vid)}},
			"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: collection}},
		},
	}
	sender := &recordingSender{}
	err := handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: job}, sender)
	require.NoError(t, err)
	require.NotNil(t, sender.completed)
	require.True(t, sender.completed.GetSuccess(), sender.completed.GetErrorMessage())
	require.True(t, fake.redistributed, "compacted shards must be redistributed")
	require.Greater(t, sender.completed.GetResult().GetOutputValues()["bytes_reclaimed"].GetInt64Value(), int64(0))
}

// TestExecuteRefusesUndecodableVolume verifies a volume missing a data shard is
// failed rather than vacuud, so ec.rebuild can repair it first.
func TestExecuteRefusesUndecodableVolume(t *testing.T) {
	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, error) {
		return syntheticTopo("c1", 1, 0x3FFE, 10), nil // data shard 0 missing
	}
	handler.transport = &fakeTransport{srcDir: t.TempDir()}

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
	require.Contains(t, err.Error(), "missing a data shard")
}
