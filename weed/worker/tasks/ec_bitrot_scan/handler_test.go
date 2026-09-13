package ec_bitrot_scan

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// fakeScrubServer is a volume server that only answers the CHECKSUM scrub.
type fakeScrubServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	mu         sync.Mutex
	resp       *volume_server_pb.ScrubEcVolumeResponse
	scrubErr   error
	modeResp   map[volume_server_pb.VolumeScrubMode]*volume_server_pb.ScrubEcVolumeResponse
	modeErr    map[volume_server_pb.VolumeScrubMode]error
	scrubCalls int
	unmounted  []string
	deleted    []string

	host string
	port uint32
}

func (f *fakeScrubServer) ScrubEcVolume(ctx context.Context, req *volume_server_pb.ScrubEcVolumeRequest) (*volume_server_pb.ScrubEcVolumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scrubCalls++
	if err, ok := f.modeErr[req.GetMode()]; ok {
		return nil, err
	}
	if resp, ok := f.modeResp[req.GetMode()]; ok {
		return resp, nil
	}
	if f.scrubErr != nil {
		return nil, f.scrubErr
	}
	return f.resp, nil
}

func (f *fakeScrubServer) setModeResponse(mode volume_server_pb.VolumeScrubMode, resp *volume_server_pb.ScrubEcVolumeResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.modeResp == nil {
		f.modeResp = map[volume_server_pb.VolumeScrubMode]*volume_server_pb.ScrubEcVolumeResponse{}
	}
	f.modeResp[mode] = resp
}

func (f *fakeScrubServer) setModeError(mode volume_server_pb.VolumeScrubMode, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.modeErr == nil {
		f.modeErr = map[volume_server_pb.VolumeScrubMode]error{}
	}
	f.modeErr[mode] = err
}

// VolumeEcShardsUnmount and VolumeEcShardsDelete record the destructive calls
// clusterRepairer.Quarantine makes, so the quarantine path can be asserted
// without a live volume server.
func (f *fakeScrubServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.GetShardIds() {
		f.unmounted = append(f.unmounted, fmt.Sprintf("%d.%d", req.GetVolumeId(), sid))
	}
	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (f *fakeScrubServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.GetShardIds() {
		f.deleted = append(f.deleted, fmt.Sprintf("%d.%d", req.GetVolumeId(), sid))
	}
	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

// nodeAddress is the "host:httpPort.grpcPort" form the cluster uses, so
// ToGrpcAddress resolves back to this fake's listener.
func (f *fakeScrubServer) nodeAddress() string {
	return fmt.Sprintf("%s:1.%d", f.host, f.port)
}

func (f *fakeScrubServer) recorded() (unmounted, deleted []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.unmounted...), append([]string(nil), f.deleted...)
}

func (f *fakeScrubServer) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scrubCalls
}

func startFakeScrubServer(t *testing.T, resp *volume_server_pb.ScrubEcVolumeResponse, scrubErr error) *fakeScrubServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	host, portStr, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	f := &fakeScrubServer{resp: resp, scrubErr: scrubErr, host: host, port: uint32(port)}
	server := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(server, f)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return f
}

// fakeRepairer records the destructive calls an auto-repair run makes.
type fakeRepairer struct {
	quarantineVolumeIDs []uint32
	quarantineRefs      [][]ec.ShardRef
	rebuildVolumeIDs    []uint32
	quarantineErr       error
	rebuildErr          error
}

func (f *fakeRepairer) Quarantine(ctx context.Context, volumeID uint32, refs []ec.ShardRef) (int, error) {
	f.quarantineVolumeIDs = append(f.quarantineVolumeIDs, volumeID)
	f.quarantineRefs = append(f.quarantineRefs, refs)
	if f.quarantineErr != nil {
		return 0, f.quarantineErr
	}
	return len(refs), nil
}

func (f *fakeRepairer) Rebuild(ctx context.Context, topo *master_pb.TopologyInfo, masters []string, collection string, volumeID uint32, diskType string) error {
	f.rebuildVolumeIDs = append(f.rebuildVolumeIDs, volumeID)
	return f.rebuildErr
}

// recordingExecSender captures progress/completion for assertions.
type recordingExecSender struct {
	progresses []*plugin_pb.JobProgressUpdate
	completed  *plugin_pb.JobCompleted
}

func (r *recordingExecSender) SendProgress(p *plugin_pb.JobProgressUpdate) error {
	r.progresses = append(r.progresses, p)
	return nil
}
func (r *recordingExecSender) SendCompleted(c *plugin_pb.JobCompleted) error {
	r.completed = c
	return nil
}

func dialOption() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

func executeRequest(volumeID uint32, collection, diskType string, autoRepair bool) *plugin_pb.ExecuteJobRequest {
	admin := map[string]*plugin_pb.ConfigValue{}
	if autoRepair {
		admin[fieldAutoRepair] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true}}
	}
	return &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId:   "job-1",
			JobType: jobType,
			Parameters: map[string]*plugin_pb.ConfigValue{
				"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)}},
				"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: collection}},
				"disk_type":  {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: diskType}},
			},
		},
		AdminConfigValues: admin,
	}
}

func topologyForHolder(f *fakeScrubServer, volumeID uint32, collection string, data, parity uint32) *master_pb.TopologyInfo {
	return topologyWithNodes([]*master_pb.DataNodeInfo{
		dataNode("n1", f.host+":1", f.port, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(volumeID, collection, data, parity)},
		}),
	})
}

func handlerFor(t *testing.T, fake *fakeScrubServer, rep repairer, volumeID uint32, data, parity uint32) *BitrotScanHandler {
	return &BitrotScanHandler{
		grpcDialOption: dialOption(),
		fetchTopology: func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error) {
			return topologyForHolder(fake, volumeID, "c1", data, parity), nil
		},
		repair: rep,
	}
}

func TestExecuteReadOnlyHealthyReportsNoCorruption(t *testing.T) {
	fake := startFakeScrubServer(t, &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes: 1,
		TotalFiles:   5,
	}, nil)
	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 4)

	sender := &recordingExecSender{}
	require.NoError(t, h.Execute(context.Background(), executeRequest(7, "c1", "hdd", false), sender))

	require.NotNil(t, sender.completed)
	require.True(t, sender.completed.GetSuccess())
	// Both phases ran: INDEX (metadata) then CHECKSUM (shards), one holder.
	require.Equal(t, 2, fake.calls())
	require.Empty(t, rep.quarantineRefs)
	require.Empty(t, rep.rebuildVolumeIDs)
	require.Equal(t, int64(5), sender.completed.GetResult().GetOutputValues()["files_verified"].GetInt64Value())
	require.Equal(t, int64(0), sender.completed.GetResult().GetOutputValues()["broken_shards"].GetInt64Value())
}

func TestExecuteReadOnlyCorruptionReportedNotRepaired(t *testing.T) {
	fake := startFakeScrubServer(t, &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes:     1,
		TotalFiles:       3,
		BrokenVolumeIds:  []uint32{7},
		BrokenShardInfos: []*volume_server_pb.EcShardInfo{{VolumeId: 7, ShardId: 2}},
	}, nil)
	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 4)

	sender := &recordingExecSender{}
	require.NoError(t, h.Execute(context.Background(), executeRequest(7, "c1", "hdd", false), sender))

	require.NotNil(t, sender.completed)
	// A completed scan is a successful run even when it finds corruption; the
	// finding is surfaced through the output values below.
	require.True(t, sender.completed.GetSuccess())
	require.Equal(t, int64(1), sender.completed.GetResult().GetOutputValues()["broken_shards"].GetInt64Value())
	// Read-only: no destructive calls.
	require.Empty(t, rep.quarantineRefs)
	require.Empty(t, rep.rebuildVolumeIDs)
	require.Equal(t, int64(0), sender.completed.GetResult().GetOutputValues()["quarantined_shards"].GetInt64Value())
}

func TestExecuteAutoRepairQuarantinesAndRebuilds(t *testing.T) {
	fake := startFakeScrubServer(t, &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes:     1,
		TotalFiles:       4,
		BrokenVolumeIds:  []uint32{7},
		BrokenShardInfos: []*volume_server_pb.EcShardInfo{{VolumeId: 7, ShardId: 2}},
	}, nil)
	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 4)

	sender := &recordingExecSender{}
	require.NoError(t, h.Execute(context.Background(), executeRequest(7, "c1", "hdd", true), sender))

	require.NotNil(t, sender.completed)
	require.True(t, sender.completed.GetSuccess(), "repaired corruption reports success")
	require.Len(t, rep.quarantineVolumeIDs, 1)
	require.Equal(t, uint32(7), rep.quarantineVolumeIDs[0])
	require.Len(t, rep.quarantineRefs, 1)
	require.Len(t, rep.quarantineRefs[0], 1)
	require.Equal(t, uint32(2), rep.quarantineRefs[0][0].ShardID)
	require.Equal(t, []uint32{7}, rep.rebuildVolumeIDs)
	require.Equal(t, int64(1), sender.completed.GetResult().GetOutputValues()["quarantined_shards"].GetInt64Value())
}

func TestExecuteAutoRepairBoundedByParityCount(t *testing.T) {
	// Parity is 1, but two shards are reported broken: never quarantine more
	// than EC can rebuild.
	fake := startFakeScrubServer(t, &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes:    1,
		TotalFiles:      2,
		BrokenVolumeIds: []uint32{7},
		BrokenShardInfos: []*volume_server_pb.EcShardInfo{
			{VolumeId: 7, ShardId: 1},
			{VolumeId: 7, ShardId: 2},
		},
	}, nil)
	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 1)

	sender := &recordingExecSender{}
	require.NoError(t, h.Execute(context.Background(), executeRequest(7, "c1", "hdd", true), sender))

	require.NotNil(t, sender.completed)
	// Completed (diagnosis) => success; the unrepaired corruption shows up in outputs.
	require.True(t, sender.completed.GetSuccess())
	require.Empty(t, rep.quarantineRefs, "must not quarantine beyond parity")
	require.Empty(t, rep.rebuildVolumeIDs)
	require.GreaterOrEqual(t, sender.completed.GetResult().GetOutputValues()["integrity_errors"].GetInt64Value(), int64(1))
}

func TestExecuteScrubRPCErrorFailsJob(t *testing.T) {
	fake := startFakeScrubServer(t, nil, status.Error(codes.Internal, "scrub blew up"))
	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 4)

	sender := &recordingExecSender{}
	err := h.Execute(context.Background(), executeRequest(7, "c1", "hdd", false), sender)
	require.Error(t, err)
	require.Nil(t, sender.completed, "transport failures do not emit a completed result")
	require.NotEmpty(t, sender.progresses)
	require.Equal(t, plugin_pb.JobState_JOB_STATE_FAILED, sender.progresses[len(sender.progresses)-1].GetState())
}

func TestExecuteIndexIssuesReportedSeparately(t *testing.T) {
	fake := startFakeScrubServer(t, nil, nil)
	fake.setModeResponse(volume_server_pb.VolumeScrubMode_CHECKSUM, &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes: 1,
		TotalFiles:   2,
	})
	fake.setModeResponse(volume_server_pb.VolumeScrubMode_INDEX, &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes: 1,
		Details:      []string{"needle 5 overlaps needle 6"},
	})
	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 4)

	sender := &recordingExecSender{}
	// auto_repair off, check_index on (default).
	require.NoError(t, h.Execute(context.Background(), executeRequest(7, "c1", "hdd", false), sender))

	require.NotNil(t, sender.completed)
	require.True(t, sender.completed.GetSuccess())
	ov := sender.completed.GetResult().GetOutputValues()
	require.Equal(t, int64(1), ov["index_errors"].GetInt64Value())
	require.Equal(t, int64(0), ov["broken_shards"].GetInt64Value())
	// Index issues never quarantine.
	require.Empty(t, rep.quarantineRefs)
	require.Empty(t, rep.rebuildVolumeIDs)
	// Both phases ran (INDEX then CHECKSUM).
	require.Equal(t, 2, fake.calls())
}

func TestExecuteCheckIndexDisabledSkipsIndex(t *testing.T) {
	fake := startFakeScrubServer(t, &volume_server_pb.ScrubEcVolumeResponse{TotalVolumes: 1, TotalFiles: 1}, nil)
	fake.setModeResponse(volume_server_pb.VolumeScrubMode_INDEX, &volume_server_pb.ScrubEcVolumeResponse{
		Details: []string{"needle 5 overlaps needle 6"},
	})
	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 4)

	req := executeRequest(7, "c1", "hdd", false)
	req.AdminConfigValues[fieldCheckIndex] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: false}}

	sender := &recordingExecSender{}
	require.NoError(t, h.Execute(context.Background(), req, sender))

	// Only CHECKSUM ran; the planted index issue was never fetched.
	require.Equal(t, 1, fake.calls())
	require.Equal(t, int64(0), sender.completed.GetResult().GetOutputValues()["index_errors"].GetInt64Value())
}

func TestExecuteIndexUnsupportedDegradesToChecksum(t *testing.T) {
	fake := startFakeScrubServer(t, nil, nil)
	fake.setModeResponse(volume_server_pb.VolumeScrubMode_CHECKSUM, &volume_server_pb.ScrubEcVolumeResponse{
		TotalVolumes: 1,
		TotalFiles:   3,
	})
	fake.setModeError(volume_server_pb.VolumeScrubMode_INDEX, status.Error(codes.Unimplemented, "unsupported mode"))

	rep := &fakeRepairer{}
	h := handlerFor(t, fake, rep, 7, 10, 4)

	sender := &recordingExecSender{}
	require.NoError(t, h.Execute(context.Background(), executeRequest(7, "c1", "hdd", false), sender))

	require.NotNil(t, sender.completed)
	require.True(t, sender.completed.GetSuccess())
	require.Equal(t, int64(3), sender.completed.GetResult().GetOutputValues()["files_verified"].GetInt64Value())
	// Both phases attempted; the index phase failed and degraded.
	require.Equal(t, 2, fake.calls())
	require.Equal(t, int64(0), sender.completed.GetResult().GetOutputValues()["index_errors"].GetInt64Value())
}

func TestClusterRepairerQuarantineUnmountsAndDeletes(t *testing.T) {
	fake := startFakeScrubServer(t, nil, nil)
	rep := newClusterRepairer(dialOption())

	refs := []ec.ShardRef{
		{ShardID: 2, Collection: "c1", NodeAddress: fake.nodeAddress()},
		{ShardID: 5, Collection: "c1", NodeAddress: fake.nodeAddress()},
	}
	n, err := rep.Quarantine(context.Background(), 7, refs)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	// Each shard copy is unmounted (so serving stops) then deleted (so
	// ec.rebuild regenerates it) — the ordering ec.shard.unmount --delete uses.
	unmounted, deleted := fake.recorded()
	require.Equal(t, []string{"7.2", "7.5"}, unmounted)
	require.Equal(t, []string{"7.2", "7.5"}, deleted)
}
