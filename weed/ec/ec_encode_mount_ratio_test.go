package ec

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// fakeMountVolumeServer holds the shards of a freshly generated volume and the
// layout they were generated in, and answers the EC mount/info RPCs the way a
// volume server does: VolumeEcShardsMount walks the requested ids in order and
// fails on the first one that is not on disk. That is what turns "the caller
// asked for more shards than this layout has" into the production failure
// (mount <vid>.5: MountEcShards <vid>.5 not found on disk) instead of a partial
// mount, and it is why the mount list has to follow the generated layout.
type fakeMountVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer

	onDisk  map[erasure_coding.ShardId]bool
	layout  *volume_server_pb.EcShardConfig
	infoErr bool
	addr    string

	mu        sync.Mutex
	batches   [][]uint32
	mounted   []erasure_coding.ShardId
	generated []*volume_server_pb.VolumeEcShardsGenerateRequest
}

// VolumeEcShardsGenerate records the request so a test can assert what the
// encode path asked the holder to generate.
func (f *fakeMountVolumeServer) VolumeEcShardsGenerate(_ context.Context, req *volume_server_pb.VolumeEcShardsGenerateRequest) (*volume_server_pb.VolumeEcShardsGenerateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.generated = append(f.generated, req)
	return &volume_server_pb.VolumeEcShardsGenerateResponse{}, nil
}

func (f *fakeMountVolumeServer) generateRequests() []*volume_server_pb.VolumeEcShardsGenerateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*volume_server_pb.VolumeEcShardsGenerateRequest(nil), f.generated...)
}

func (f *fakeMountVolumeServer) VolumeEcShardsMount(_ context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, append([]uint32(nil), req.ShardIds...))
	for _, shardId := range req.ShardIds {
		if !f.onDisk[erasure_coding.ShardId(shardId)] {
			return nil, status.Error(codes.Unknown,
				fmt.Sprintf("mount %d.%d: MountEcShards %d.%d not found on disk", req.VolumeId, shardId, req.VolumeId, shardId))
		}
		f.mounted = append(f.mounted, erasure_coding.ShardId(shardId))
	}
	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (f *fakeMountVolumeServer) VolumeEcShardsInfo(_ context.Context, req *volume_server_pb.VolumeEcShardsInfoRequest) (*volume_server_pb.VolumeEcShardsInfoResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.infoErr {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("VolumeEcShardsInfo: EC volume %d not found", req.VolumeId))
	}
	return &volume_server_pb.VolumeEcShardsInfoResponse{EcShardConfig: f.layout}, nil
}

func (f *fakeMountVolumeServer) mountedShards() []erasure_coding.ShardId {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]erasure_coding.ShardId(nil), f.mounted...)
}

func (f *fakeMountVolumeServer) mountBatches() [][]uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]uint32(nil), f.batches...)
}

// address returns a node id in the "host:httpPort.grpcPort" form the cluster
// uses, so ToGrpcAddress resolves to this fake's listener. The http port is
// unused and only has to parse.
func (f *fakeMountVolumeServer) address() string {
	host, port, err := net.SplitHostPort(f.addr)
	if err != nil {
		return f.addr
	}
	return fmt.Sprintf("%s:1.%s", host, port)
}

func newFakeMountVolumeServer(t *testing.T, layout *volume_server_pb.EcShardConfig, onDisk ...erasure_coding.ShardId) *fakeMountVolumeServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	f := &fakeMountVolumeServer{
		onDisk: make(map[erasure_coding.ShardId]bool, len(onDisk)),
		layout: layout,
		addr:   listener.Addr().String(),
	}
	for _, shardId := range onDisk {
		f.onDisk[shardId] = true
	}

	server := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(server, f)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return f
}

func testMountDialOption() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

func shardIdsUpTo(total int) []erasure_coding.ShardId {
	shardIds := make([]erasure_coding.ShardId, total)
	for i := range shardIds {
		shardIds[i] = erasure_coding.ShardId(i)
	}
	return shardIds
}

const mountTestCollection = "blender-blender"

// A volume the cluster configured 3+2 generates five shards. The build's 0..13
// list fails on shard 5, so the mount has to follow what the holder generated.
func TestMountGeneratedEcShardsFollowsCustomRatio(t *testing.T) {
	vid := needle.VolumeId(3606)
	layout := &volume_server_pb.EcShardConfig{DataShards: 3, ParityShards: 2}

	// The pre-fix call, for the record: the build's list against a 3+2 layout
	// mounts the prefix it can and fails on the first shard past the layout.
	preFix := newFakeMountVolumeServer(t, layout, shardIdsUpTo(5)...)
	err := MountEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(preFix.address()), erasure_coding.AllShardIds())
	require.ErrorContains(t, err, "mount 3606.5")
	assert.Equal(t, erasure_coding.ShardIdsToUint32(erasure_coding.AllShardIds()), preFix.mountBatches()[0])
	assert.Equal(t, shardIdsUpTo(5), preFix.mountedShards())

	holder := newFakeMountVolumeServer(t, layout, shardIdsUpTo(5)...)
	require.NoError(t, mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address())))

	assert.Equal(t, shardIdsUpTo(5), holder.mountedShards())
	assert.Equal(t, [][]uint32{{0}, {1, 2, 3, 4}}, holder.mountBatches())
}

// A holder that reports no layout (a binary predating the field) keeps the
// build's 10+4 behavior.
func TestMountGeneratedEcShardsDefaultsToBuildRatio(t *testing.T) {
	vid := needle.VolumeId(3607)
	holder := newFakeMountVolumeServer(t, nil, shardIdsUpTo(erasure_coding.TotalShardsCount)...)

	require.NoError(t, mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address())))

	assert.Equal(t, shardIdsUpTo(erasure_coding.TotalShardsCount), holder.mountedShards())
	require.Len(t, holder.mountBatches(), 2)
	assert.Equal(t, []uint32{0}, holder.mountBatches()[0])
	assert.Equal(t, erasure_coding.ShardIdsToUint32(shardIdsUpTo(erasure_coding.TotalShardsCount))[1:], holder.mountBatches()[1])
}

// A layout wider than the build default must not be truncated to 14 ids.
func TestMountGeneratedEcShardsHandlesLayoutWiderThanDefault(t *testing.T) {
	vid := needle.VolumeId(3608)
	holder := newFakeMountVolumeServer(t,
		&volume_server_pb.EcShardConfig{DataShards: 12, ParityShards: 4},
		shardIdsUpTo(16)...)

	require.NoError(t, mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address())))

	assert.Equal(t, shardIdsUpTo(16), holder.mountedShards())
}

// A holder that cannot report its layout falls back to the build's list rather
// than failing the encode on the probe.
func TestMountGeneratedEcShardsFallsBackWhenLayoutCannotBeRead(t *testing.T) {
	vid := needle.VolumeId(3609)
	holder := newFakeMountVolumeServer(t, nil, shardIdsUpTo(erasure_coding.TotalShardsCount)...)
	holder.infoErr = true

	require.NoError(t, mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address())))

	assert.Equal(t, shardIdsUpTo(erasure_coding.TotalShardsCount), holder.mountedShards())
}

// A volume whose holder reports a layout the policy does not name must not be
// brought online: it would be mounted, rebalanced and have its source deleted
// while sitting at a ratio nobody chose. This is the case a volume server that
// predates the requested-layout field produces.
func TestMountGeneratedEcShardsRefusesPolicyMismatch(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))

	vid := needle.VolumeId(3611)
	holder := newFakeMountVolumeServer(t,
		&volume_server_pb.EcShardConfig{DataShards: 2, ParityShards: 2},
		shardIdsUpTo(4)...)

	err := mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address()))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "EC policy")
	assert.Contains(t, err.Error(), "generated 2+2")
	// Only shard 0 was mounted (enough to read the layout); the rest was stopped.
	assert.Equal(t, []erasure_coding.ShardId{0}, holder.mountedShards())
}

// The same holder with a matching policy is brought online, and the mount still
// follows the holder's own layout.
func TestMountGeneratedEcShardsAcceptsMatchingPolicy(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	require.NoError(t, erasure_coding.SetCollectionECConfig(mountTestCollection, 3, 2))

	vid := needle.VolumeId(3612)
	holder := newFakeMountVolumeServer(t,
		&volume_server_pb.EcShardConfig{DataShards: 3, ParityShards: 2},
		shardIdsUpTo(5)...)

	require.NoError(t, mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address())))
	assert.Equal(t, shardIdsUpTo(5), holder.mountedShards())
}

// A holder that reports no layout cannot be checked against the policy; the
// build's list is used, as before this path knew about policies.
func TestMountGeneratedEcShardsWithoutHolderLayoutIgnoresPolicy(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))

	vid := needle.VolumeId(3613)
	holder := newFakeMountVolumeServer(t, nil, shardIdsUpTo(erasure_coding.TotalShardsCount)...)

	require.NoError(t, mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address())))
	assert.Equal(t, shardIdsUpTo(erasure_coding.TotalShardsCount), holder.mountedShards())
}

// The encode path must tell the holder the policy's layout — that is how a
// volume with no recorded layout stops defaulting to 10+4 — and must send
// nothing when no policy covers the collection, so an unconfigured cluster
// keeps the volume-server behavior.
func TestGenerateEcShardsSendsPolicyLayout(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	vid := needle.VolumeId(3614)
	holder := newFakeMountVolumeServer(t, nil)

	require.NoError(t, generateEcShards(testMountDialOption(), vid, mountTestCollection, pb.ServerAddress(holder.address())))
	requests := holder.generateRequests()
	require.Len(t, requests, 1)
	assert.Nil(t, requests[0].GetEcShardConfig(), "no policy: send no layout, the holder keeps its own rule")

	require.NoError(t, erasure_coding.SetCollectionECConfig(mountTestCollection, 3, 2))
	require.NoError(t, generateEcShards(testMountDialOption(), vid, mountTestCollection, pb.ServerAddress(holder.address())))
	requests = holder.generateRequests()
	require.Len(t, requests, 2)
	policy := requests[1].GetEcShardConfig()
	require.NotNil(t, policy, "a configured policy must travel with the generate request")
	assert.Equal(t, uint32(3), policy.DataShards)
	assert.Equal(t, uint32(2), policy.ParityShards)
}

// The capacity pre-flight charges the layout the volumes will actually get, so a
// 3+2 cluster is not asked for 14 slots per volume.
func TestEcLayoutTotalShardsFollowsPolicy(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	assert.Equal(t, erasure_coding.TotalShardsCount, ecLayoutTotalShards("unconfigured"))

	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))
	assert.Equal(t, 5, ecLayoutTotalShards("anything"))

	require.NoError(t, erasure_coding.SetCollectionECConfig("blender-blender", 6, 3))
	assert.Equal(t, 9, ecLayoutTotalShards("blender-blender"))
	assert.Equal(t, 5, ecLayoutTotalShards("other"))
}

// A volume with nothing generated still reports the holder's error for shard 0
// instead of mounting a partial layout.
func TestMountGeneratedEcShardsReportsMissingFirstShard(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	vid := needle.VolumeId(3610)
	holder := newFakeMountVolumeServer(t, &volume_server_pb.EcShardConfig{DataShards: 3, ParityShards: 2})

	err := mountGeneratedEcShards(testMountDialOption(), mountTestCollection, vid, pb.ServerAddress(holder.address()))

	require.ErrorContains(t, err, "mount 3610.0: MountEcShards 3610.0 not found on disk")
	assert.Empty(t, holder.mountedShards())
	assert.Equal(t, [][]uint32{{0}}, holder.mountBatches())
}
