package erasure_coding

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	ecstorage "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// A worker has no filer of its own: the admin sends the filer addresses with
// every job, and syncECPolicyFromCluster reads the policy document from one of
// them. That is the only way a worker-encoded volume learns the cluster ratio,
// so it is the one path that must not silently fall back to the build default.
// The fake serves just the slice of the filer API the document needs.

type fakePolicyFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu      sync.Mutex
	entries map[string]*filer_pb.Entry
}

func policyEntryKey(dir, name string) string { return dir + "/" + name }

func (f *fakePolicyFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, found := f.entries[policyEntryKey(req.Directory, req.Name)]
	if !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (f *fakePolicyFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[policyEntryKey(req.Directory, req.Entry.Name)] = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *fakePolicyFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[policyEntryKey(req.Directory, req.Entry.Name)] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *fakePolicyFiler) put(dir, name string, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[policyEntryKey(dir, name)] = &filer_pb.Entry{Name: name, Content: content}
}

// startFakePolicyFiler serves the fake on a loopback port and returns the
// "host:httpPort.grpcPort" address a cluster context carries.
func startFakePolicyFiler(t *testing.T) (*fakePolicyFiler, pb.ServerAddress) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	f := &fakePolicyFiler{entries: map[string]*filer_pb.Entry{}}
	server := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(server, f)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	host, portString, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(portString)
	require.NoError(t, err)
	return f, pb.NewServerAddress(host, 1, grpcPort)
}

func policyTestDialOption() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// The admin writes the document; a worker loading it from the cluster context
// must end up resolving the same ratio, overrides included.
func TestSyncECPolicyFromClusterLoadsThePolicy(t *testing.T) {
	ecstorage.ResetECConfig()
	t.Cleanup(ecstorage.ResetECConfig)

	filer, filerAddress := startFakePolicyFiler(t)
	filer.put("/etc/seaweedfs", "ec_ratio.json", []byte(
		`{"global":{"data_shards":3,"parity_shards":2},`+
			`"collections":{"blender-blender":{"data_shards":5,"parity_shards":1}}}`))

	syncECPolicyFromCluster(
		&plugin_pb.ClusterContext{FilerAddresses: []string{string(filerAddress)}},
		policyTestDialOption())

	global, found := ecstorage.GlobalECConfig()
	require.True(t, found, "the worker must read the policy the admin wrote")
	assert.Equal(t, ecstorage.ECConfigEntry{DataShards: 3, ParityShards: 2}, global)
	override, found := ecstorage.ResolveECConfig("blender-blender")
	require.True(t, found)
	assert.Equal(t, ecstorage.ECConfigEntry{DataShards: 5, ParityShards: 1}, override)

	// …and an unset job-type config then inherits it, collection by collection.
	cfg := NewDefaultConfig()
	assert.Equal(t, 5, cfg.ResolveDataShardsFor("blender-blender"))
	assert.Equal(t, 1, cfg.ResolveParityShardsFor("blender-blender"))
	assert.Equal(t, 3, cfg.ResolveDataShardsFor("other"))
}

// No filers in the context: leave the process's policy alone rather than
// clearing it (a context is sometimes empty on an older admin).
func TestSyncECPolicyFromClusterIgnoresAbsentFilers(t *testing.T) {
	ecstorage.ResetECConfig()
	t.Cleanup(ecstorage.ResetECConfig)
	require.NoError(t, ecstorage.SetGlobalECConfig(3, 2))

	syncECPolicyFromCluster(nil, policyTestDialOption())
	syncECPolicyFromCluster(&plugin_pb.ClusterContext{}, policyTestDialOption())
	syncECPolicyFromCluster(&plugin_pb.ClusterContext{FilerAddresses: []string{"", "   "}}, policyTestDialOption())

	global, found := ecstorage.GlobalECConfig()
	require.True(t, found)
	assert.Equal(t, ecstorage.ECConfigEntry{DataShards: 3, ParityShards: 2}, global)
}

// A document that fails validation must not turn the policy into "none": the
// process keeps what it had, and the warn-and-continue leaves the encode to fall
// back to the build default *only* if there never was a policy.
func TestSyncECPolicyFromClusterKeepsPolicyOnBrokenDocument(t *testing.T) {
	ecstorage.ResetECConfig()
	t.Cleanup(ecstorage.ResetECConfig)
	require.NoError(t, ecstorage.SetGlobalECConfig(3, 2))

	filer, filerAddress := startFakePolicyFiler(t)
	filer.put("/etc/seaweedfs", "ec_ratio.json", []byte(`{"global":{"data_shards":0,"parity_shards":2}}`))

	syncECPolicyFromCluster(
		&plugin_pb.ClusterContext{FilerAddresses: []string{string(filerAddress)}},
		policyTestDialOption())

	global, found := ecstorage.GlobalECConfig()
	require.True(t, found, "a rejected document must not erase the loaded policy")
	assert.Equal(t, ecstorage.ECConfigEntry{DataShards: 3, ParityShards: 2}, global)
}

// An unreachable filer is the same story: warn, keep what we have, carry on.
func TestSyncECPolicyFromClusterKeepsPolicyWhenFilerUnreachable(t *testing.T) {
	ecstorage.ResetECConfig()
	t.Cleanup(ecstorage.ResetECConfig)
	require.NoError(t, ecstorage.SetGlobalECConfig(3, 2))

	// Close a listener so nothing answers on that port: the dial is lazy, so the
	// RPC is what fails.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host, portString, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(portString)
	require.NoError(t, err)
	require.NoError(t, lis.Close())

	syncECPolicyFromCluster(
		&plugin_pb.ClusterContext{FilerAddresses: []string{pb.NewServerAddress(host, 1, grpcPort).String()}},
		policyTestDialOption())

	global, found := ecstorage.GlobalECConfig()
	require.True(t, found)
	assert.Equal(t, ecstorage.ECConfigEntry{DataShards: 3, ParityShards: 2}, global)
}

// A filer that answers with no document means "no policy" (that is how a policy
// is deleted), so the fallback is the build default — the same state a worker
// with no policy at all is in.
func TestSyncECPolicyFromClusterEmptyFilerClearsPolicy(t *testing.T) {
	ecstorage.ResetECConfig()
	t.Cleanup(ecstorage.ResetECConfig)
	_, filerAddress := startFakePolicyFiler(t)

	require.NoError(t, ecstorage.SetGlobalECConfig(3, 2))
	syncECPolicyFromCluster(
		&plugin_pb.ClusterContext{FilerAddresses: []string{string(filerAddress)}},
		policyTestDialOption())

	_, found := ecstorage.GlobalECConfig()
	assert.False(t, found, "a deleted document must not leave a stale policy in a worker")
	assert.Equal(t, ecstorage.DataShardsCount, NewDefaultConfig().ResolveDataShardsFor("other"))
}
