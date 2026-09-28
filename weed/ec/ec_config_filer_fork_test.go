package ec

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
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// fakeECConfigFiler is an in-memory filer with just enough of the API for the
// policy document: lookup, create, update. Entries are keyed by "dir/name", and
// directories are entries too, so the helper's directory bootstrap is exercised.
type fakeECConfigFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu      sync.Mutex
	entries map[string]*filer_pb.Entry
}

func fakeEntryKey(dir, name string) string { return dir + "/" + name }

func (f *fakeECConfigFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, found := f.entries[fakeEntryKey(req.Directory, req.Name)]
	if !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (f *fakeECConfigFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[fakeEntryKey(req.Directory, req.Entry.Name)] = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *fakeECConfigFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[fakeEntryKey(req.Directory, req.Entry.Name)] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *fakeECConfigFiler) put(dir, name string, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[fakeEntryKey(dir, name)] = &filer_pb.Entry{Name: name, Content: content}
}

func (f *fakeECConfigFiler) get(dir, name string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, found := f.entries[fakeEntryKey(dir, name)]
	if !found {
		return nil, false
	}
	return entry.Content, true
}

func (f *fakeECConfigFiler) has(dir, name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, found := f.entries[fakeEntryKey(dir, name)]
	return found
}

// startFakeECConfigFiler serves the fake on a loopback port and returns the
// "host:httpPort.grpcPort" address the cluster uses, so the helper's dial
// resolves to it.
func startFakeECConfigFiler(t *testing.T) (*fakeECConfigFiler, pb.ServerAddress) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	f := &fakeECConfigFiler{entries: map[string]*filer_pb.Entry{}}
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

func testECConfigDialOption() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// The admin dashboard and `ec.config` write this document; any process about to
// encode reads it back. A round trip through the filer must reproduce the policy
// exactly, and create the directory the file lives in when the filer is fresh.
func TestSaveAndLoadECConfigThroughFiler(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	filerServer, filerAddress := startFakeECConfigFiler(t)

	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))
	require.NoError(t, erasure_coding.SetCollectionECConfig("blender-blender", 5, 1))
	require.NoError(t, SaveECConfigToFiler(testECConfigDialOption(), filerAddress))

	dir, name := ecConfigDirAndName()
	assert.Equal(t, "/etc/seaweedfs", dir)
	assert.Equal(t, "ec_ratio.json", name)
	assert.True(t, filerServer.has("/", "etc"), "the helper bootstraps a missing /etc")
	assert.True(t, filerServer.has("/etc", "seaweedfs"), "and the missing /etc/seaweedfs")
	content, found := filerServer.get(dir, name)
	require.True(t, found)
	assert.Contains(t, string(content), `"data_shards": 3`)
	assert.Contains(t, string(content), `"blender-blender"`)

	// Another process (a shell, an admin) loads it.
	erasure_coding.ResetECConfig()
	require.NoError(t, LoadECConfigFromFiler(testECConfigDialOption(), filerAddress))

	global, found := erasure_coding.GlobalECConfig()
	require.True(t, found)
	assert.Equal(t, erasure_coding.ECConfigEntry{DataShards: 3, ParityShards: 2}, global)
	override, found := erasure_coding.ResolveECConfig("blender-blender")
	require.True(t, found)
	assert.Equal(t, erasure_coding.ECConfigEntry{DataShards: 5, ParityShards: 1}, override)
}

// A filer with no document means "no policy": the process falls back to the
// build default rather than keeping a policy from an earlier load.
func TestLoadECConfigFromEmptyFilerClearsPolicy(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	_, filerAddress := startFakeECConfigFiler(t)

	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))
	require.NoError(t, LoadECConfigFromFiler(testECConfigDialOption(), filerAddress))

	_, found := erasure_coding.GlobalECConfig()
	assert.False(t, found, "a deleted document must not leave a stale policy in memory")
}

// A malformed document is an error, and it must not disturb what the process had
// loaded before: encoding under a layout nobody chose is the failure this whole
// feature exists to prevent.
func TestLoadECConfigRejectsMalformedDocument(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	filerServer, filerAddress := startFakeECConfigFiler(t)

	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))
	dir, name := ecConfigDirAndName()
	filerServer.put(dir, name, []byte(`{"global":{"data_shards":0,"parity_shards":2}}`))

	err := LoadECConfigFromFiler(testECConfigDialOption(), filerAddress)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "global EC ratio")

	global, found := erasure_coding.GlobalECConfig()
	require.True(t, found, "the previously loaded policy must survive a rejected document")
	assert.Equal(t, erasure_coding.ECConfigEntry{DataShards: 3, ParityShards: 2}, global)

	filerServer.put(dir, name, []byte(`not json at all`))
	assert.Error(t, LoadECConfigFromFiler(testECConfigDialOption(), filerAddress))
}
