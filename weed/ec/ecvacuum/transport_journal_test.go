package ecvacuum

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// shardWatch is a minimal volume server for the redistribute tests: it records the
// RPC sequence Redistribute drives (which extension was pushed, with the shard id
// and disk id the server was told) and, like the real ReceiveFile, it writes the
// pushed file into its own directory — so a test can assert what happened to the
// on-disk .ecj, not merely which RPCs were attempted.
type shardWatch struct {
	volume_server_pb.UnimplementedVolumeServerServer

	baseDir string
	address string
	mu      sync.Mutex
	events  []string
	pushed  map[string]pushInfo
	// disks records the disk id of every push of an extension, in order, so a
	// test can see a sidecar pushed once per disk group.
	disks map[string][]uint32
}

type pushInfo struct {
	bytes   uint64
	count   int
	shardID uint32
	diskID  uint32
}

// newShardWatch starts a watch server whose address sorts before deadHolder
// ("127.0.0.1:1"), so the live holder is always pushed first.
func newShardWatch(t *testing.T) *shardWatch {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	sv := &shardWatch{baseDir: t.TempDir(), pushed: map[string]pushInfo{}, disks: map[string][]uint32{}}
	srv := pb.NewGrpcServer()
	volume_server_pb.RegisterVolumeServerServer(srv, sv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	// The "0.<port>" form is what ServerAddress.ToGrpcAddress() strips back to a
	// dialable host:port, and it sorts before the dead holder's ":1".
	sv.address = fmt.Sprintf("127.0.0.1:0.%d", lis.Addr().(*net.TCPAddr).Port)
	return sv
}

func (w *shardWatch) Address() string { return w.address }

func (w *shardWatch) record(event string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, event)
}

func (w *shardWatch) snapshotEvents() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.events...)
}

func (w *shardWatch) pushedInfo(ext string) pushInfo {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pushed[ext]
}

// pushedDisks returns the disk id of every push of an extension, in order.
func (w *shardWatch) pushedDisks(ext string) []uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]uint32(nil), w.disks[ext]...)
}

// diskFile reads back what the server holds for an extension.
func (w *shardWatch) diskFile(vid uint32, ext string) ([]byte, error) {
	return os.ReadFile(filepath.Join(w.baseDir, fmt.Sprintf("%d%s", vid, ext)))
}

func (w *shardWatch) ReceiveFile(stream volume_server_pb.VolumeServer_ReceiveFileServer) error {
	var (
		info *volume_server_pb.ReceiveFileInfo
		file *os.File
		n    uint64
	)
	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if info == nil {
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{Error: "missing file info"})
			}
			w.mu.Lock()
			prev := w.pushed[info.GetExt()]
			w.pushed[info.GetExt()] = pushInfo{
				bytes:   n,
				count:   prev.count + 1,
				shardID: info.GetShardId(),
				diskID:  info.GetDiskId(),
			}
			w.disks[info.GetExt()] = append(w.disks[info.GetExt()], info.GetDiskId())
			w.mu.Unlock()
			w.record("push:" + info.GetExt())
			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{BytesWritten: n})
		}
		if err != nil {
			return err
		}
		if i := req.GetInfo(); i != nil {
			info = i
			path := filepath.Join(w.baseDir, fmt.Sprintf("%d%s", i.GetVolumeId(), i.GetExt()))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return err
			}
			if file, err = os.Create(path); err != nil {
				return err
			}
			continue
		}
		if file == nil {
			return fmt.Errorf("file info not received")
		}
		written, werr := file.Write(req.GetFileContent())
		if werr != nil {
			return werr
		}
		n += uint64(written)
	}
}

func (w *shardWatch) VolumeEcShardsUnmount(context.Context, *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	w.record("unmount")
	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (w *shardWatch) VolumeEcShardsMount(context.Context, *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {
	w.record("mount")
	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

// deadHolder is a holder address nothing listens on.
const deadHolder = "127.0.0.1:1"

// redistributeFixture builds a real encoded volume (20 needles, 10 deleted) and
// returns its directory and shard base name.
func redistributeFixture(t *testing.T) (dir, base string) {
	t.Helper()
	dir = t.TempDir()
	buildEncodedFixture(t, dir, "c1", 7, 20, 10)
	return dir, ShardBaseName("c1", 7)
}

func newRedistributeTransport(t *testing.T) *ClusterTransport {
	t.Helper()
	return NewClusterTransport(grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// redistributeTarget puts the given shard ids on one holder, each on its own disk.
func redistributeTarget(holder string, diskOf func(shardID uint32) uint32) VacuumTarget {
	target := VacuumTarget{VolumeID: 7, Collection: "c1", DataShards: 10, ParityShards: 4,
		Holders: map[uint32]string{}, DiskIDs: map[uint32]uint32{}}
	for i := uint32(0); i < 14; i++ {
		target.Holders[i] = holder
		target.DiskIDs[i] = diskOf(i)
	}
	return target
}

// TestRedistributeKeepsDeleteJournalsWhenAPushFails is the regression test for
// the rollback hazard: a push that fails on a later holder must leave the earlier
// holders' delete journals completely untouched. Clearing a holder's journal
// cannot be undone — the collected originals carry only the *merged* journal, not
// the per-holder subsets — so a journal cleared before the failure would revert
// that holder's deletes while the restored (original) .ecx served the needles
// again.
func TestRedistributeKeepsDeleteJournalsWhenAPushFails(t *testing.T) {
	dir, base := redistributeFixture(t)
	live := newShardWatch(t)

	// The holder's own delete journal, as a real one is on disk.
	journal := []byte("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, os.WriteFile(filepath.Join(live.baseDir, "7.ecj"), journal, 0o644))

	target := redistributeTarget(live.Address(), func(uint32) uint32 { return 0 })
	for i := uint32(10); i < 14; i++ {
		target.Holders[i] = deadHolder
	}

	ctx := context.Background()
	transport := newRedistributeTransport(t)
	err := transport.Redistribute(ctx, target, dir, base, true)
	require.Error(t, err, "the dead holder must fail the push")
	require.Zero(t, live.pushedInfo(".ecj").count,
		"the journal clear must not run when a shard push failed")

	// The rollback run.go performs on a failed push.
	require.Error(t, transport.Redistribute(ctx, target, dir, base, false))
	require.Zero(t, live.pushedInfo(".ecj").count, "the rollback must not push a journal either")

	onDisk, err := live.diskFile(7, ".ecj")
	require.NoError(t, err)
	require.Equal(t, journal, onDisk, "the holder's deletes must survive a failed vacuum")
}

// TestRedistributeClearsJournalsOnlyAfterEveryHolderIsMounted verifies the order:
// the empty journal lands after the new generation is complete and mounted, so a
// failure anywhere in phase 1 leaves every journal intact.
func TestRedistributeClearsJournalsOnlyAfterEveryHolderIsMounted(t *testing.T) {
	dir, base := redistributeFixture(t)
	live := newShardWatch(t)
	require.NoError(t, os.WriteFile(filepath.Join(live.baseDir, "7.ecj"), []byte("0123456789abcdef"), 0o644))

	target := redistributeTarget(live.Address(), func(uint32) uint32 { return 0 })
	require.NoError(t, newRedistributeTransport(t).Redistribute(context.Background(), target, dir, base, true))

	events := live.snapshotEvents()
	require.GreaterOrEqual(t, len(events), 4)
	require.Equal(t, []string{"mount", "unmount", "push:.ecj", "mount"}, events[len(events)-4:],
		"the clear is a second pass: the shards are mounted, unmounted again, the journal replaced, and remounted")

	onDisk, err := live.diskFile(7, ".ecj")
	require.NoError(t, err)
	require.Empty(t, onDisk, "a successful vacuum spends the deletes")
	require.EqualValues(t, 0, live.pushedInfo(".ecj").bytes)
}

// TestRedistributeSendsEachShardToItsOwnDisk verifies the disk id travels with
// each shard. Without it ReceiveFile auto-selects a disk, which on a multi-disk
// node can move a shard to a sibling and leave the old shard file behind as an
// orphan copy.
func TestRedistributeSendsEachShardToItsOwnDisk(t *testing.T) {
	dir, base := redistributeFixture(t)
	live := newShardWatch(t)

	// One node, two disks: shards 0-9 on disk 2, the parity shards on disk 5.
	target := redistributeTarget(live.Address(), func(shardID uint32) uint32 {
		if shardID < 10 {
			return 2
		}
		return 5
	})
	require.NoError(t, newRedistributeTransport(t).Redistribute(context.Background(), target, dir, base, true))

	require.EqualValues(t, 2, live.pushedInfo(".ec00").diskID)
	require.EqualValues(t, 5, live.pushedInfo(".ec13").diskID)
	require.Equal(t, []uint32{2, 5}, live.pushedDisks(".ecx"),
		"each group's shards and sidecars go back to that group's disk")

	require.Equal(t, []uint32{2, 5}, live.pushedDisks(".ecj"),
		"each group's delete journal is spent on its own disk")
	events := live.snapshotEvents()
	require.GreaterOrEqual(t, len(events), 4)
	require.Equal(t, []string{"unmount", "push:.ecj", "push:.ecj", "mount"}, events[len(events)-4:],
		"one node-wide unmount clears every disk's journal, then the whole set is remounted")
}
