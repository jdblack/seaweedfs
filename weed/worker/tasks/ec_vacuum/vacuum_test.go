package ec_vacuum

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// appendNeedle writes one needle into a live volume via the public
// WriteNeedleBlob path (which also updates the .idx needle map).
func appendNeedle(t *testing.T, v *storage.Volume, dir string, id uint64, data []byte) {
	t.Helper()
	n := &needle.Needle{Id: types.NeedleId(id), Data: data, Cookie: types.Cookie(id), Ttl: needle.EMPTY_TTL}
	n.Checksum = needle.NewCRC(data)

	scratch, err := os.CreateTemp(dir, "needle-blob")
	require.NoError(t, err)
	scratchName := scratch.Name()
	defer os.Remove(scratchName)
	defer scratch.Close()

	_, _, _, err = n.Append(backend.NewDiskFile(scratch), v.Version())
	require.NoError(t, err)
	blob, err := os.ReadFile(scratchName)
	require.NoError(t, err)
	require.NoError(t, v.WriteNeedleBlob(types.NeedleId(id), blob, n.Size))
}

// buildEncodedFixture creates a real volume with `live` needles, encodes it into
// EC shards, and (simulating runtime deletes) journals the first `deleted`
// needle ids to .ecj — exactly the on-disk shape ec_vacuum must reclaim.
func buildEncodedFixture(t *testing.T, dir, collection string, vid uint32, live, deleted int) vacuumOptions {
	t.Helper()

	rp, err := super_block.NewReplicaPlacementFromString("000")
	require.NoError(t, err)
	v, err := storage.NewVolume(dir, dir, collection, needle.VolumeId(vid), storage.NeedleMapInMemory, rp, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)

	data := bytes.Repeat([]byte("abcdefgh"), 128*1024) // 1 MiB per needle
	for i := 1; i <= live; i++ {
		appendNeedle(t, v, dir, uint64(i), data)
	}
	v.Close()

	base := filepath.Join(dir, shardBaseName(collection, vid))
	require.NoError(t, erasure_coding.WriteSortedFileFromIdx(base, ".ecx"))

	ctx := erasure_coding.NewDefaultECContext(collection, needle.VolumeId(vid))
	_, err = erasure_coding.WriteEcFiles(base, ctx)
	require.NoError(t, err)

	require.NoError(t, volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(v.Version()),
		DatFileSize: ctx.DatFileSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   uint32(ctx.DataShards),
			ParityShards: uint32(ctx.ParityShards),
			BlockSize:    ctx.BlockSize,
		},
	}))

	if deleted > 0 {
		f, err := os.Create(base + ".ecj")
		require.NoError(t, err)
		for i := 1; i <= deleted; i++ {
			b := make([]byte, types.NeedleIdSize)
			types.NeedleIdToBytes(b, types.NeedleId(i))
			_, err = f.Write(b)
			require.NoError(t, err)
		}
		require.NoError(t, f.Close())
	}

	return vacuumOptions{
		VolumeID:           vid,
		Collection:         collection,
		DataShards:         ctx.DataShards,
		ParityShards:       ctx.ParityShards,
		EncodedDatFileSize: ctx.DatFileSize,
		BlockSize:          ctx.BlockSize,
		Version:            uint32(v.Version()),
		DiskType:           types.HardDriveType,
	}
}

// TestVacuumLocalDirReclaimsDeletedSpace drives the full worker-local pipeline:
// decode the shards, compact the deleted needles, re-encode, and confirm the
// shard set shrank and only live needles survive.
func TestVacuumLocalDirReclaimsDeletedSpace(t *testing.T) {
	dir := t.TempDir()
	opts := buildEncodedFixture(t, dir, "c1", 1, 20, 10) // 20 needles, 10 deleted
	base := shardBaseName("c1", 1)

	res, err := vacuumLocalDir(dir, base, opts)
	require.NoError(t, err)
	require.Greater(t, res.OldShardBytes, res.NewShardBytes, "compaction must shrink the shard set")

	// The new .ecx must describe only the 10 live needles.
	fi, err := os.Stat(filepath.Join(dir, base+".ecx"))
	require.NoError(t, err)
	require.EqualValues(t, 10, fi.Size()/int64(types.NeedleMapEntrySize))

	// The deletion journal is folded in, and the temporary decoded volume is gone.
	require.NoFileExists(t, filepath.Join(dir, base+".ecj"))
	require.NoFileExists(t, filepath.Join(dir, base+".dat"))
	require.NoFileExists(t, filepath.Join(dir, base+".idx"))

	// A fresh .vif with a new encode generation must exist for the new shards.
	vi, _, found, err := volume_info.MaybeLoadVolumeInfo(filepath.Join(dir, base+".vif"))
	require.NoError(t, err)
	require.True(t, found)
	require.NotZero(t, vi.GetEcShardConfig().GetEncodeTsNs())
}

// TestVacuumLocalDirNoLiveNeedles verifies an all-deleted volume returns the
// no-op sentinel rather than producing an empty volume.
func TestVacuumLocalDirNoLiveNeedles(t *testing.T) {
	dir := t.TempDir()
	opts := buildEncodedFixture(t, dir, "c1", 2, 5, 5) // every needle deleted
	base := shardBaseName("c1", 2)

	_, err := vacuumLocalDir(dir, base, opts)
	require.ErrorContains(t, err, erasure_coding.EcNoLiveEntriesSubstring)
}

// TestVacuumLocalDirStopsDiskProber guards against the scratch-dir prober leak:
// compactDecodedVolume needs a *storage.DiskLocation for Store.CompactVolumeFiles
// and builds it with the volume server's constructor. That constructor starts a
// statfs prober which only DiskLocation.Close stops, while VacuumVolume deletes
// the scratch dir the moment it finishes (run.go's defer os.RemoveAll(root)).
// Without the close, the prober keeps statfs-ing the removed dir once a minute
// for the life of the process and pins
// volumeServer_disk_error_status{<dir>,type="error"} = 1 on the worker's
// /metrics, alongside four volumeServer_resource children.
//
// The prober probes once immediately, before any 60s tick, so both symptoms are
// observable without waiting: the five gauge children appear at once, and the
// prober goroutine outlives the call. Assert on both.
func TestVacuumLocalDirStopsDiskProber(t *testing.T) {
	dir := t.TempDir()
	opts := buildEncodedFixture(t, dir, "c1", 3, 20, 10)
	base := shardBaseName("c1", 3)

	t.Cleanup(func() {
		stats.VolumeServerDiskErrorGauge.DeleteLabelValues(dir, "error")
		for _, k := range []string{"all", "used", "free", "avail"} {
			stats.VolumeServerResourceGauge.DeleteLabelValues(dir, k)
		}
	})

	// The gauges are process-wide, so snapshot instead of assuming them empty.
	diskErrorsBefore := testutil.CollectAndCount(stats.VolumeServerDiskErrorGauge)
	resourcesBefore := testutil.CollectAndCount(stats.VolumeServerResourceGauge)
	goroutinesBefore := runtime.NumGoroutine()

	_, err := vacuumLocalDir(dir, base, opts)
	require.NoError(t, err)

	// A leaked prober publishes its first probe's children immediately; a closed
	// one leaves the vectors untouched.
	require.Never(t, func() bool {
		return testutil.CollectAndCount(stats.VolumeServerDiskErrorGauge) > diskErrorsBefore ||
			testutil.CollectAndCount(stats.VolumeServerResourceGauge) > resourcesBefore
	}, 2*time.Second, 50*time.Millisecond, "vacuum local dir published disk-probe gauge children for its scratch dir")

	// The scratch constructor starts nothing, so it also writes no vol_dir.uuid.
	require.NoFileExists(t, filepath.Join(dir, storage.UUIDFileName), "scratch dir must not carry a vol_dir.uuid")

	// Sample the process-wide goroutine count from this goroutine rather than from
	// inside a require.Eventually condition, whose evaluation goroutine would count
	// itself. The prober, if leaked, is still alive here: it loops for 60s without
	// a Close.
	var leaked int
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		if leaked = runtime.NumGoroutine() - goroutinesBefore; leaked <= 0 {
			leaked = 0
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Zero(t, leaked, "vacuum local dir left %d goroutine(s) behind (leaked disk prober)", leaked)
}
