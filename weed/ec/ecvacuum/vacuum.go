package ecvacuum

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// Options carries the per-volume inputs resolved from the .vif and the
// master topology.
type Options struct {
	VolumeID     uint32
	Collection   string
	DataShards   int
	ParityShards int
	// EncodedDatFileSize is the .dat length at encode time (from .vif), which
	// fixed the shard block layout. 0 falls back to inferring the layout from
	// the shard size.
	EncodedDatFileSize int64
	// BlockSize is the uniform shard block size from .vif (0 = legacy layout).
	BlockSize   int64
	Version     uint32
	ExpireAtSec uint64
	DiskType    types.DiskType
}

// Result reports what a local vacuum reclaimed.
type Result struct {
	OldShardBytes int64
	NewShardBytes int64
	LiveNeedles   uint64
}

// HasAllDataShards reports whether every data shard id (0..dataShards-1) is
// present. WriteDatFile reads only data shards, so a volume missing any of them
// cannot be decoded locally and must be left for ec.rebuild instead.
func HasAllDataShards(bits erasure_coding.ShardBits, dataShards int) bool {
	for i := 0; i < dataShards; i++ {
		if !bits.Has(erasure_coding.ShardId(i)) {
			return false
		}
	}
	return true
}

// ShardBaseName returns the on-disk base name shared by a volume's EC shards and
// its decoded .dat/.idx (both use the <collection>_<vid> convention).
func ShardBaseName(collection string, volumeID uint32) string {
	return erasure_coding.EcShardBaseFileName(collection, int(volumeID))
}

// totalShardBytes sums the sizes of the shards that currently exist for the
// volume's layout in dir.
func totalShardBytes(dir, base string, total int) int64 {
	var sum int64
	for i := 0; i < total; i++ {
		if fi, err := os.Stat(filepath.Join(dir, base+erasure_coding.ToExt(i))); err == nil {
			sum += fi.Size()
		}
	}
	return sum
}

// largeSmallBlockSizes returns the shard block layout for a volume, honoring the
// uniform .vif block size when recorded.
func largeSmallBlockSizes(blockSize int64) (int64, int64) {
	if blockSize > 0 {
		return blockSize, blockSize
	}
	return erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize
}

// VacuumLocalDir compacts a volume whose shards have been collected into dir
// under base: it decodes the shards into a normal volume, compacts the deleted
// needles away, re-encodes a fresh shard set with the same ratio, and leaves the
// compacted shards plus their .ecx/.vif/.ecsum sidecars in dir. The decoded
// .dat/.idx are removed before returning, so dir holds only the new generation.
//
// Every step reuses the open-source storage primitives the volume server itself
// uses to decode (VolumeEcShardsToVolume) and encode (VolumeEcShardsGenerate), so
// the worker never invents its own shard byte layout.
func VacuumLocalDir(dir, base string, opts Options) (Result, error) {
	var res Result
	if opts.DataShards <= 0 || opts.ParityShards <= 0 ||
		!erasure_coding.ValidEcShardCounts(uint32(opts.DataShards), uint32(opts.ParityShards)) {
		return res, fmt.Errorf("invalid EC ratio %d+%d", opts.DataShards, opts.ParityShards)
	}
	total := opts.DataShards + opts.ParityShards
	// base is the <collection>_<vid> file-name prefix; fullBase joins it to the
	// working directory, which is what the storage helpers (and the decoded
	// .dat/.idx, named from collection+vid by the volume package) operate on.
	fullBase := filepath.Join(dir, base)

	dataShardFiles := make([]string, opts.DataShards)
	for i := 0; i < opts.DataShards; i++ {
		dataShardFiles[i] = fullBase + erasure_coding.ToExt(i)
		if _, err := os.Stat(dataShardFiles[i]); err != nil {
			return res, fmt.Errorf("data shard %d missing: %w", i, err)
		}
	}
	res.OldShardBytes = totalShardBytes(dir, base, total)

	// Fold the .ecj runtime deletes into .ecx so the index reflects the true
	// live set before decoding.
	if err := erasure_coding.RebuildEcxFile(fullBase); err != nil {
		return res, fmt.Errorf("rebuild ecx: %w", err)
	}
	hasLive, err := erasure_coding.HasLiveNeedles(fullBase)
	if err != nil {
		return res, fmt.Errorf("has live needles: %w", err)
	}
	if !hasLive {
		// An all-deleted volume is a no-op. Use the shared substring so the caller
		// recognises it the same way the ec.decode client recognises a server's
		// "no live entries" refusal.
		return res, fmt.Errorf("ec volume %d %s", opts.VolumeID, erasure_coding.EcNoLiveEntriesSubstring)
	}

	datFileSize, err := erasure_coding.FindDatFileSize(dataShardFiles[0], fullBase)
	if err != nil {
		return res, fmt.Errorf("find dat file size: %w", err)
	}
	largeBlockSize, smallBlockSize := largeSmallBlockSizes(opts.BlockSize)

	if err := erasure_coding.WriteDatFile(fullBase, datFileSize, opts.EncodedDatFileSize, dataShardFiles, largeBlockSize, smallBlockSize); err != nil {
		return res, fmt.Errorf("write dat file: %w", err)
	}
	if err := erasure_coding.VerifyDecodedDatFile(fullBase, datFileSize); err != nil {
		return res, err
	}
	if err := erasure_coding.WriteIdxFileFromEcIndex(fullBase); err != nil {
		return res, fmt.Errorf("write idx file: %w", err)
	}

	// The old EC generation is being replaced; drop its bitrot sidecars so a
	// stale .ecsum can never masquerade as the new generation's protection.
	erasure_coding.RemoveBitrotSidecars(fullBase)

	// Compact the decoded volume offline, in the worker's own directory — the
	// same Store.CompactVolumeFiles the server runs, but against the worker's
	// local copy so no volume server pays the CPU or disk cost.
	if err := compactDecodedVolume(dir, opts); err != nil {
		return res, fmt.Errorf("compact decoded volume: %w", err)
	}

	// Re-encode the compacted .dat into a fresh, deletes-free shard set. This
	// writes .ec00.. and leaves the layout it used on ctx.
	ctx := &erasure_coding.ECContext{
		DataShards:   opts.DataShards,
		ParityShards: opts.ParityShards,
		Collection:   opts.Collection,
		VolumeId:     needle.VolumeId(opts.VolumeID),
	}
	ecBitrot, err := erasure_coding.WriteEcFiles(fullBase, ctx)
	if err != nil {
		return res, fmt.Errorf("re-encode shards: %w", err)
	}
	if erasure_coding.BitrotProtectionEnabled && ecBitrot != nil {
		if serr := erasure_coding.SaveBitrotSidecar(erasure_coding.BitrotSidecarPath(fullBase, 0), ecBitrot); serr != nil {
			glog.Warningf("ec_vacuum: volume %d: failed to write bitrot sidecar: %v", opts.VolumeID, serr)
		}
	}

	// Rebuild .ecx from the compacted .idx (live needles only) and stamp a fresh
	// .vif generation so any shard still in flight from the old generation is
	// rejected by readers.
	if err := erasure_coding.WriteSortedFileFromIdx(fullBase, ".ecx"); err != nil {
		return res, fmt.Errorf("write ecx: %w", err)
	}
	vi := &volume_server_pb.VolumeInfo{
		Version:     opts.Version,
		ExpireAtSec: opts.ExpireAtSec,
		DatFileSize: ctx.DatFileSize,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   uint32(opts.DataShards),
			ParityShards: uint32(opts.ParityShards),
			EncodeTsNs:   time.Now().UnixNano(),
			BlockSize:    ctx.BlockSize,
		},
	}
	if err := volume_info.SaveVolumeInfo(fullBase+".vif", vi); err != nil {
		return res, fmt.Errorf("save vif: %w", err)
	}

	// Verify the re-encoded shards decode back to exactly the compacted data
	// before returning: this catches a truncated or mis-encoded shard here, in
	// the worker's own copy, rather than after the live shards have been
	// overwritten on the holders.
	if err := VerifyEncodedShards(fullBase); err != nil {
		stats.ECVacuumVerifyFailuresCounter.Inc()
		return res, fmt.Errorf("verify re-encoded shards: %w", err)
	}

	res.NewShardBytes = totalShardBytes(dir, base, total)

	// Leave only the new EC artifact set behind.
	for _, ext := range []string{".dat", ".idx", ".cpd", ".cpx", ".ecj"} {
		_ = os.Remove(fullBase + ext)
	}
	return res, nil
}

// compactDecodedVolume runs the standard offline volume compaction against the
// decoded .dat/.idx in dir. Store.CompactVolumeFiles uses none of its receiver
// state (it operates on the passed location), so a zero-value Store is a
// deliberate, documented reuse of that path rather than a re-implementation.
//
// The location comes from storage.NewScratchDiskLocation, not the volume
// server's NewDiskLocation. dir is scratch that VacuumVolume deletes with
// `defer os.RemoveAll(root)` (run.go), and NewDiskLocation starts a
// minute-interval statfs prober (and writes vol_dir.uuid) meant to live as long
// as the process; pointed at a deleted directory it would report a dead disk on
// every tick and pin volumeServer_disk_error_status{<dir>,type="error"} = 1 on
// the worker's /metrics. The scratch constructor starts nothing, so there is no
// prober to stop and no gauge child to delete afterwards.
func compactDecodedVolume(dir string, opts Options) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	location := storage.NewScratchDiskLocation(dir, dir, opts.DiskType)
	store := &storage.Store{}
	return store.CompactVolumeFiles(needle.VolumeId(opts.VolumeID), opts.Collection, location, storage.NeedleMapInMemory, 0, 0, 0)
}

// VerifyEncodedShards decodes a freshly re-encoded shard set back into a .dat and
// checks it is byte-identical (up to the live extent) to the compacted .dat it
// was encoded from. It is the vacuum's "verify before you replace" gate: it
// proves the compacted shards on the worker are complete and self-consistent
// before the caller overwrites the volume servers' live shards.
//
// fullBase is the file-name prefix (directory joined to the <collection>_<vid>
// base name) of the re-encoded set; the .vif, .dat, .ecx and .ec00.. are read
// from there.
func VerifyEncodedShards(fullBase string) error {
	vi, _, found, err := volume_info.MaybeLoadVolumeInfo(fullBase + ".vif")
	if err != nil {
		return fmt.Errorf("read vif: %w", err)
	}
	cfg := vi.GetEcShardConfig()
	if !found || cfg == nil {
		return fmt.Errorf("re-encoded volume is missing its EC config in %s.vif", filepath.Base(fullBase))
	}
	dataShards := int(cfg.GetDataShards())
	if dataShards <= 0 || dataShards > erasure_coding.MaxShardCount {
		return fmt.Errorf("re-encoded .vif has invalid data shard count %d", dataShards)
	}
	encodedSize := vi.GetDatFileSize()
	if encodedSize <= 0 {
		return fmt.Errorf("re-encoded .vif has no dat file size")
	}

	compactedDat := fullBase + ".dat"
	if fi, statErr := os.Stat(compactedDat); statErr != nil {
		return fmt.Errorf("compacted .dat missing: %w", statErr)
	} else if fi.Size() < encodedSize {
		return fmt.Errorf("compacted .dat is %d bytes, short of the encoded %d", fi.Size(), encodedSize)
	}

	dataShardFiles := make([]string, dataShards)
	for i := 0; i < dataShards; i++ {
		dataShardFiles[i] = fullBase + erasure_coding.ToExt(i)
		if _, statErr := os.Stat(dataShardFiles[i]); statErr != nil {
			return fmt.Errorf("re-encoded data shard %d missing: %w", i, statErr)
		}
	}

	datFileSize, err := erasure_coding.FindDatFileSize(dataShardFiles[0], fullBase)
	if err != nil {
		return fmt.Errorf("find dat file size: %w", err)
	}
	largeBlockSize, smallBlockSize := largeSmallBlockSizes(cfg.GetBlockSize())

	verifyBase := fullBase + ".verify"
	defer os.Remove(verifyBase + ".dat")
	if err := erasure_coding.WriteDatFile(verifyBase, datFileSize, encodedSize, dataShardFiles, largeBlockSize, smallBlockSize); err != nil {
		return fmt.Errorf("decode back the re-encoded shards: %w", err)
	}
	if err := erasure_coding.VerifyDecodedDatFile(verifyBase, datFileSize); err != nil {
		return err
	}

	want, err := crc32File(compactedDat, datFileSize)
	if err != nil {
		return err
	}
	got, err := crc32File(verifyBase+".dat", datFileSize)
	if err != nil {
		return err
	}
	if want != got {
		return fmt.Errorf("re-encoded shards decode to different bytes than the compacted volume (crc %08x != %08x)", got, want)
	}
	return nil
}

// crc32File streams up to limit bytes of a file through CRC32 and returns the
// checksum, without loading the whole file into memory.
func crc32File(path string, limit int64) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	buf := make([]byte, 1<<20)
	var read int64
	for read < limit {
		want := int64(len(buf))
		if remaining := limit - read; remaining < want {
			want = remaining
		}
		n, readErr := f.Read(buf[:want])
		if n > 0 {
			_, _ = h.Write(buf[:n])
			read += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
	}
	return h.Sum32(), nil
}

// copyEcArtifacts copies a volume's collected EC artifact set (shards, index,
// delete journal, and .vif/.ecsum sidecars) from srcDir to dstDir, so the
// vacuum can mutate a scratch copy while the collected originals stay intact for
// rollback. Absent optional sidecars (.ecj, .ecsum) and absent shards (a
// partially-redundant volume) are skipped rather than treated as errors.
func copyEcArtifacts(srcDir, dstDir, base string, total int) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	exts := make([]string, 0, total+4)
	for i := 0; i < total; i++ {
		exts = append(exts, erasure_coding.ToExt(i))
	}
	exts = append(exts, ".ecx", ".ecj", ".vif", ".ecsum")

	for _, ext := range exts {
		if err := streamCopyFile(filepath.Join(srcDir, base+ext), filepath.Join(dstDir, base+ext)); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("copy %s for scratch: %w", ext, err)
		}
	}
	return nil
}

// streamCopyFile copies src to dst in a bounded buffer rather than loading the
// whole file into memory, so a multi-gigabyte EC shard does not have to fit in
// the worker's heap.
func streamCopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
