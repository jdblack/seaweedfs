package ecvacuum

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
)

// VacuumTarget describes one volume to vacuum together with its shard placement
// (shard id -> holder gRPC address) for the newest encode generation.
type VacuumTarget struct {
	VolumeID     uint32
	Collection   string
	DiskType     string
	DataShards   int
	ParityShards int
	EncodeTsNs   int64
	Holders      map[uint32]string
}

// sortedShardIDs returns the target's shard ids in ascending order for
// deterministic collection and redistribution.
func (t VacuumTarget) sortedShardIDs() []uint32 {
	ids := make([]uint32, 0, len(t.Holders))
	for id := range t.Holders {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ShardTransport moves EC shards and their sidecars between the worker and the
// volume servers. It is an interface so the handler can be driven by test
// doubles without a live cluster.
type ShardTransport interface {
	// Collect pulls the target's shards and sidecars into dir, writing them under
	// the <base><ext> names the storage helpers expect, and returns the shard ids
	// actually placed on disk.
	Collect(ctx context.Context, target VacuumTarget, dir, base string) ([]uint32, error)
	// Redistribute unmounts each holder's old shards, pushes the compacted shards
	// and sidecars, and mounts them so the holders serve the new generation. When
	// clearJournal is set, each holder's .ecj delete journal is overwritten with an
	// empty one (the vacuum has spent those deletes); the rollback path passes
	// false to keep the holders' journals, which still describe a real generation.
	Redistribute(ctx context.Context, target VacuumTarget, dir, base string, clearJournal bool) error
}

// ClusterTransport is the production ShardTransport, driving the same
// volume-server RPCs the ec.encode / ec.decode shell paths use.
type ClusterTransport struct {
	dialOpt grpc.DialOption
}

// NewClusterTransport returns a ShardTransport that drives the volume-server
// shard RPCs over gRPC, dialing every holder with dialOpt.
func NewClusterTransport(dialOpt grpc.DialOption) *ClusterTransport {
	return &ClusterTransport{dialOpt: dialOpt}
}

// Collect copies every shard and the .ecx/.ecj/.vif/.ecsum sidecars off the
// holders into the worker's working directory. Sidecars are best-effort: a
// volume that never had a bitrot sidecar simply has no .ecsum to fetch.
func (c *ClusterTransport) Collect(ctx context.Context, target VacuumTarget, dir, base string) ([]uint32, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ids := target.sortedShardIDs()
	if len(ids) == 0 {
		return nil, fmt.Errorf("no shards reported for volume %d", target.VolumeID)
	}

	var collected []uint32
	for _, id := range ids {
		holder := target.Holders[id]
		dest := filepath.Join(dir, base+erasure_coding.ToExt(int(id)))
		if err := c.copyEcFileFromHolder(ctx, holder, target.VolumeID, target.Collection, erasure_coding.ToExt(int(id)), dest); err != nil {
			return collected, fmt.Errorf("collect shard %d from %s: %w", id, holder, err)
		}
		collected = append(collected, id)
	}

	// The .ecx/.vif/.ecsum sidecars are shared: every holder keeps an identical
	// copy, so reading one holder is enough (and a missing one is a no-op).
	sidecarHolder := target.Holders[ids[0]]
	for _, ext := range []string{".ecx", ".vif", ".ecsum"} {
		dest := filepath.Join(dir, base+ext)
		if err := c.copyEcFileFromHolder(ctx, sidecarHolder, target.VolumeID, target.Collection, ext, dest); err != nil {
			return collected, fmt.Errorf("collect sidecar %s: %w", ext, err)
		}
	}

	// The .ecj delete journal is NOT shared: each delete is tombstoned on exactly
	// one holder (the needle's primary data-shard owner), so every holder's
	// journal holds a disjoint subset of the volume's deletes. Folding only one
	// holder's journal — as a plain sidecar fetch would — leaves the other
	// holders' deletes applied to the shards but invisible to the re-encode, so
	// the volume only ever shrinks by one holder's worth.
	if err := c.collectMergedEcj(ctx, target, dir, base); err != nil {
		return collected, err
	}
	return collected, nil
}

// collectMergedEcj fetches the .ecj deletion journal from every shard holder and
// concatenates the entries into one local journal, so RebuildEcxFile folds the
// volume's complete set of deletes. Each holder is visited once (a holder with
// several shards keeps a single shared journal) in ascending shard-id order, for
// a deterministic result. Absent or empty journals contribute nothing.
func (c *ClusterTransport) collectMergedEcj(ctx context.Context, target VacuumTarget, dir, base string) error {
	dest := filepath.Join(dir, base+".ecj")
	out, err := os.Create(dest)
	if err != nil {
		return err
	}

	seen := make(map[string]bool, len(target.Holders))
	for _, id := range target.sortedShardIDs() {
		holder := target.Holders[id]
		if seen[holder] {
			continue
		}
		seen[holder] = true

		part := fmt.Sprintf("%s.ecj.%d", dest, id)
		if err := c.copyEcFileFromHolder(ctx, holder, target.VolumeID, target.Collection, ".ecj", part); err != nil {
			out.Close()
			return fmt.Errorf("collect .ecj from %s: %w", holder, err)
		}
		// copyEcFileFromHolder removes a 0-byte file, so a holder with no journal
		// simply contributes nothing.
		in, openErr := os.Open(part)
		if openErr != nil {
			if os.IsNotExist(openErr) {
				continue
			}
			out.Close()
			return openErr
		}
		// A torn journal (a partial final append) would shift every following
		// entry, so folding it into the merged stream could mask the wrong
		// needles. Refuse it rather than merge a misaligned journal.
		if fi, statErr := in.Stat(); statErr != nil {
			in.Close()
			out.Close()
			return statErr
		} else if fi.Size()%types.NeedleIdSize != 0 {
			in.Close()
			out.Close()
			return fmt.Errorf("holder %s journal for EC volume %d is %d bytes, not a multiple of %d", holder, target.VolumeID, fi.Size(), types.NeedleIdSize)
		}
		_, copyErr := io.Copy(out, in)
		in.Close()
		_ = os.Remove(part)
		if copyErr != nil {
			out.Close()
			return copyErr
		}
	}

	if closeErr := out.Close(); closeErr != nil {
		return closeErr
	}
	// No deletes anywhere: leave no journal rather than a stray 0-byte file.
	if fi, statErr := os.Stat(dest); statErr == nil && fi.Size() == 0 {
		_ = os.Remove(dest)
	}
	return nil
}

// copyEcFileFromHolder streams one EC file (shard or sidecar) from a holder to
// destPath. A missing source leaves no file behind rather than an empty stub.
func (c *ClusterTransport) copyEcFileFromHolder(ctx context.Context, holder string, volumeID uint32, collection, ext, destPath string) error {
	const missing = true
	err := operation.WithVolumeServerClient(false, pb.ServerAddress(holder), c.dialOpt,
		func(client volume_server_pb.VolumeServerClient) error {
			stream, err := client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
				VolumeId:                 volumeID,
				Collection:               collection,
				Ext:                      ext,
				IsEcVolume:               true,
				StopOffset:               math.MaxInt64,
				IgnoreSourceFileNotFound: missing,
			})
			if err != nil {
				return err
			}
			f, err := os.Create(destPath)
			if err != nil {
				return err
			}
			defer f.Close()
			for {
				resp, rerr := stream.Recv()
				if rerr == io.EOF {
					return nil
				}
				if rerr != nil {
					return rerr
				}
				if len(resp.GetFileContent()) == 0 {
					continue
				}
				if _, werr := f.Write(resp.GetFileContent()); werr != nil {
					return werr
				}
			}
		})
	if err != nil {
		return err
	}
	// A source that does not exist yields an empty file; drop it so the storage
	// helpers see "absent" rather than "0 bytes".
	if fi, statErr := os.Stat(destPath); statErr == nil && fi.Size() == 0 {
		_ = os.Remove(destPath)
	}
	return nil
}

// Redistribute replaces the holders' shards with the compacted set: it unmounts
// each holder's shards, pushes the compacted shards and sidecars, then mounts
// them. The shard ids are unchanged (the ratio is preserved), so each compacted
// shard overwrites its predecessor in place. A reader that hits a momentarily
// unmounted shard falls back to Reed-Solomon reconstruction.
//
// When clearJournal is set, the re-encoded shards no longer contain the deleted
// needles, so each holder's .ecj delete journal is overwritten with an empty one.
// Leaving the spent deletes behind would keep the volume reporting a non-zero
// delete count for needles that are gone (misleading vacuum detection and anyone
// inspecting the volume). The overwrite happens while the shards are unmounted,
// which ReceiveFile requires.
func (c *ClusterTransport) Redistribute(ctx context.Context, target VacuumTarget, dir, base string, clearJournal bool) error {
	byHolder := make(map[string][]uint32)
	var holders []string
	for _, id := range target.sortedShardIDs() {
		h := target.Holders[id]
		if _, ok := byHolder[h]; !ok {
			holders = append(holders, h)
		}
		byHolder[h] = append(byHolder[h], id)
	}
	sort.Strings(holders)

	if clearJournal {
		if err := os.WriteFile(filepath.Join(dir, base+".ecj"), nil, 0o644); err != nil {
			return fmt.Errorf("create empty delete journal: %w", err)
		}
	}

	for _, holder := range holders {
		ids := byHolder[holder]
		if err := c.unmountShards(ctx, holder, target.VolumeID, ids); err != nil {
			return fmt.Errorf("unmount old shards on %s: %w", holder, err)
		}
		for _, id := range ids {
			src := filepath.Join(dir, base+erasure_coding.ToExt(int(id)))
			if err := c.pushEcFile(ctx, holder, target, erasure_coding.ToExt(int(id)), id, src); err != nil {
				return fmt.Errorf("push shard %d to %s: %w", id, holder, err)
			}
		}
		// Each holder keeps a self-contained copy of the shared sidecars.
		for _, ext := range []string{".ecx", ".vif", ".ecsum"} {
			src := filepath.Join(dir, base+ext)
			if _, err := os.Stat(src); err != nil {
				continue
			}
			if err := c.pushEcFile(ctx, holder, target, ext, 0, src); err != nil {
				return fmt.Errorf("push sidecar %s to %s: %w", ext, holder, err)
			}
		}
		if clearJournal {
			if err := c.pushEcFile(ctx, holder, target, ".ecj", 0, filepath.Join(dir, base+".ecj")); err != nil {
				return fmt.Errorf("clear delete journal on %s: %w", holder, err)
			}
		}
		if err := c.mountShards(ctx, holder, target, ids); err != nil {
			return fmt.Errorf("mount compacted shards on %s: %w", holder, err)
		}
	}
	return nil
}

func (c *ClusterTransport) unmountShards(ctx context.Context, holder string, volumeID uint32, ids []uint32) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(holder), c.dialOpt,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
				VolumeId: volumeID,
				ShardIds: ids,
			})
			return err
		})
}

func (c *ClusterTransport) mountShards(ctx context.Context, holder string, target VacuumTarget, ids []uint32) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(holder), c.dialOpt,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
				VolumeId:       target.VolumeID,
				Collection:     target.Collection,
				ShardIds:       ids,
				SourceDiskType: target.DiskType,
			})
			return err
		})
}

// pushEcFile streams one EC shard or sidecar to a holder via ReceiveFile.
func (c *ClusterTransport) pushEcFile(ctx context.Context, holder string, target VacuumTarget, ext string, shardID uint32, srcPath string) error {
	fi, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return operation.WithVolumeServerClient(false, pb.ServerAddress(holder), c.dialOpt,
		func(client volume_server_pb.VolumeServerClient) error {
			stream, err := client.ReceiveFile(ctx)
			if err != nil {
				return err
			}
			if err := stream.Send(&volume_server_pb.ReceiveFileRequest{
				Data: &volume_server_pb.ReceiveFileRequest_Info{Info: &volume_server_pb.ReceiveFileInfo{
					VolumeId:   target.VolumeID,
					Collection: target.Collection,
					Ext:        ext,
					IsEcVolume: true,
					ShardId:    shardID,
					FileSize:   uint64(fi.Size()),
				}},
			}); err != nil {
				return err
			}
			buf := make([]byte, 64*1024)
			for {
				n, rerr := f.Read(buf)
				if n > 0 {
					if serr := stream.Send(&volume_server_pb.ReceiveFileRequest{
						Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: buf[:n]},
					}); serr != nil {
						return serr
					}
				}
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					return rerr
				}
			}
			resp, cerr := stream.CloseAndRecv()
			if cerr != nil {
				return cerr
			}
			if resp.GetError() != "" {
				return fmt.Errorf("receive %s: %s", ext, resp.GetError())
			}
			return nil
		})
}
