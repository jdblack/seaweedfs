package ecvacuum

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
	Holders      map[uint32]string
	// DiskIDs is the disk each shard id sits on, as the volume server counts its
	// own -dir list (the topology's per-shard disk_id). Redistribute passes it to
	// ReceiveFile so a compacted shard overwrites its predecessor on the disk it
	// came from. With disk_id unset the server auto-selects a disk, which on a
	// multi-disk node can move the shard to a sibling and leave the old shard
	// file behind as an orphan copy — the very state that makes a volume
	// un-vacuumable (see duplicateShardIDs). An absent or zero entry keeps the
	// auto-selection, so a caller that does not know the disk still works.
	DiskIDs map[uint32]uint32
}

// holderDisk identifies one volume server disk. A shard belongs to exactly one
// disk of one node, so a node holding two disks' worth of a volume is two
// groups, each of which ReceiveFile must be told about explicitly.
type holderDisk struct {
	address string
	diskID  uint32
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
	// and sidecars, and mounts them so the holders serve the new generation. An
	// error means the volume is on mixed generations and the caller should push
	// dir's counterpart (the collected originals) back.
	//
	// When clearJournal is set, each holder's .ecj delete journal is overwritten
	// with an empty one (the vacuum has spent those deletes), but only after every
	// holder serves the new generation, and best-effort: see the implementation
	// for why the order makes the rollback exact and why a failure there must not
	// fail the vacuum. The rollback path passes false to keep the holders'
	// journals, which still describe a real generation.
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

// shardsByHolder groups the target's shard ids by the (holder, disk) that owns
// them, ascending inside each group and with the groups themselves sorted, so
// collection and redistribution are deterministic.
func (t VacuumTarget) shardsByHolder() (map[holderDisk][]uint32, []holderDisk) {
	byDisk := make(map[holderDisk][]uint32)
	var keys []holderDisk
	for _, id := range t.sortedShardIDs() {
		key := holderDisk{address: t.Holders[id], diskID: t.DiskIDs[id]}
		if _, ok := byDisk[key]; !ok {
			keys = append(keys, key)
		}
		byDisk[key] = append(byDisk[key], id)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].address != keys[j].address {
			return keys[i].address < keys[j].address
		}
		return keys[i].diskID < keys[j].diskID
	})
	return byDisk, keys
}

// Redistribute replaces the holders' shards with the compacted set. It runs in
// two phases, and the split is what makes VacuumVolume's rollback exact:
//
//  1. Every (holder, disk) is switched to the new generation: unmount its old
//     shards, push the compacted shards and sidecars, mount them. An error here
//     leaves the volume on mixed generations, while the caller's collected
//     originals are still a complete copy of the previous one.
//  2. Only once every holder serves the new generation are the spent delete
//     journals cleared, best-effort.
//
// The order matters because clearing a journal cannot be undone. Each journal —
// one per disk a holder keeps shards on — holds a disjoint subset of the volume's
// deletes, and the caller keeps the originals' *merged* journal rather than those
// subsets, so a rollback could never restore what a partially-cleared set gave up:
// those deletes would revert silently while the original .ecx (which does not carry
// them) served the needles again. Clearing last means a failed push has touched no
// journal at all.
//
// Clearing is best-effort because it is cosmetic: the new .ecx was rebuilt from
// the compacted index, so it no longer names the deleted needles, and a leftover
// journal entry for an unknown id is simply ignored (RebuildEcxFile treats it as
// NotFound). A holder that keeps its journal serves exactly the same live set,
// only with a stale delete count — so a failure here must not fail a vacuum whose
// data is already correct and verified.
//
// The shard ids are unchanged (the ratio is preserved), so each compacted shard
// overwrites its predecessor in place. A reader that hits a momentarily
// unmounted shard falls back to Reed-Solomon reconstruction.
func (c *ClusterTransport) Redistribute(ctx context.Context, target VacuumTarget, dir, base string, clearJournal bool) error {
	byDisk, disks := target.shardsByHolder()

	for _, key := range disks {
		if err := c.replaceShards(ctx, key, target, dir, base, byDisk[key]); err != nil {
			return err
		}
	}

	if clearJournal {
		c.clearDeleteJournals(ctx, target, dir, base, byDisk, disks)
	}
	return nil
}

// replaceShards points one (holder, disk) at the new generation.
func (c *ClusterTransport) replaceShards(ctx context.Context, key holderDisk, target VacuumTarget, dir, base string, ids []uint32) error {
	if err := c.unmountShards(ctx, key.address, target.VolumeID, ids); err != nil {
		return fmt.Errorf("unmount old shards on %s: %w", key.address, err)
	}
	for _, id := range ids {
		src := filepath.Join(dir, base+erasure_coding.ToExt(int(id)))
		if err := c.pushEcFile(ctx, key, target, erasure_coding.ToExt(int(id)), id, src); err != nil {
			return fmt.Errorf("push shard %d to %s: %w", id, key.address, err)
		}
	}
	// Each holder keeps a self-contained copy of the shared sidecars.
	for _, ext := range []string{".ecx", ".vif", ".ecsum"} {
		src := filepath.Join(dir, base+ext)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := c.pushEcFile(ctx, key, target, ext, 0, src); err != nil {
			return fmt.Errorf("push sidecar %s to %s: %w", ext, key.address, err)
		}
	}
	if err := c.mountShards(ctx, key.address, target, ids); err != nil {
		return fmt.Errorf("mount compacted shards on %s: %w", key.address, err)
	}
	return nil
}

// clearDeleteJournals overwrites each disk's .ecj with an empty one, so the master
// stops reporting deletes the compacted generation has already spent. The clear
// needs the shards unmounted (ReceiveFile refuses while the volume is mounted), and
// the unmount is per *node*, not per disk: a multi-disk node mounts one EcVolume per
// disk, and ReceiveFile refuses while the volume is mounted anywhere on it, so a
// sibling disk left mounted would block every clear. The remount is attempted even
// when a push failed, so a failure here never leaves shards unmounted. Failures are
// logged rather than returned — see Redistribute for why they must not fail the
// vacuum.
func (c *ClusterTransport) clearDeleteJournals(ctx context.Context, target VacuumTarget, dir, base string, byDisk map[holderDisk][]uint32, disks []holderDisk) {
	journal := filepath.Join(dir, base+".ecj")
	if err := os.WriteFile(journal, nil, 0o644); err != nil {
		glog.Warningf("ec_vacuum: volume %d: cannot prepare an empty delete journal; holders keep theirs: %v", target.VolumeID, err)
		return
	}
	// disks is sorted by (address, disk), so one node's groups are contiguous.
	for i := 0; i < len(disks); {
		j := i
		var nodeIDs []uint32
		for ; j < len(disks) && disks[j].address == disks[i].address; j++ {
			nodeIDs = append(nodeIDs, byDisk[disks[j]]...)
		}
		c.clearDeleteJournalsOnNode(ctx, target, journal, disks[i:j], nodeIDs)
		i = j
	}
}

// clearDeleteJournalsOnNode unmounts every shard the node holds for the volume,
// clears each of its disks' journals, and mounts the whole set back.
func (c *ClusterTransport) clearDeleteJournalsOnNode(ctx context.Context, target VacuumTarget, journal string, groups []holderDisk, nodeIDs []uint32) {
	address := groups[0].address
	if err := c.unmountShards(ctx, address, target.VolumeID, nodeIDs); err != nil {
		// Leave the shards as they are rather than unmounting half of them.
		glog.Warningf("ec_vacuum: volume %d: %s: cannot unmount to clear the delete journal: %v", target.VolumeID, address, err)
		return
	}
	for _, key := range groups {
		if err := c.pushEcFile(ctx, key, target, ".ecj", 0, journal); err != nil {
			glog.Warningf("ec_vacuum: volume %d: %s disk %d: keeps its delete journal; the deletes are already folded into the new index: %v", target.VolumeID, address, key.diskID, err)
		}
	}
	if err := c.mountShards(ctx, address, target, nodeIDs); err != nil {
		glog.Errorf("ec_vacuum: volume %d: %s: compacted shards are not remounted after the delete-journal clear: %v", target.VolumeID, address, err)
	}
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

// pushEcFile streams one EC shard or sidecar to a holder via ReceiveFile, telling
// the holder which disk the file belongs to so it lands where the shard already
// lives rather than wherever the server's own disk preference points.
func (c *ClusterTransport) pushEcFile(ctx context.Context, key holderDisk, target VacuumTarget, ext string, shardID uint32, srcPath string) error {
	fi, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	return operation.WithVolumeServerClient(false, pb.ServerAddress(key.address), c.dialOpt,
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
					DiskId:     key.diskID,
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
