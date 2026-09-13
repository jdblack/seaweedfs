package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestUpdateEcShardsRefreshesMetadataWithoutBitmapChange pins the fix for a
// vacuum-induced staleness: ec.vacuum unmounts and remounts the same shard ids,
// so the full EC heartbeat's shard bitmap is unchanged while FileCount /
// DeleteCount (and the ratio/generation) are the only fields that moved. The
// master must still apply the incoming list, or it keeps reporting FileCount 0
// and the vacuum's deleted-ratio detection reads 0 for the volume forever.
func TestUpdateEcShardsRefreshesMetadataWithoutBitmapChange(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 100})

	const vid = uint32(42)
	const collection = "movies-archive"
	shards := []erasure_coding.ShardId{0, 1, 2, 3, 4}

	// First full heartbeat: a healthy 5-shard volume with known counts.
	first := buildEcShardMessage(vid, collection, "", 0, shards)
	first.FileCount, first.DeleteCount = 424, 10
	first.EncodeTsNs = 100
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{first}, dn)
	if fc, dels := ecShardCounts(t, dn, needle.VolumeId(vid)); fc != 424 || dels != 10 {
		t.Fatalf("after first sync counts = %d/%d, want 424/10", fc, dels)
	}

	// Second full heartbeat: identical shard bitmap, but the counts changed
	// (a later vacuum found deletes and then re-encoded). Nothing in the
	// bitmap moved, so this must still reach the master's stored entry.
	second := buildEcShardMessage(vid, collection, "", 0, shards)
	second.FileCount, second.DeleteCount = 414, 0
	second.EncodeTsNs = 200
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{second}, dn)

	fc, dels := ecShardCounts(t, dn, needle.VolumeId(vid))
	if fc != 414 {
		t.Errorf("FileCount = %d, want 414: a metadata-only full heartbeat was dropped", fc)
	}
	if dels != 0 {
		t.Errorf("DeleteCount = %d, want 0: a metadata-only full heartbeat was dropped", dels)
	}
}

// ecShardCounts sums the FileCount/DeleteCount the master stored for vid.
func ecShardCounts(t *testing.T, dn *DataNode, vid needle.VolumeId) (fileCount, deleteCount uint64) {
	t.Helper()
	for _, ev := range dn.GetEcShards() {
		if ev.VolumeId == vid {
			fileCount += ev.FileCount
			deleteCount += ev.DeleteCount
		}
	}
	return
}
