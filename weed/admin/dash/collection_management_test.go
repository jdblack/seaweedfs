package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// ecShard builds the EcShardInfos for a disk holding one EC volume entry.
func ecShard(collection string, id uint32, bits uint32, sizes []int64) []*master_pb.VolumeEcShardInformationMessage {
	return []*master_pb.VolumeEcShardInformationMessage{
		{Id: id, Collection: collection, EcIndexBits: bits, ShardSizes: sizes},
	}
}

// TestCollectCollectionInfoCountsECSize covers the reported bug: a collection
// whose data lives almost entirely in EC volumes reported only the regular
// volumes' bytes, because GetClusterCollections added nothing for EC volumes.
// The size now comes from the shared aggregation, which counts EC data shards.
func TestCollectCollectionInfoCountsECSize(t *testing.T) {
	const allShards = (1 << 14) - 1 // 10 data + 4 parity
	const firstSeven = (1 << 7) - 1 // shards 0..6
	sevens := []int64{1000, 1000, 1000, 1000, 1000, 1000, 1000}

	// One EC volume split over two nodes: 10 shards of 1000 bytes each, i.e.
	// 10000 bytes of logical data, plus 4000 bytes of parity that must not
	// count, and the volume must be counted once rather than once per node.
	nodeA := &master_pb.DataNodeInfo{Id: "a", DiskInfos: map[string]*master_pb.DiskInfo{
		"hdd": {Type: "hdd", EcShardInfos: ecShard("movies", 42, firstSeven, sevens)},
	}}
	nodeB := &master_pb.DataNodeInfo{Id: "b", DiskInfos: map[string]*master_pb.DiskInfo{
		"hdd": {Type: "hdd", EcShardInfos: ecShard("movies", 42, allShards&^firstSeven, sevens)},
	}}

	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{RackInfos: []*master_pb.RackInfo{
				{DataNodeInfos: []*master_pb.DataNodeInfo{nodeA, nodeB}},
			}},
		},
	}

	collections, totalVolumes, totalEcVolumes := collectCollectionInfo(topo)

	collection, ok := collections["movies"]
	if !ok {
		t.Fatalf("expected collection movies, got %v", collections)
	}
	if totalVolumes != 0 {
		t.Errorf("totalVolumes: got %d, want 0", totalVolumes)
	}
	if totalEcVolumes != 1 || collection.EcVolumeCount != 1 {
		t.Errorf("EC volume count: total=%d per-collection=%d, want 1 each (deduped cluster-wide)",
			totalEcVolumes, collection.EcVolumeCount)
	}
	if collection.TotalSize != 10000 {
		t.Errorf("TotalSize: got %d, want 10000 (EC data shards only, parity excluded)", collection.TotalSize)
	}
	if collection.VolumeCount != 0 {
		t.Errorf("VolumeCount: got %d, want 0", collection.VolumeCount)
	}
}

// TestCollectCollectionInfoCountsEcVolumeOnceAcrossDisks verifies a single EC
// volume whose shards sit on two physical disks of one node is counted once.
// Deduping per disk (the old behaviour) counted each shard holder separately.
func TestCollectCollectionInfoCountsEcVolumeOnceAcrossDisks(t *testing.T) {
	const allShards = (1 << 14) - 1
	const firstSeven = (1 << 7) - 1
	sevens := []int64{1000, 1000, 1000, 1000, 1000, 1000, 1000}

	node := &master_pb.DataNodeInfo{Id: "a", DiskInfos: map[string]*master_pb.DiskInfo{
		"hdd": {Type: "hdd", EcShardInfos: ecShard("c", 9, firstSeven, sevens)},
		"ssd": {Type: "ssd", EcShardInfos: ecShard("c", 9, allShards&^firstSeven, sevens)},
	}}
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{RackInfos: []*master_pb.RackInfo{{DataNodeInfos: []*master_pb.DataNodeInfo{node}}}},
		},
	}

	collections, _, totalEcVolumes := collectCollectionInfo(topo)

	if totalEcVolumes != 1 {
		t.Errorf("totalEcVolumes: got %d, want 1", totalEcVolumes)
	}
	if collections["c"].EcVolumeCount != 1 {
		t.Errorf("EcVolumeCount: got %d, want 1", collections["c"].EcVolumeCount)
	}
	if got := collections["c"].DiskTypes; len(got) != 2 {
		t.Errorf("DiskTypes: got %v, want both hdd and ssd", got)
	}
}

// TestCollectCollectionInfoRegularReplicaLogicalSize verifies a replicated
// regular volume is reported logically: every copy's bytes are summed but each
// copy is divided by the replica count, so the collection does not read as
// double, and tombstones are netted out.
func TestCollectCollectionInfoRegularReplicaLogicalSize(t *testing.T) {
	replica := func(id string) *master_pb.DataNodeInfo {
		return &master_pb.DataNodeInfo{Id: id, DiskInfos: map[string]*master_pb.DiskInfo{
			"ssd": {Type: "ssd", VolumeInfos: []*master_pb.VolumeInformationMessage{
				{Id: 7, Collection: "photos", ReplicaPlacement: 1, DiskType: "ssd", Size: 1000, DeletedByteCount: 100},
			}},
		}}
	}

	// The two copies live in different data centers, so the collection must be
	// reported as spanning multiple DCs.
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{Id: "dc1", RackInfos: []*master_pb.RackInfo{{DataNodeInfos: []*master_pb.DataNodeInfo{replica("a")}}}},
			{Id: "dc2", RackInfos: []*master_pb.RackInfo{{DataNodeInfos: []*master_pb.DataNodeInfo{replica("b")}}}},
		},
	}

	collections, totalVolumes, _ := collectCollectionInfo(topo)
	collection := collections["photos"]
	if collection == nil {
		t.Fatalf("expected collection photos, got %v", collections)
	}
	if totalVolumes != 2 || collection.VolumeCount != 2 {
		t.Errorf("volume count: total=%d per-collection=%d, want 2 each (one entry per replica)",
			totalVolumes, collection.VolumeCount)
	}
	if collection.TotalSize != 900 {
		t.Errorf("TotalSize: got %d, want 900 ((1000-100) counted once across 2 copies)", collection.TotalSize)
	}
	if collection.DataCenter != "multi" {
		t.Errorf("DataCenter: got %q, want \"multi\"", collection.DataCenter)
	}
	if got := collection.DiskTypes; len(got) != 1 || got[0] != "ssd" {
		t.Errorf("DiskTypes: got %v, want [ssd]", got)
	}
}

// TestCollectCollectionInfoDefaultCollection verifies empty collection names
// are bucketed under "default", matching collectCollectionStats.
func TestCollectCollectionInfoDefaultCollection(t *testing.T) {
	node := &master_pb.DataNodeInfo{Id: "a", DiskInfos: map[string]*master_pb.DiskInfo{
		"": {VolumeInfos: []*master_pb.VolumeInformationMessage{
			{Id: 1, Collection: "", Size: 500},
		}},
	}}
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{RackInfos: []*master_pb.RackInfo{{DataNodeInfos: []*master_pb.DataNodeInfo{node}}}},
		},
	}

	collections, _, _ := collectCollectionInfo(topo)
	collection := collections["default"]
	if collection == nil {
		t.Fatalf("expected default collection, got %v", collections)
	}
	if collection.TotalSize != 500 {
		t.Errorf("TotalSize: got %d, want 500", collection.TotalSize)
	}
	// An empty disk type is reported as hdd.
	if got := collection.DiskTypes; len(got) != 1 || got[0] != "hdd" {
		t.Errorf("DiskTypes: got %v, want [hdd]", got)
	}
}
