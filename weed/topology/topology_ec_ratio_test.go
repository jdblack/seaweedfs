package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestDiskInfoCarriesEcShardRatio guards the path the admin reads: a volume's
// per-volume EC ratio must survive registration and reappear on the master's
// DiskInfo.EcShardInfos, not be flattened to the build default.
func TestDiskInfoCarriesEcShardRatio(t *testing.T) {
	topo := NewTopology("ecratio", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})

	// A 3+2 volume: shards 0..4 present.
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 12, Collection: "c", EcIndexBits: 0b11111, DiskId: 0, DataShards: 3, ParityShards: 2},
	}, dn)

	info := diskInfoOf(t, dn)
	got := info.EcShardInfos[0]
	if got.GetDataShards() != 3 || got.GetParityShards() != 2 {
		t.Fatalf("ratio lost through registration: data=%d parity=%d, want 3+2",
			got.GetDataShards(), got.GetParityShards())
	}
}

// TestIncrementalSyncCarriesEcShardRatio covers the single-shard heartbeat path.
func TestIncrementalSyncCarriesEcShardRatio(t *testing.T) {
	topo := NewTopology("ecratio", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.2", 8080, 18080, "", "", map[string]uint32{"": 100})

	topo.IncrementalSyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 13, Collection: "c", EcIndexBits: 0b1, DiskId: 0, DataShards: 3, ParityShards: 2},
	}, nil, dn)

	info := diskInfoOf(t, dn)
	got := info.EcShardInfos[0]
	if got.GetDataShards() != 3 || got.GetParityShards() != 2 {
		t.Fatalf("ratio lost through incremental registration: data=%d parity=%d, want 3+2",
			got.GetDataShards(), got.GetParityShards())
	}
}

func diskInfoOf(t *testing.T, dn *DataNode) *master_pb.DiskInfo {
	t.Helper()
	var info *master_pb.DiskInfo
	for _, c := range dn.Children() {
		info = c.(*Disk).ToDiskInfo(VolumeFilter{})
	}
	if info == nil || len(info.EcShardInfos) == 0 {
		t.Fatalf("the node reported no EC shard info: %+v", info)
	}
	return info
}
