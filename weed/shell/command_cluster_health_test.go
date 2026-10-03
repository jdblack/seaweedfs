package shell

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func mustReplicaPlacement(t *testing.T, spec string) uint32 {
	t.Helper()
	rp, err := super_block.NewReplicaPlacementFromString(spec)
	if err != nil {
		t.Fatalf("bad replication spec %q: %v", spec, err)
	}
	return uint32(rp.Byte())
}

func healthTestNode(id, dc, rack string, disks map[string]*master_pb.DiskInfo) *healthNode {
	return &healthNode{id: id, address: id, dc: dc, rack: rack, disks: disks}
}

func healthTestDisk(volumes []*master_pb.VolumeInformationMessage) map[string]*master_pb.DiskInfo {
	return map[string]*master_pb.DiskInfo{
		"hdd": {Type: "hdd", VolumeInfos: volumes},
	}
}

func TestCheckNodeInventory(t *testing.T) {
	nodes := []*healthNode{
		healthTestNode("a:1", "dc1", "r1", nil),
		healthTestNode("b:1", "dc1", "r2", nil),
		healthTestNode("c:1", "dc2", "r1", nil),
	}

	if f := checkNodeInventory(nodes, 3, 1, nil); f.status != healthOK {
		t.Errorf("healthy inventory: status %v, want OK", f.status)
	} else if !strings.Contains(f.summary, "3 volume server(s)") || !strings.Contains(f.summary, "dc1=2, dc2=1") {
		t.Errorf("healthy inventory summary %q", f.summary)
	}

	if f := checkNodeInventory(nodes, 3, 0, nil); f.status != healthWarn {
		t.Errorf("no filers: status %v, want WARN", f.status)
	}

	if f := checkNodeInventory(nil, 3, 1, nil); f.status != healthFail {
		t.Errorf("no volume servers: status %v, want FAIL", f.status)
	}
}

func TestCheckDiskCapacity(t *testing.T) {
	gib := uint64(1) << 30
	tests := []struct {
		name string
		disk *master_pb.DiskInfo
		want healthSeverity
	}{
		{
			name: "healthy",
			disk: &master_pb.DiskInfo{MaxVolumeCount: 10, FreeVolumeCount: 5, DiskTotalBytes: 16 * gib, DiskFreeBytes: 8 * gib},
			want: healthOK,
		},
		{
			name: "no free volume slots",
			disk: &master_pb.DiskInfo{MaxVolumeCount: 10, FreeVolumeCount: 0, DiskTotalBytes: 16 * gib, DiskFreeBytes: 8 * gib},
			want: healthWarn,
		},
		{
			name: "low free space",
			disk: &master_pb.DiskInfo{MaxVolumeCount: 10, FreeVolumeCount: 5, DiskTotalBytes: 16 * gib, DiskFreeBytes: 512 << 20},
			want: healthWarn,
		},
		{
			name: "critically low free space",
			disk: &master_pb.DiskInfo{MaxVolumeCount: 10, FreeVolumeCount: 5, DiskTotalBytes: 16 * gib, DiskFreeBytes: gib / 4},
			want: healthFail,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes := []*healthNode{healthTestNode("a:1", "dc1", "r1", map[string]*master_pb.DiskInfo{"hdd": tc.disk})}
			if f := checkDiskCapacity(nodes, 5); f.status != tc.want {
				t.Errorf("status %v, want %v (%s)", f.status, tc.want, f.summary)
			}
		})
	}
}

func TestCheckVolumeReplication(t *testing.T) {
	vol := func(id uint32, rp uint32) *master_pb.VolumeInformationMessage {
		return &master_pb.VolumeInformationMessage{Id: id, ReplicaPlacement: rp}
	}
	spec := func(rep string) uint32 { return mustReplicaPlacement(t, rep) }

	t.Run("healthy single replica", func(t *testing.T) {
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(1, spec("000"))})),
		}
		f := checkVolumeReplication(nodes)
		if f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})

	t.Run("under replicated", func(t *testing.T) {
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(7, spec("010"))})),
		}
		f := checkVolumeReplication(nodes)
		if f.status != healthFail {
			t.Fatalf("status %v, want FAIL (%s)", f.status, f.summary)
		}
		if !strings.Contains(f.summary, "1 under-replicated") {
			t.Errorf("summary %q", f.summary)
		}
		if len(f.details) == 0 || !strings.Contains(f.details[0], "volume 7") {
			t.Errorf("details %v, want volume 7", f.details)
		}
	})

	t.Run("over replicated", func(t *testing.T) {
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(8, spec("000"))})),
			healthTestNode("b:1", "dc1", "r2", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(8, spec("000"))})),
		}
		f := checkVolumeReplication(nodes)
		if f.status != healthWarn {
			t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
		}
		if !strings.Contains(f.summary, "1 over-replicated") {
			t.Errorf("summary %q", f.summary)
		}
	})

	t.Run("misplaced replicas", func(t *testing.T) {
		// two copies as required, but both sit in the same rack while the
		// placement demands different racks
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(9, spec("010"))})),
			healthTestNode("b:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(9, spec("010"))})),
		}
		f := checkVolumeReplication(nodes)
		if f.status != healthWarn {
			t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
		}
		if !strings.Contains(f.summary, "1 misplaced") {
			t.Errorf("summary %q", f.summary)
		}
	})

	t.Run("correctly placed across racks", func(t *testing.T) {
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(10, spec("010"))})),
			healthTestNode("b:1", "dc1", "r2", healthTestDisk([]*master_pb.VolumeInformationMessage{vol(10, spec("010"))})),
		}
		if f := checkVolumeReplication(nodes); f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})
}

func TestCheckEcReplication(t *testing.T) {
	ecDisk := func(infos ...*master_pb.VolumeEcShardInformationMessage) map[string]*master_pb.DiskInfo {
		return map[string]*master_pb.DiskInfo{"hdd": {Type: "hdd", EcShardInfos: infos}}
	}

	t.Run("no ec volumes", func(t *testing.T) {
		f := checkEcReplication([]*healthNode{healthTestNode("a:1", "dc1", "r1", healthTestDisk(nil))})
		if f.status != healthOK || f.summary != "no EC volumes" {
			t.Errorf("got %v %q", f.status, f.summary)
		}
	})

	t.Run("healthy", func(t *testing.T) {
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(&master_pb.VolumeEcShardInformationMessage{Id: 1, EcIndexBits: 0b11111111111111})),
		}
		if f := checkEcReplication(nodes); f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})

	t.Run("under replicated", func(t *testing.T) {
		// shard 3 is missing everywhere: 13 of 14 remain, above the 10 data
		// shards, so the volume has lost redundancy but is still
		// reconstructable - degraded, not failed.
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(&master_pb.VolumeEcShardInformationMessage{Id: 2, EcIndexBits: 0b11111111110111})),
		}
		f := checkEcReplication(nodes)
		if f.status != healthWarn {
			t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
		}
		if len(f.details) == 0 || !strings.Contains(f.details[0], "missing 3") {
			t.Errorf("details %v", f.details)
		}
	})

	t.Run("over replicated", func(t *testing.T) {
		// shard 0 lives on both nodes
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(&master_pb.VolumeEcShardInformationMessage{Id: 3, EcIndexBits: 0b11111111111111})),
			healthTestNode("b:1", "dc1", "r2", ecDisk(&master_pb.VolumeEcShardInformationMessage{Id: 3, EcIndexBits: 0b00000000000001})),
		}
		f := checkEcReplication(nodes)
		if f.status != healthWarn {
			t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
		}
		if len(f.details) == 0 || !strings.Contains(f.details[0], "duplicated shard(s) [0]") {
			t.Errorf("details %v", f.details)
		}
	})

	t.Run("custom ratio 3+2", func(t *testing.T) {
		// 5 bits is complete for a 3+2 volume even though the build default is 14
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(&master_pb.VolumeEcShardInformationMessage{
				Id: 4, EcIndexBits: 0b11111, DataShards: 3, ParityShards: 2,
			})),
		}
		if f := checkEcReplication(nodes); f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})
}

func TestCheckEcSizing(t *testing.T) {
	// ecSized builds one holder's heartbeat: the bits are the shards present,
	// each reported at the given size.
	ecSized := func(id uint32, dataShards, parityShards int, size int64, bits uint32) *master_pb.VolumeEcShardInformationMessage {
		msg := &master_pb.VolumeEcShardInformationMessage{
			Id:           id,
			EcIndexBits:  bits,
			DataShards:   uint32(dataShards),
			ParityShards: uint32(parityShards),
		}
		for b := bits; b != 0; b >>= 1 {
			if b&1 != 0 {
				msg.ShardSizes = append(msg.ShardSizes, size)
			}
		}
		return msg
	}
	ecDisk := func(infos ...*master_pb.VolumeEcShardInformationMessage) map[string]*master_pb.DiskInfo {
		return map[string]*master_pb.DiskInfo{"hdd": {Type: "hdd", EcShardInfos: infos}}
	}
	const mb = int64(1) << 20

	t.Run("no ec volumes", func(t *testing.T) {
		f := checkEcSizing([]*healthNode{healthTestNode("a:1", "dc1", "r1", healthTestDisk(nil))}, 30*mb)
		if f.status != healthOK || f.summary != "no EC volumes" {
			t.Errorf("got %v %q", f.status, f.summary)
		}
	})

	t.Run("a full volume is fine", func(t *testing.T) {
		// 10 data shards of 5MB is 50MB of data, above the 30MB floor
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(ecSized(1, 10, 4, 5*mb, 0b11111111111111))),
		}
		if f := checkEcSizing(nodes, 30*mb); f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})

	t.Run("an undersized volume is counted", func(t *testing.T) {
		// 10 data shards of 1MB is 10MB of data, below the 30MB floor
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(ecSized(2, 10, 4, 1*mb, 0b11111111111111))),
		}
		f := checkEcSizing(nodes, 30*mb)
		if f.status != healthWarn {
			t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
		}
		if !strings.Contains(f.summary, "1 of 1 EC volume(s) undersized") {
			t.Errorf("summary %q", f.summary)
		}
		if len(f.details) == 0 || !strings.Contains(f.details[0], "volume 2") {
			t.Errorf("details %v", f.details)
		}
	})

	t.Run("threshold zero disables the size check", func(t *testing.T) {
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(ecSized(3, 10, 4, 1*mb, 0b11111111111111))),
		}
		if f := checkEcSizing(nodes, 0); f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})

	t.Run("a truncated shard is reported", func(t *testing.T) {
		// every shard is 5MB except one, which was cut short
		msg := ecSized(4, 10, 4, 5*mb, 0b11111111111111)
		msg.ShardSizes[len(msg.ShardSizes)-1] = 4096
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(msg)),
		}
		f := checkEcSizing(nodes, 30*mb)
		if f.status != healthWarn {
			t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
		}
		if !strings.Contains(f.summary, "disagree on size") {
			t.Errorf("summary %q", f.summary)
		}
	})

	t.Run("unreported sizes are not judged", func(t *testing.T) {
		// a volume server that has not heartbeated sizes reports none; that is
		// not a reason to call the volume undersized or uneven
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(&master_pb.VolumeEcShardInformationMessage{
				Id: 5, EcIndexBits: 0b11111111111111, DataShards: 10, ParityShards: 4,
			})),
		}
		if f := checkEcSizing(nodes, 30*mb); f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})

	t.Run("sizes are merged across holders", func(t *testing.T) {
		// the 14 shards are split over two holders, all the same 5MB: still one
		// full volume, not two half ones
		nodes := []*healthNode{
			healthTestNode("a:1", "dc1", "r1", ecDisk(ecSized(6, 10, 4, 5*mb, 0b00000000001111))),
			healthTestNode("b:1", "dc1", "r2", ecDisk(ecSized(6, 10, 4, 5*mb, 0b11111111110000))),
		}
		if f := checkEcSizing(nodes, 30*mb); f.status != healthOK {
			t.Errorf("status %v, want OK (%s)", f.status, f.summary)
		}
	})
}

func TestCheckGarbage(t *testing.T) {
	nodes := []*healthNode{
		healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{
			{Id: 1, Size: 1000, DeletedByteCount: 500}, // 50% garbage
			{Id: 2, Size: 1000, DeletedByteCount: 100}, // 10% garbage
			{Id: 3, Size: 0, DeletedByteCount: 100},    // empty, not judged
		})),
	}
	f := checkGarbage(nodes, 0.3)
	if f.status != healthWarn {
		t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
	}
	if !strings.Contains(f.summary, "1 of 3") {
		t.Errorf("summary %q", f.summary)
	}
	if len(f.details) != 1 || !strings.Contains(f.details[0], "volume 1") {
		t.Errorf("details %v", f.details)
	}

	if f := checkGarbage(nodes, 0.9); f.status != healthOK {
		t.Errorf("high threshold: status %v, want OK", f.status)
	}
}

func TestCheckBalance(t *testing.T) {
	disk := func(volumeCount int64) map[string]*master_pb.DiskInfo {
		return map[string]*master_pb.DiskInfo{"hdd": {Type: "hdd", VolumeCount: volumeCount}}
	}

	balanced := []*healthNode{
		healthTestNode("a:1", "dc1", "r1", disk(50)),
		healthTestNode("b:1", "dc1", "r2", disk(50)),
	}
	if f := checkBalance(balanced, 0.15); f.status != healthOK {
		t.Errorf("balanced: status %v, want OK (%s)", f.status, f.summary)
	}

	skewed := []*healthNode{
		healthTestNode("a:1", "dc1", "r1", disk(100)),
		healthTestNode("b:1", "dc1", "r2", disk(10)),
	}
	f := checkBalance(skewed, 0.15)
	if f.status != healthWarn {
		t.Fatalf("skewed: status %v, want WARN (%s)", f.status, f.summary)
	}
	if !strings.Contains(f.summary, "unbalanced") {
		t.Errorf("summary %q", f.summary)
	}
}

func TestCheckEmptyVolumes(t *testing.T) {
	now := time.Now()
	empty := func(id uint32, modifiedAt int64) *master_pb.VolumeInformationMessage {
		return &master_pb.VolumeInformationMessage{Id: id, Size: super_block.SuperBlockSize, ModifiedAtSecond: modifiedAt}
	}

	nodes := []*healthNode{
		healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{
			empty(1, now.Add(-48*time.Hour).Unix()), // idle
			empty(2, now.Unix()),                    // freshly created
		})),
	}
	f := checkEmptyVolumes(nodes, now, 24*time.Hour)
	if f.status != healthWarn {
		t.Fatalf("status %v, want WARN (%s)", f.status, f.summary)
	}
	if !strings.Contains(f.summary, "1 empty volume(s) idle") {
		t.Errorf("summary %q", f.summary)
	}
	if len(f.details) == 0 || !strings.Contains(f.details[0], "volume 1") {
		t.Errorf("details %v", f.details)
	}

	// a volume carrying data is not empty
	withData := []*healthNode{
		healthTestNode("a:1", "dc1", "r1", healthTestDisk([]*master_pb.VolumeInformationMessage{
			{Id: 3, Size: 4096, ModifiedAtSecond: now.Add(-48 * time.Hour).Unix()},
		})),
	}
	if f := checkEmptyVolumes(withData, now, 24*time.Hour); f.status != healthOK {
		t.Errorf("non-empty volume: status %v, want OK (%s)", f.status, f.summary)
	}
}

func TestHealthReportVerdict(t *testing.T) {
	render := func(findings ...healthFinding) (string, healthSeverity) {
		var buf bytes.Buffer
		r := &healthReport{writer: &buf, detailed: false}
		for _, f := range findings {
			r.add(f)
		}
		worst := r.print()
		return buf.String(), worst
	}

	out, worst := render(healthFinding{name: "a", status: healthOK, summary: "fine"})
	if worst != healthOK || !strings.Contains(out, "RESULT: HEALTHY (0 failure(s), 0 warning(s))") {
		t.Errorf("healthy: worst %v, out %q", worst, out)
	}

	out, worst = render(
		healthFinding{name: "a", status: healthOK, summary: "fine"},
		healthFinding{name: "b", status: healthWarn, summary: "meh"},
	)
	if worst != healthWarn || !strings.Contains(out, "RESULT: DEGRADED (0 failure(s), 1 warning(s))") {
		t.Errorf("degraded: worst %v, out %q", worst, out)
	}

	out, worst = render(
		healthFinding{name: "b", status: healthWarn, summary: "meh"},
		healthFinding{name: "c", status: healthFail, summary: "broken"},
	)
	if worst != healthFail || !strings.Contains(out, "RESULT: UNHEALTHY (1 failure(s), 1 warning(s))") {
		t.Errorf("unhealthy: worst %v, out %q", worst, out)
	}
	if !strings.Contains(out, "[WARN] b: meh") || !strings.Contains(out, "[FAIL] c: broken") {
		t.Errorf("formatted findings missing: %q", out)
	}
}

func TestHealthFindingRenderCapsDetails(t *testing.T) {
	f := healthFinding{name: "x", status: healthWarn, summary: "s", details: []string{"d1", "d2", "d3"}}

	var buf bytes.Buffer
	f.render(&buf, true, 2)
	out := buf.String()
	if !strings.Contains(out, "d1") || !strings.Contains(out, "d2") {
		t.Errorf("details missing: %q", out)
	}
	if strings.Contains(out, "d3") {
		t.Errorf("detail d3 should be capped: %q", out)
	}
	if !strings.Contains(out, "... and 1 more") {
		t.Errorf("cap notice missing: %q", out)
	}

	buf.Reset()
	f.render(&buf, false, 2)
	if strings.Contains(buf.String(), "d1") {
		t.Errorf("details should be hidden without -detailed: %q", buf.String())
	}
}
