package ec_bitrot_scan

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// recordingDetectionSender captures proposals/activities for assertions.
type recordingDetectionSender struct {
	proposals *plugin_pb.DetectionProposals
	complete  *plugin_pb.DetectionComplete
	events    []*plugin_pb.ActivityEvent
}

func (r *recordingDetectionSender) SendProposals(p *plugin_pb.DetectionProposals) error {
	r.proposals = p
	return nil
}
func (r *recordingDetectionSender) SendComplete(c *plugin_pb.DetectionComplete) error {
	r.complete = c
	return nil
}
func (r *recordingDetectionSender) SendActivity(e *plugin_pb.ActivityEvent) error {
	r.events = append(r.events, e)
	return nil
}

func ecInfo(id uint32, collection string, data, parity uint32) *master_pb.VolumeEcShardInformationMessage {
	return &master_pb.VolumeEcShardInformationMessage{
		Id:           id,
		Collection:   collection,
		EcIndexBits:  0x3FFF,
		DataShards:   data,
		ParityShards: parity,
	}
}

// topologyWithNodes builds a topology with one data center / rack holding the
// given nodes.
func topologyWithNodes(nodes []*master_pb.DataNodeInfo) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id:        "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: nodes}},
		}},
	}
}

func dataNode(id, address string, grpcPort uint32, disks map[string][]*master_pb.VolumeEcShardInformationMessage) *master_pb.DataNodeInfo {
	diskInfos := make(map[string]*master_pb.DiskInfo, len(disks))
	for dt, ecs := range disks {
		diskInfos[dt] = &master_pb.DiskInfo{EcShardInfos: ecs}
	}
	return &master_pb.DataNodeInfo{
		Id:        id,
		Address:   address,
		GrpcPort:  grpcPort,
		DiskInfos: diskInfos,
	}
}

func TestEnumerateEcVolumesDedupesByVolumeCollectionDiskType(t *testing.T) {
	topo := topologyWithNodes([]*master_pb.DataNodeInfo{
		// Two holders of the same (vid, collection, hdd) tuple => one candidate.
		dataNode("n1", "127.0.0.1:1", 9001, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(7, "c1", 10, 4)},
		}),
		dataNode("n2", "127.0.0.2:1", 9002, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(7, "c1", 10, 4)},
		}),
		// Same vid+collection on a different disk type => a second candidate.
		dataNode("n3", "127.0.0.3:1", 9003, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"ssd": {ecInfo(7, "c1", 10, 4)},
		}),
		// Same vid, different collection => a third candidate.
		dataNode("n4", "127.0.0.4:1", 9004, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(7, "c2", 10, 4)},
		}),
	})

	candidates, err := enumerateEcVolumes(topo, "")
	require.NoError(t, err)
	require.Len(t, candidates, 3)

	got := map[string]bool{}
	for _, c := range candidates {
		got[c.dedupeKey()] = true
	}
	require.True(t, got["ec_bitrot_scan:7:c1:hdd"])
	require.True(t, got["ec_bitrot_scan:7:c1:ssd"])
	require.True(t, got["ec_bitrot_scan:7:c2:hdd"])
}

func TestEnumerateEcVolumesCollectionFilter(t *testing.T) {
	topo := topologyWithNodes([]*master_pb.DataNodeInfo{
		dataNode("n1", "127.0.0.1:1", 9001, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(1, "keep", 10, 4), ecInfo(2, "drop", 10, 4)},
		}),
	})

	candidates, err := enumerateEcVolumes(topo, "keep")
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, uint32(1), candidates[0].VolumeID)
	require.Equal(t, "keep", candidates[0].Collection)
}

func TestBuildProposalsWindow(t *testing.T) {
	candidates := []ecVolumeCandidate{
		{VolumeID: 1, Collection: "c", DiskType: "hdd"},
		{VolumeID: 2, Collection: "c", DiskType: "hdd"},
		{VolumeID: 3, Collection: "c", DiskType: "hdd"},
	}

	proposals, hasMore := buildProposals(candidates, 2, 0)
	require.True(t, hasMore)
	require.Len(t, proposals, 2)
	require.Equal(t, "ec_bitrot_scan:1:c:hdd", proposals[0].GetDedupeKey())
	require.Equal(t, jobType, proposals[0].GetJobType())

	// A non-positive slice means the whole roster.
	all, hasMore := buildProposals(candidates, 0, 0)
	require.False(t, hasMore)
	require.Len(t, all, 3)

	// A window running off the end wraps to the front of the roster.
	wrapped, hasMore := buildProposals(candidates, 2, 2)
	require.True(t, hasMore)
	require.Len(t, wrapped, 2)
	require.Equal(t, "ec_bitrot_scan:3:c:hdd", wrapped[0].GetDedupeKey())
	require.Equal(t, "ec_bitrot_scan:1:c:hdd", wrapped[1].GetDedupeKey())
}

func TestHoldersForVolume(t *testing.T) {
	topo := topologyWithNodes([]*master_pb.DataNodeInfo{
		dataNode("n1", "127.0.0.1:1", 9001, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(7, "c1", 10, 4)},
		}),
		dataNode("n2", "127.0.0.2:1", 9002, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(7, "c1", 10, 4)},
		}),
		dataNode("n3", "127.0.0.3:1", 9003, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(7, "other", 10, 4)},
		}),
	})

	holders := holdersForVolume(topo, 7, "c1", "hdd")
	require.Equal(t, []string{"127.0.0.1:1.9001", "127.0.0.2:1.9002"}, holders)

	// Narrows to another disk type: none.
	require.Empty(t, holdersForVolume(topo, 7, "c1", "ssd"))
}

func TestShardRatioForVolume(t *testing.T) {
	topo := topologyWithNodes([]*master_pb.DataNodeInfo{
		dataNode("n1", "127.0.0.1:1", 9001, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(7, "c1", 14, 3)},
		}),
	})
	data, parity := shardRatioForVolume(topo, 7, "c1")
	require.Equal(t, 14, data)
	require.Equal(t, 3, parity)
}

func TestDetectMinimumIntervalFloor(t *testing.T) {
	topo := topologyWithNodes([]*master_pb.DataNodeInfo{
		dataNode("n1", "127.0.0.1:1", 9001, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(1, "c1", 10, 4)},
		}),
	})

	fetchCalls := 0
	h := &BitrotScanHandler{
		fetchTopology: func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error) {
			fetchCalls++
			return topo, nil
		},
	}

	// The floor is off by default: a run 10s ago does not stop detection.
	sender := &recordingDetectionSender{}
	req := &plugin_pb.RunDetectionRequest{
		JobType:           jobType,
		LastSuccessfulRun: timestamppb.New(time.Now().Add(-10 * time.Second)),
		AdminRuntime:      &plugin_pb.AdminRuntimeConfig{DetectionIntervalMinutes: 240},
	}
	require.NoError(t, h.Detect(context.Background(), req, sender))
	require.Len(t, sender.proposals.GetProposals(), 1)
	require.Equal(t, 1, fetchCalls)

	// A floor at or below the detection interval is honored.
	sender = &recordingDetectionSender{}
	req = &plugin_pb.RunDetectionRequest{
		JobType:           jobType,
		LastSuccessfulRun: timestamppb.New(time.Now().Add(-10 * time.Second)),
		AdminRuntime:      &plugin_pb.AdminRuntimeConfig{DetectionIntervalMinutes: 240},
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{
			fieldMinIntervalMins: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 60}},
		},
	}
	require.NoError(t, h.Detect(context.Background(), req, sender))
	require.Empty(t, sender.proposals.GetProposals())
	require.Equal(t, 1, fetchCalls, "topology must not be fetched when the floor skips")

	// A floor longer than the detection interval would stretch one sweep over
	// several targets (the stored 4320 against a 4-hour tick), so it is ignored.
	sender = &recordingDetectionSender{}
	req = &plugin_pb.RunDetectionRequest{
		JobType:           jobType,
		LastSuccessfulRun: timestamppb.New(time.Now().Add(-10 * time.Second)),
		AdminRuntime:      &plugin_pb.AdminRuntimeConfig{DetectionIntervalMinutes: 240},
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{
			fieldMinIntervalMins: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 4320}},
		},
	}
	require.NoError(t, h.Detect(context.Background(), req, sender))
	require.Len(t, sender.proposals.GetProposals(), 1)
	require.Equal(t, 2, fetchCalls)
}

func TestEffectiveFloorMinutes(t *testing.T) {
	const sweep, tick = 10080, 240
	// Unset / disabled.
	require.Zero(t, effectiveFloorMinutes(&Config{MinIntervalMinutes: 0, ScanIntervalMinutes: sweep}, tick))
	// At or below the detection interval: honored, it cannot stretch the pass.
	require.Equal(t, 60, effectiveFloorMinutes(&Config{MinIntervalMinutes: 60, ScanIntervalMinutes: sweep}, tick))
	require.Equal(t, 240, effectiveFloorMinutes(&Config{MinIntervalMinutes: 240, ScanIntervalMinutes: sweep}, tick))
	// Longer than the detection interval: ignored, or the pass would span
	// several sweep targets (4320 min of floor against a 240 min tick is 18x).
	require.Zero(t, effectiveFloorMinutes(&Config{MinIntervalMinutes: 4320, ScanIntervalMinutes: sweep}, tick))
	// Nothing to reason about: the floor is the only pacing control there is.
	require.Equal(t, 4320, effectiveFloorMinutes(&Config{MinIntervalMinutes: 4320, ScanIntervalMinutes: 0}, tick))
	require.Equal(t, 4320, effectiveFloorMinutes(&Config{MinIntervalMinutes: 4320, ScanIntervalMinutes: sweep}, 0))
	require.Zero(t, effectiveFloorMinutes(nil, tick))
}

func TestDetectMinIntervalMinutesOverride(t *testing.T) {
	topo := topologyWithNodes([]*master_pb.DataNodeInfo{
		dataNode("n1", "127.0.0.1:1", 9001, map[string][]*master_pb.VolumeEcShardInformationMessage{
			"hdd": {ecInfo(1, "c1", 10, 4)},
		}),
	})
	h := &BitrotScanHandler{
		fetchTopology: func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error) {
			return topo, nil
		},
	}

	// Floor of 1 minute, below the weekly sweep target: a run 2 minutes ago is
	// outside it, so detection proceeds.
	sender := &recordingDetectionSender{}
	req := &plugin_pb.RunDetectionRequest{
		JobType:           jobType,
		LastSuccessfulRun: timestamppb.New(time.Now().Add(-2 * time.Minute)),
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{
			fieldMinIntervalMins: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
		},
	}
	require.NoError(t, h.Detect(context.Background(), req, sender))
	require.Len(t, sender.proposals.GetProposals(), 1)
}
