package ec_vacuum

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// sizedEcEntry builds one volume's shard entry, with reported shard sizes: the
// sizes are what make the volume's fullness computable, so an entry without them is
// the "older volume server" case the decode route declines to judge.
func sizedEcEntry(collection string, vid uint32, shardSize int64, fileCount, deleteCount uint64) *master_pb.VolumeEcShardInformationMessage {
	sizes := make([]int64, 14)
	for i := range sizes {
		sizes[i] = shardSize
	}
	return &master_pb.VolumeEcShardInformationMessage{
		Id:           vid,
		Collection:   collection,
		EcIndexBits:  0x3FFF,
		DataShards:   10,
		ParityShards: 4,
		FileCount:    fileCount,
		DeleteCount:  deleteCount,
		EncodeTsNs:   1,
		ShardSizes:   sizes,
	}
}

func sizedEcTopo(collection string, vid uint32, shardSize int64, fileCount, deleteCount uint64) *master_pb.TopologyInfo {
	return ecTopoOf(sizedEcEntry(collection, vid, shardSize, fileCount, deleteCount))
}

// ecTopoOf puts the given EC shard entries on one node.
func ecTopoOf(entries ...*master_pb.VolumeEcShardInformationMessage) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "r1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id:      "n1",
					Address: "127.0.0.1:8080",
					DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {MaxVolumeCount: 100, EcShardInfos: entries},
					},
				}},
			}},
		}},
	}
}

// decodeTestLimitMb is the volume size limit the routing tests measure fullness
// against. With 10 data shards of 1 MB each a volume is 10 MB encoded, so the
// limit decides whether a given live size reads as undersized.
const decodeTestLimitMb = 40

func detectWith(t *testing.T, topo *master_pb.TopologyInfo, limitMb uint64, values map[string]*plugin_pb.ConfigValue) *fakeDetectionSender {
	t.Helper()
	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, uint64, error) {
		return topo, limitMb, nil
	}
	sender := &fakeDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:           jobType,
		AdminConfigValues: values,
	}, sender)
	require.NoError(t, err)
	return sender
}

// TestDetectRoutesUndersizedVolumeToDecode verifies that a volume whose live data
// sits below the decode threshold is proposed for decode, and that it does not
// also get a vacuum: 50% deleted clears the vacuum threshold too, and the two
// actions cannot run against the same shard set.
func TestDetectRoutesUndersizedVolumeToDecode(t *testing.T) {
	// 10 MB encoded, half deleted -> 5 MB live -> 12.5% of a 40 MB limit.
	sender := detectWith(t, sizedEcTopo("c1", 7, 1<<20, 100, 50), decodeTestLimitMb, map[string]*plugin_pb.ConfigValue{
		fieldDecodeBelowFullness: {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.50}},
	})

	require.Len(t, sender.proposals, 1, "one proposal per volume: decode wins over vacuum")
	require.Equal(t, modeDecode, sender.proposals[0].GetParameters()[fieldMode].GetStringValue())
	require.Equal(t, int64(7), sender.proposals[0].GetParameters()["volume_id"].GetInt64Value())
	require.Contains(t, sender.proposals[0].GetDetail(), "fullness=12.5%")
	require.Equal(t, int32(1), sender.complete.GetTotalProposals())
}

// TestDetectDecodesEmptiedVolume verifies the extreme case — a volume with no live
// data left, which a vacuum can only re-encode into a fresh near-empty shard set —
// is decoded, and at high priority.
func TestDetectDecodesEmptiedVolume(t *testing.T) {
	sender := detectWith(t, sizedEcTopo("c1", 9, 1<<20, 100, 100), decodeTestLimitMb, nil)

	require.Len(t, sender.proposals, 1)
	require.Equal(t, modeDecode, sender.proposals[0].GetParameters()[fieldMode].GetStringValue())
	require.Equal(t, plugin_pb.JobPriority_JOB_PRIORITY_HIGH, sender.proposals[0].GetPriority())
}

// TestDetectKeepsLiveVolumeOnVacuum verifies the threshold boundary: a volume
// above the decode threshold stays on the vacuum route even though it is deleted
// past the vacuum threshold.
func TestDetectKeepsLiveVolumeOnVacuum(t *testing.T) {
	// 30 MB encoded, 30% deleted -> 21 MB live -> 52.5% of a 40 MB limit, just
	// over the 50% threshold.
	sender := detectWith(t, sizedEcTopo("c1", 11, 3<<20, 100, 30), decodeTestLimitMb, nil)

	require.Len(t, sender.proposals, 1)
	require.Equal(t, modeVacuum, sender.proposals[0].GetParameters()[fieldMode].GetStringValue())
	require.NotContains(t, sender.proposals[0].GetDetail(), "fullness")
}

// TestDetectSkipsDecodeWhenDisabled verifies that 0 turns the route off and leaves
// the volume to the vacuum it would otherwise have been.
func TestDetectSkipsDecodeWhenDisabled(t *testing.T) {
	sender := detectWith(t, sizedEcTopo("c1", 7, 1<<20, 100, 50), decodeTestLimitMb, map[string]*plugin_pb.ConfigValue{
		fieldDecodeBelowFullness: {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
	})

	require.Len(t, sender.proposals, 1)
	require.Equal(t, modeVacuum, sender.proposals[0].GetParameters()[fieldMode].GetStringValue())
}

// TestDetectDecodeSkippedWithoutVolumeSizeLimit verifies that a missing volume size
// limit costs only the decode route. Fullness is measured against that limit, and
// guessing it is how a cluster nominates every EC volume it owns, so the route
// declines — loudly, not silently — and vacuum detection still runs.
func TestDetectDecodeSkippedWithoutVolumeSizeLimit(t *testing.T) {
	sender := detectWith(t, sizedEcTopo("c1", 7, 1<<20, 100, 50), 0, nil)

	require.Len(t, sender.proposals, 1)
	require.Equal(t, modeVacuum, sender.proposals[0].GetParameters()[fieldMode].GetStringValue())
	require.Greater(t, sender.activities, 0, "the skip must be reported, not silent")
	require.True(t, sender.complete.GetSuccess())
}

// TestParseJobParams verifies the mode default and the rejection of an unknown one.
func TestParseJobParams(t *testing.T) {
	job := &plugin_pb.JobSpec{Parameters: map[string]*plugin_pb.ConfigValue{
		"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7}},
		"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "c1"}},
		"disk_type":  {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "hdd"}},
	}}

	mode, vid, collection, diskType, err := parseJobParams(job)
	require.NoError(t, err)
	require.Equal(t, modeVacuum, mode, "an absent mode is a vacuum, so proposals queued before the decode route still execute")
	require.Equal(t, uint32(7), vid)
	require.Equal(t, "c1", collection)
	require.Equal(t, "hdd", diskType)

	job.Parameters[fieldMode] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringValue{StringValue: modeDecode}}
	mode, _, _, _, err = parseJobParams(job)
	require.NoError(t, err)
	require.Equal(t, modeDecode, mode)

	job.Parameters[fieldMode] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "compost"}}
	_, _, _, _, err = parseJobParams(job)
	require.Error(t, err)
}

// decodeJob builds a decode-mode job spec for the given volume.
func decodeJob(volumeID int64, collection, diskType string) *plugin_pb.JobSpec {
	return &plugin_pb.JobSpec{
		JobId:   "ec-decode-test",
		JobType: jobType,
		Parameters: map[string]*plugin_pb.ConfigValue{
			fieldMode:    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: modeDecode}},
			"volume_id":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: volumeID}},
			"collection": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: collection}},
			"disk_type":  {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: diskType}},
		},
	}
}

// TestExecuteDecodeRoutesToDecodeVolume verifies a decode-mode job reaches the
// decode action with its target intact, and does not touch the vacuum transport —
// the two modes must not be able to run each other's work.
func TestExecuteDecodeRoutesToDecodeVolume(t *testing.T) {
	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, uint64, error) {
		return syntheticTopo("c1", 7, 0x3FFF, 10), 0, nil
	}
	vacuumTransport := &fakeTransport{srcDir: t.TempDir()}
	handler.transport = vacuumTransport

	type decoded struct {
		volumeID   uint32
		collection string
		diskType   string
	}
	var calls []decoded
	handler.decodeVolume = func(_ context.Context, _ []string, _ *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) error {
		calls = append(calls, decoded{volumeID, collection, diskType})
		return nil
	}

	sender := &recordingSender{}
	err := handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: decodeJob(7, "c1", "hdd")}, sender)
	require.NoError(t, err)
	require.Equal(t, []decoded{{7, "c1", "hdd"}}, calls)
	require.True(t, sender.completed.GetSuccess())
	require.Contains(t, sender.completed.GetResult().GetSummary(), "Decoded undersized EC volume 7")
	require.Equal(t, modeDecode, sender.completed.GetResult().GetOutputValues()["action"].GetStringValue())
	require.Empty(t, vacuumTransport.redistributeDirs, "a decode must not run the vacuum pipeline")
}

// TestExecuteDecodeFailureFailsJob verifies a failed decode is reported as a failed
// job rather than a success. failJob reports through a FAILED progress update and
// the returned error, so there is no completion to report.
func TestExecuteDecodeFailureFailsJob(t *testing.T) {
	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, uint64, error) {
		return syntheticTopo("c1", 7, 0x3FFF, 10), 0, nil
	}
	handler.decodeVolume = func(context.Context, []string, *master_pb.TopologyInfo, uint32, string, string) error {
		return errors.New("induced decode failure")
	}

	sender := &recordingSender{}
	err := handler.Execute(context.Background(), &plugin_pb.ExecuteJobRequest{Job: decodeJob(7, "c1", "hdd")}, sender)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode EC volume 7")
	require.Contains(t, err.Error(), "induced decode failure")
	require.Nil(t, sender.completed, "a failed decode must not report a completion")
}

// TestDetectCapsCombinedProposals verifies the result cap spans both actions, and
// that the budget goes to the decode proposals first.
func TestDetectCapsCombinedProposals(t *testing.T) {
	topo := ecTopoOf(
		sizedEcEntry("c1", 7, 1<<20, 100, 50), // 10 MB encoded, 5 MB live -> decode
		sizedEcEntry("c1", 8, 3<<20, 100, 30), // 30 MB encoded, 21 MB live -> vacuum
	)
	handler := NewEcVacuumHandler(nil, t.TempDir())
	handler.fetchTopology = func(context.Context, []string) (*master_pb.TopologyInfo, uint64, error) {
		return topo, decodeTestLimitMb, nil
	}

	sender := &fakeDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:    jobType,
		MaxResults: 1,
	}, sender)
	require.NoError(t, err)
	require.Len(t, sender.proposals, 1, "the cap covers both actions together")
	require.Equal(t, modeDecode, sender.proposals[0].GetParameters()[fieldMode].GetStringValue(),
		"the budget goes to the decode before the vacuum")
	require.Equal(t, int32(1), sender.complete.GetTotalProposals())
}

// TestDetectSkipsFullyDeletedVolumeForVacuum verifies that a volume with no live
// needles is not proposed for compaction when the decode route is off: nothing can
// be compacted, so a vacuum proposal would cost a full collect of every shard per
// cycle and then skip.
func TestDetectSkipsFullyDeletedVolumeForVacuum(t *testing.T) {
	sender := detectWith(t, sizedEcTopo("c1", 9, 1<<20, 100, 100), decodeTestLimitMb, map[string]*plugin_pb.ConfigValue{
		fieldDecodeBelowFullness: {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
	})

	require.Empty(t, sender.proposals, "a fully-deleted volume is not a vacuum candidate")
	require.Equal(t, int32(0), sender.complete.GetTotalProposals())
}

// TestDedupeByVolumeKeepsTheFirstGroup verifies the decode list is one proposal per
// volume: the shared enumeration is per (volume, collection, disk type), while a
// decode acts on the whole volume, so a second group's proposal could only race
// the first and fail.
func TestDedupeByVolumeKeepsTheFirstGroup(t *testing.T) {
	in := []garbageCandidate{
		{VolumeID: 7, Collection: "c1", DiskType: "hdd"},
		{VolumeID: 7, Collection: "c1", DiskType: "ssd"},
		{VolumeID: 8, Collection: "c1", DiskType: "hdd"},
	}

	out := dedupeByVolume(in)

	require.Len(t, out, 2)
	require.Equal(t, in[0], out[0], "the first group is kept")
	require.Equal(t, uint32(8), out[1].VolumeID)
}
