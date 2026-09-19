package ec_bitrot_scan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestScanSliceSize(t *testing.T) {
	cases := []struct {
		name                         string
		candidates, sweep, tick, cap int
		wantSlice, wantCycles        int
	}{
		// A 7-day sweep at a 4-hour tick is 42 cycles; the slice follows the roster.
		{"weekly, 822 volumes", 822, 10080, 240, 20, 20, 42},
		{"weekly, 2000 volumes", 2000, 10080, 240, 2000, 48, 42},
		{"weekly, 5000 volumes", 5000, 10080, 240, 5000, 120, 42},
		// The admin cap binds, which stretches the pass rather than dropping coverage.
		{"cap binds", 2000, 10080, 240, 20, 20, 42},
		// A roster with fewer volumes than cycles still gets one per cycle.
		{"tiny roster", 10, 10080, 240, 500, 1, 42},
		// Degenerate configs collapse to a single cycle.
		{"no sweep target", 822, 0, 240, 500, 500, 1},
		{"no tick", 822, 10080, 0, 500, 500, 1},
		{"empty roster", 0, 10080, 240, 500, 0, 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slice, cycles := scanSliceSize(tc.candidates, tc.sweep, tc.tick, tc.cap)
			require.Equal(t, tc.wantSlice, slice)
			require.Equal(t, tc.wantCycles, cycles)
			// The slice never exceeds the admin's cap, and never drops below one
			// volume per cycle while there is anything to scan.
			require.LessOrEqual(t, slice, tc.cap)
			if tc.candidates > 0 {
				require.GreaterOrEqual(t, slice, 1)
			}
		})
	}
}

// TestRotationCoversEveryVolumeWithinAPass is the invariant the bookmark exists
// for: whatever the roster size and derived slice, one pass reaches every
// volume, and no single cycle proposes more than the slice.
func TestRotationCoversEveryVolumeWithinAPass(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7, 10, 822, 1000} {
		for _, slice := range []int{1, 2, 3, 7, 10, 20, 333} {
			candidates := make([]ecVolumeCandidate, 0, n)
			for i := 1; i <= n; i++ {
				candidates = append(candidates, ecVolumeCandidate{VolumeID: uint32(i), Collection: "c", DiskType: "hdd"})
			}
			// The same derivation Detect applies: a pass takes ceil(N/slice) cycles.
			cycles := (n + slice - 1) / slice

			h := &BitrotScanHandler{workingDir: t.TempDir()}
			seen := map[int64]int{}
			for cycle := 0; cycle < cycles; cycle++ {
				start := bookmarkPosition(candidates, readBookmark(h.workingDir))
				proposals, _ := buildProposals(candidates, slice, start)
				require.LessOrEqual(t, len(proposals), slice,
					"cycle %d proposed more than the slice (n=%d slice=%d)", cycle, n, slice)
				for _, p := range proposals {
					seen[p.GetParameters()["volume_id"].GetInt64Value()]++
				}
				h.advanceBookmark(candidates, start, slice)
			}
			require.Len(t, seen, n, "a full pass must reach every volume (n=%d slice=%d)", n, slice)
		}
	}
}

// TestDetectRotatesThroughTheRoster drives the rotation end to end: consecutive
// detection cycles, with the bookmark on disk carrying the position between
// them, must walk the roster instead of re-proposing its head.
func TestDetectRotatesThroughTheRoster(t *testing.T) {
	const volumes = 5
	ecInfos := make([]*master_pb.VolumeEcShardInformationMessage, 0, volumes)
	for i := 1; i <= volumes; i++ {
		ecInfos = append(ecInfos, ecInfo(uint32(i), "c1", 10, 4))
	}
	topo := topologyWithNodes([]*master_pb.DataNodeInfo{
		dataNode("n1", "127.0.0.1:1", 9001, map[string][]*master_pb.VolumeEcShardInformationMessage{"hdd": ecInfos}),
	})
	h := &BitrotScanHandler{
		workingDir: t.TempDir(),
		fetchTopology: func(ctx context.Context, masters []string) (*master_pb.TopologyInfo, error) {
			return topo, nil
		},
	}

	seen := map[int64]bool{}
	for cycle := 0; cycle < volumes; cycle++ {
		sender := &recordingDetectionSender{}
		req := &plugin_pb.RunDetectionRequest{
			JobType:    jobType,
			MaxResults: 1,
			// Weekly target at a 4-hour tick => ceil(5/42) = 1 volume per cycle.
			AdminRuntime: &plugin_pb.AdminRuntimeConfig{DetectionIntervalMinutes: 240},
		}
		require.NoError(t, h.Detect(context.Background(), req, sender))
		proposals := sender.proposals.GetProposals()
		require.Len(t, proposals, 1, "cycle %d", cycle)
		seen[proposals[0].GetParameters()["volume_id"].GetInt64Value()] = true
	}
	require.Len(t, seen, volumes, "consecutive cycles must cover the whole roster")
}
