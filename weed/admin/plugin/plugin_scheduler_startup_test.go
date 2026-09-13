package plugin

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestStartupSettleWindow(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{"uses the detection interval", 5 * time.Minute, 5 * time.Minute},
		{"caps a long interval", 6 * time.Hour, maxStartupSettleWindow},
		{"caps at the boundary", maxStartupSettleWindow, maxStartupSettleWindow},
		{"unset interval", 0, maxStartupSettleWindow},
		{"negative interval", -time.Minute, maxStartupSettleWindow},
	} {
		if got := startupSettleWindow(tc.interval); got != tc.want {
			t.Errorf("%s: startupSettleWindow(%v) = %v, want %v", tc.name, tc.interval, got, tc.want)
		}
	}
}

// TestFirstScanWaitsForClusterToSettle pins the deploy behaviour. A scan seconds
// after startup reads a topology that is still being rebuilt — the master and the
// volume servers are restarting and re-registering — and a half-populated
// topology reads as damage: ec_vacuum reported 665 of 674 volumes "missing a data
// shard" only because the holder of that shard had not checked in yet. So the
// first scan of a process waits one detection interval, and normal cadence
// resumes after that.
func TestFirstScanWaitsForClusterToSettle(t *testing.T) {
	t.Parallel()

	const jobType = "vacuum"
	const interval = 10 * time.Minute

	t.Run("fresh process defers its first scan by one interval", func(t *testing.T) {
		pluginSvc := newSchedulerTestPlugin(t, jobType, interval)
		ls := pluginSvc.lanes[LaneDefault]

		if _, due := pluginSvc.collectDueJobTypes(ls, []string{jobType}); len(due) != 0 {
			t.Fatalf("first scan must wait for the settling window, got %d due job type(s)", len(due))
		}

		nextAt, ok := pluginSvc.nextDetectionAt[jobType]
		if !ok {
			t.Fatal("expected the first scan to be scheduled, not left unscheduled")
		}
		if delta := time.Until(nextAt); delta < interval-time.Minute || delta > interval+time.Minute {
			t.Fatalf("first scan scheduled in %v, want about %v", delta, interval)
		}
	})

	t.Run("settled process keeps the short never-run stagger", func(t *testing.T) {
		pluginSvc := newSchedulerTestPlugin(t, jobType, interval)
		pluginSvc.startedAt = time.Now().UTC().Add(-interval - time.Second)
		ls := pluginSvc.lanes[LaneDefault]

		// Once past its settling window, a job type's first scan is only held
		// back by the never-run stagger, not by a whole interval.
		if _, due := pluginSvc.collectDueJobTypes(ls, []string{jobType}); len(due) != 0 {
			t.Fatalf("first iteration should only be staggered, got %d due job type(s)", len(due))
		}
		nextAt, ok := pluginSvc.nextDetectionAt[jobType]
		if !ok {
			t.Fatal("expected the first scan to be scheduled")
		}
		if delta := time.Until(nextAt); delta > time.Minute {
			t.Fatalf("settled process should scan shortly, scheduled in %v", delta)
		}

		// And the scan does come due as soon as that stagger elapses.
		pluginSvc.nextDetectionAt[jobType] = time.Now().UTC().Add(-time.Second)
		if _, due := pluginSvc.collectDueJobTypes(ls, []string{jobType}); len(due) != 1 {
			t.Fatalf("expected %q to be due after the stagger elapsed, got %d due job type(s)", jobType, len(due))
		}
	})
}

// newSchedulerTestPlugin builds a plugin whose background lane loops never start,
// so a test can drive collectDueJobTypes itself without racing them. Omitting the
// cluster-context provider is what keeps the loops idle.
func newSchedulerTestPlugin(t *testing.T, jobType string, interval time.Duration) *Plugin {
	t.Helper()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(pluginSvc.Shutdown)

	if err := pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
		JobType: jobType,
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{
			Enabled:                  true,
			DetectionIntervalMinutes: int32(interval / time.Minute),
		},
	}); err != nil {
		t.Fatalf("SaveJobTypeConfig: %v", err)
	}
	return pluginSvc
}
