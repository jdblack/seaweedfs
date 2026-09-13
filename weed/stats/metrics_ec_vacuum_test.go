package stats

import (
	"testing"
)

func TestECVacuumMetricsRegistered(t *testing.T) {
	t.Cleanup(ECVacuumJobsSkippedCounter.Reset)

	// A CounterVec emits no family until it has a child, so create one. The plain
	// counters/gauge/histogram are emitted by Gather as soon as they are
	// registered, so they need no touch.
	ECVacuumJobsSkippedCounter.WithLabelValues("below_threshold").Inc()

	metrics, err := Gather.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	want := map[string]bool{
		"SeaweedFS_ecVacuum_jobs_detected_total":            false,
		"SeaweedFS_ecVacuum_jobs_executed_total":            false,
		"SeaweedFS_ecVacuum_jobs_failed_total":              false,
		"SeaweedFS_ecVacuum_jobs_skipped_total":             false,
		"SeaweedFS_ecVacuum_bytes_reclaimed_total":          false,
		"SeaweedFS_ecVacuum_shard_rebuild_seconds":          false,
		"SeaweedFS_ecVacuum_verify_failures_total":          false,
		"SeaweedFS_ecVacuum_rolled_back_total":              false,
		"SeaweedFS_ecVacuum_last_success_timestamp_seconds": false,
	}
	for _, mf := range metrics {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("%s: metric not registered with Gather", name)
		}
	}
}
