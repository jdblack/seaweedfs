package stats

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Fork-local addition (not present in upstream SeaweedFS). The EC vacuum metrics
// live in their own file with their own registration, so upstream edits to
// metrics.go never conflict with them. Namespace and Gather come from metrics.go
// in this package.

// subsystemECVacuum is the Prometheus subsystem for the EC vacuum task.
const subsystemECVacuum = "ecVacuum"

var (
	// ECVacuumJobsDetectedCounter counts EC volumes the vacuum detector proposes
	// for compaction in each detection cycle.
	ECVacuumJobsDetectedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "jobs_detected_total",
			Help:      "Counter of EC volumes detected as requiring vacuum.",
		})

	// ECVacuumJobsExecutedCounter counts successful EC vacuum compactions.
	ECVacuumJobsExecutedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "jobs_executed_total",
			Help:      "Counter of successful EC vacuum compactions.",
		})

	// ECVacuumJobsFailedCounter counts failed EC vacuum compactions.
	ECVacuumJobsFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "jobs_failed_total",
			Help:      "Counter of failed EC vacuum compactions.",
		})

	// ECVacuumJobsSkippedCounter counts EC volumes the vacuum detector or executor
	// declined to compact, by reason (below_threshold, missing_data_shard,
	// no_live_entries).
	ECVacuumJobsSkippedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "jobs_skipped_total",
			Help:      "Counter of EC volumes skipped by vacuum, by reason.",
		}, []string{"reason"})

	// ECVacuumBytesReclaimedCounter counts storage bytes freed by EC vacuum.
	ECVacuumBytesReclaimedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "bytes_reclaimed_total",
			Help:      "Counter of storage bytes reclaimed by EC vacuum.",
		})

	// ECVacuumShardRebuildHistogram records the duration of the local
	// decode/compact/re-encode step of EC vacuum.
	ECVacuumShardRebuildHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "shard_rebuild_seconds",
			Help:      "Bucketed histogram of EC vacuum local shard rebuild duration.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		})

	// ECVacuumVerifyFailuresCounter counts re-encoded shard sets that failed the
	// decode-back verification before being distributed.
	ECVacuumVerifyFailuresCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "verify_failures_total",
			Help:      "Counter of EC vacuum re-encode verification failures.",
		})

	// ECVacuumRolledBackCounter counts redistributions that failed and were rolled
	// back to the collected originals.
	ECVacuumRolledBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "rolled_back_total",
			Help:      "Counter of EC vacuum redistributions rolled back to the original shards.",
		})

	// ECVacuumLastSuccessTimestamp is the unix time of the last successful vacuum
	// run, so a staleness alert can be built on the cadence gate.
	ECVacuumLastSuccessTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: subsystemECVacuum,
			Name:      "last_success_timestamp_seconds",
			Help:      "Unix timestamp of the last successful EC vacuum run.",
		})
)

func init() {
	Gather.MustRegister(ECVacuumJobsDetectedCounter)
	Gather.MustRegister(ECVacuumJobsExecutedCounter)
	Gather.MustRegister(ECVacuumJobsFailedCounter)
	Gather.MustRegister(ECVacuumJobsSkippedCounter)
	Gather.MustRegister(ECVacuumBytesReclaimedCounter)
	Gather.MustRegister(ECVacuumShardRebuildHistogram)
	Gather.MustRegister(ECVacuumVerifyFailuresCounter)
	Gather.MustRegister(ECVacuumRolledBackCounter)
	Gather.MustRegister(ECVacuumLastSuccessTimestamp)
}
