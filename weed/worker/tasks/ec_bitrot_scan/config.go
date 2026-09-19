// Package ec_bitrot_scan implements the "ec_bitrot_scan" plugin job type: a
// scheduled, paginated, concurrency-limited scan that verifies every EC shard's
// raw bytes against the per-block CRC32C checksum sidecar (the same read-only
// verification behind the "ec.scrub -mode checksum" shell command), including
// cold parity shards that normal serving never reads.
//
// Every EC volume is a candidate, so detection walks the roster as a rotation:
// each cycle proposes the next slice of volumes, wrapping to the front, and a
// bookmark in the worker's working directory records where the last cycle
// stopped. The slice size is derived from ScanIntervalMinutes and the admin's
// detection interval (see scanSliceSize), so one full pass stays at the
// configured length as the roster grows — a fixed batch size would stretch the
// pass linearly with the fleet.
//
// With CheckIndex enabled (the default) it also runs the INDEX scrub,
// validating the .ecx needle index. Index findings are reported separately and
// never auto-repaired.
//
// With AutoRepair disabled (the default) the job is strictly read-only. With it
// enabled, a scrub that reports broken shards (Reed-Solomon-confirmed corruption,
// never a stale-sidecar false positive) quarantines each one — unmount + delete on
// its holder, bounded by the volume's parity count — so the normal ec.rebuild
// regenerates a clean, RS-verified copy.
package ec_bitrot_scan

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
)

const (
	// jobType is the canonical plugin job type string.
	jobType = "ec_bitrot_scan"

	// defaultScanIntervalMinutes is the target time for one full pass over the
	// EC volume roster: 7 days. Bitrot accrues over months, so a slow cadence is
	// deliberate — the point of the sweep is to catch the rare silent
	// corruption while parity can still rebuild it, not to re-read the fleet
	// continuously.
	//
	// The per-cycle slice is derived from this and the admin's detection
	// interval (see scanSliceSize), so the pass length stays put as the roster
	// grows instead of stretching the way a fixed batch size would.
	defaultScanIntervalMinutes = 7 * 24 * 60 // 10080, i.e. weekly

	// defaultMinIntervalMinutes is the default value of the OPTIONAL floor on
	// how often this job type may run. 0 disables it, which is the default: the
	// cadence target above is what paces the sweep, and a floor layered on top
	// of it only stretches one pass across several targets. It remains
	// available for the degenerate case (a roster small enough that the derived
	// slice cannot lengthen the pass).
	defaultMinIntervalMinutes = 0

	// defaultScanTimeoutSeconds bounds a single volume's holder-by-holder scrub.
	defaultScanTimeoutSeconds = 1800

	fieldCollectionFilter = "collection_filter"
	fieldDiskType         = "disk_type"
	fieldAutoRepair       = "auto_repair"
	fieldCheckIndex       = "check_index"
	fieldMinIntervalMins  = "min_interval_minutes"
	fieldScanIntervalMins = "scan_interval_minutes"
	fieldScanTimeoutSecs  = "scan_timeout_seconds"
)

// Config is the resolved configuration for one ec_bitrot_scan detection or
// execution cycle. It is built from the admin + worker descriptor config values.
type Config struct {
	// CollectionFilter scopes scrubs to matching collections (empty = all).
	CollectionFilter string
	// DiskType scopes scrubs to EC shards on a disk type (empty = all).
	DiskType string
	// AutoRepair, when true, quarantines RS-confirmed broken shards (unmount +
	// delete, parity-bounded) so ec.rebuild regenerates them. Off = read-only.
	AutoRepair bool
	// CheckIndex, when true, also verifies the .ecx needle index (the INDEX
	// scrub mode) alongside the shard checksums. Index issues are reported,
	// never auto-repaired.
	CheckIndex bool
	// MinIntervalMinutes is an OPTIONAL floor on how often this job type may
	// run: when > 0 and no longer than the admin's detection interval,
	// detection is skipped when the last successful run is more recent than
	// this. 0 disables it. A longer floor is ignored, because it would stretch
	// one sweep over several detection intervals.
	MinIntervalMinutes int
	// ScanIntervalMinutes is the target time for one full pass over the
	// roster. Detection derives its per-cycle slice from this and the admin's
	// detection interval, so the pass length holds as the fleet grows.
	ScanIntervalMinutes int
	// ScanTimeoutSeconds bounds a single volume's scrub across its holders.
	ScanTimeoutSeconds int
}

// NewDefaultConfig returns the defaults: read-only (no auto-repair), metadata
// checks on, a weekly full pass, no minimum-interval floor, and a 1800s
// per-volume scan timeout.
func NewDefaultConfig() *Config {
	return &Config{
		AutoRepair:          false,
		CheckIndex:          true,
		MinIntervalMinutes:  defaultMinIntervalMinutes,
		ScanIntervalMinutes: defaultScanIntervalMinutes,
		ScanTimeoutSeconds:  defaultScanTimeoutSeconds,
	}
}

// effectiveFloorMinutes returns the minimum-interval floor actually applied: 0
// when the operator left it unset, or when applying it would break the sweep
// target. A pass is ceil(sweep/tick) cycles, so a floor longer than the admin's
// detection interval makes detection run less often than the pass assumes and
// silently stretches it — the stored 4320 against a 4-hour tick would turn a
// weekly pass into a 126-day one. The sweep target is the explicit pacing knob,
// so such a floor is ignored and warned about rather than obeyed; a floor at or
// below the tick cannot stretch anything, and is honored because it is then the
// only thing keeping detection from running more often than intended.
func effectiveFloorMinutes(cfg *Config, tickMinutes int) int {
	if cfg == nil || cfg.MinIntervalMinutes <= 0 {
		return 0
	}
	if cfg.ScanIntervalMinutes <= 0 || tickMinutes <= 0 || cfg.MinIntervalMinutes <= tickMinutes {
		return cfg.MinIntervalMinutes
	}
	glog.Warningf("ec_bitrot_scan: min_interval_minutes=%d exceeds the %d-minute detection interval and is ignored; the %d-minute sweep target governs",
		cfg.MinIntervalMinutes, tickMinutes, cfg.ScanIntervalMinutes)
	return 0
}

// deriveConfig resolves a Config from the admin- and worker-side descriptor
// config values, falling back to the defaults for anything absent.
func deriveConfig(adminValues, workerValues map[string]*plugin_pb.ConfigValue) *Config {
	cfg := NewDefaultConfig()

	// Admin-side scope.
	cfg.CollectionFilter = strings.TrimSpace(pluginworker.ReadStringConfig(adminValues, fieldCollectionFilter, cfg.CollectionFilter))
	cfg.DiskType = strings.TrimSpace(pluginworker.ReadStringConfig(adminValues, fieldDiskType, cfg.DiskType))
	cfg.AutoRepair = readBoolConfig(adminValues, fieldAutoRepair, cfg.AutoRepair)
	cfg.CheckIndex = readBoolConfig(adminValues, fieldCheckIndex, cfg.CheckIndex)

	// Worker-side cadence/timeout. A non-positive value falls back to the default.
	if v := pluginworker.ReadIntConfig(workerValues, fieldMinIntervalMins, cfg.MinIntervalMinutes); v > 0 {
		cfg.MinIntervalMinutes = v
	}
	if v := pluginworker.ReadIntConfig(workerValues, fieldScanIntervalMins, cfg.ScanIntervalMinutes); v > 0 {
		cfg.ScanIntervalMinutes = v
	}
	if v := pluginworker.ReadIntConfig(workerValues, fieldScanTimeoutSecs, cfg.ScanTimeoutSeconds); v > 0 {
		cfg.ScanTimeoutSeconds = v
	}

	return cfg
}

// readBoolConfig reads a boolean plugin config value, accepting a native bool, a
// "true"/"1"/"yes" string, or a non-zero int, and falling back otherwise.
func readBoolConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback bool) bool {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_BoolValue:
		return kind.BoolValue
	case *plugin_pb.ConfigValue_StringValue:
		s := strings.TrimSpace(strings.ToLower(kind.StringValue))
		if s == "true" || s == "1" || s == "yes" {
			return true
		}
		if s == "false" || s == "0" || s == "no" {
			return false
		}
		glog.V(1).Infof("readBoolConfig: unrecognized string value %q for field %q, using fallback %v", kind.StringValue, field, fallback)
	case *plugin_pb.ConfigValue_Int64Value:
		return kind.Int64Value != 0
	default:
		glog.V(1).Infof("readBoolConfig: unexpected config value type %T for field %q, using fallback", value.Kind, field)
	}
	return fallback
}
