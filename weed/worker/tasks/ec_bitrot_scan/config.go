// Package ec_bitrot_scan implements the "ec_bitrot_scan" plugin job type: a
// scheduled, paginated, concurrency-limited scan that verifies every EC shard's
// raw bytes against the per-block CRC32C checksum sidecar (the same read-only
// verification behind the "ec.scrub -mode checksum" shell command), including
// cold parity shards that normal serving never reads.
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

	// defaultMinIntervalMinutes is the default spacing between sweep runs (3
	// days). Bitrot accrues over months, so a slow cadence is deliberate; this
	// diverges from the enterprise default of 300 seconds, which (combined with
	// 10-minute detection) amounts to near-continuous scanning.
	defaultMinIntervalMinutes = 3 * 24 * 60 // 4320, i.e. 3 days

	// defaultScanTimeoutSeconds bounds a single volume's holder-by-holder scrub.
	defaultScanTimeoutSeconds = 1800

	fieldCollectionFilter = "collection_filter"
	fieldDiskType         = "disk_type"
	fieldAutoRepair       = "auto_repair"
	fieldCheckIndex       = "check_index"
	fieldMinIntervalMins  = "min_interval_minutes"
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
	// MinIntervalMinutes skips detection when the last successful run is more
	// recent than this many minutes.
	MinIntervalMinutes int
	// ScanTimeoutSeconds bounds a single volume's scrub across its holders.
	ScanTimeoutSeconds int
}

// NewDefaultConfig returns the defaults: read-only (no auto-repair), metadata
// checks on, a 3-day scan interval, and a 1800s per-volume scan timeout.
func NewDefaultConfig() *Config {
	return &Config{
		AutoRepair:         false,
		CheckIndex:         true,
		MinIntervalMinutes: defaultMinIntervalMinutes,
		ScanTimeoutSeconds: defaultScanTimeoutSeconds,
	}
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
