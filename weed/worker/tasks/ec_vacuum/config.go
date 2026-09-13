// Package ec_vacuum implements the "ec_vacuum" plugin job type: a scheduled
// worker job that reclaims the space wasted by deleted needles in erasure-coded
// volumes.
//
// An EC volume's .ecx is a sealed sorted index written at encode time; runtime
// deletes are appended to an .ecj journal and masked out at read time, so the
// bytes of deleted needles stay allocated in the shards indefinitely. ec_vacuum
// detects EC volumes whose deleted-needle ratio has crossed a threshold, then
// collects each volume's shards onto the worker's local -workingDir, decodes
// them into a normal volume, compacts the deleted needles away, re-encodes a
// fresh shard set, and distributes the compacted shards back — so the volume
// servers never carry the compaction CPU or disk cost. This mirrors the design
// of SeaweedFS Enterprise's Automatic EC Vacuum; the detection and compaction
// primitives are all part of the open-source storage layer.
package ec_vacuum

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
)

const (
	// jobType is the canonical plugin job type string.
	jobType = "ec_vacuum"

	// defaultGarbageThreshold is the deleted-needle ratio at which a volume is
	// proposed for vacuum, matching the enterprise default of 0.30.
	defaultGarbageThreshold = 0.30

	// defaultMinIntervalMinutes matches the enterprise default scan cadence
	// (every 30 minutes).
	defaultMinIntervalMinutes = 30

	// defaultScanTimeoutSeconds bounds one volume's collect → compact →
	// redistribute cycle.
	defaultScanTimeoutSeconds = 3600

	fieldCollectionFilter = "collection_filter"
	fieldDiskType         = "disk_type"
	fieldGarbageThreshold = "garbage_threshold"
	fieldMinIntervalMins  = "min_interval_minutes"
	fieldScanTimeoutSecs  = "scan_timeout_seconds"
)

// Config is the resolved configuration for one ec_vacuum detection or execution
// cycle. It is built from the admin + worker descriptor config values.
type Config struct {
	// CollectionFilter scopes vacuums to matching collections (empty = all).
	CollectionFilter string
	// DiskType scopes vacuums to EC shards on a disk type (empty = all).
	DiskType string
	// GarbageThreshold is the deleted-needle ratio (deleteCount / fileCount) a
	// volume must reach to be proposed for vacuum.
	GarbageThreshold float64
	// MinIntervalMinutes skips detection when the last successful run is more
	// recent than this many minutes.
	MinIntervalMinutes int
	// ScanTimeoutSeconds bounds a single volume's vacuum cycle.
	ScanTimeoutSeconds int
}

// NewDefaultConfig returns the defaults: a 30% delete-ratio threshold, a
// 30-minute detection cadence, and a one-hour per-volume timeout.
func NewDefaultConfig() *Config {
	return &Config{
		GarbageThreshold:   defaultGarbageThreshold,
		MinIntervalMinutes: defaultMinIntervalMinutes,
		ScanTimeoutSeconds: defaultScanTimeoutSeconds,
	}
}

// deriveConfig resolves a Config from the admin- and worker-side descriptor
// config values, falling back to the defaults for anything absent.
func deriveConfig(adminValues, workerValues map[string]*plugin_pb.ConfigValue) *Config {
	cfg := NewDefaultConfig()

	cfg.CollectionFilter = strings.TrimSpace(pluginworker.ReadStringConfig(adminValues, fieldCollectionFilter, cfg.CollectionFilter))
	cfg.DiskType = strings.TrimSpace(pluginworker.ReadStringConfig(adminValues, fieldDiskType, cfg.DiskType))

	if v := pluginworker.ReadDoubleConfig(adminValues, fieldGarbageThreshold, cfg.GarbageThreshold); v > 0 && v <= 1 {
		cfg.GarbageThreshold = v
	} else if v != 0 {
		glog.V(1).Infof("ec_vacuum: ignoring out-of-range garbage_threshold %v; using %v", v, cfg.GarbageThreshold)
	}

	if v := pluginworker.ReadIntConfig(workerValues, fieldMinIntervalMins, cfg.MinIntervalMinutes); v > 0 {
		cfg.MinIntervalMinutes = v
	}
	if v := pluginworker.ReadIntConfig(workerValues, fieldScanTimeoutSecs, cfg.ScanTimeoutSeconds); v > 0 {
		cfg.ScanTimeoutSeconds = v
	}

	return cfg
}
