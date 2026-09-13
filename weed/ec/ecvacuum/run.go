// Package ecvacuum is the shared core for reclaiming the space wasted by deleted
// needles in erasure-coded volumes: candidate enumeration (EnumerateGarbageEcVolumes),
// the shard transport that collects and redistributes EC shards (ClusterTransport),
// and the local decode/compact/re-encode step (VacuumLocalDir, VacuumVolume).
//
// It is shared by the ec_vacuum plugin worker (weed/worker/tasks/ec_vacuum) and the
// `ec.vacuum` shell command. It deliberately depends on neither weed/plugin/worker
// nor weed/shell, so both can import it without an import cycle.
package ecvacuum

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// RunOptions configures a single-volume vacuum.
type RunOptions struct {
	// WorkingDir is the scratch root; a per-volume subdirectory is created under
	// it and removed when the vacuum finishes (success or failure). Empty uses
	// os.TempDir().
	WorkingDir string
	// Timeout bounds the whole collect -> compact -> redistribute cycle. Zero
	// means no extra bound beyond the caller's context.
	Timeout time.Duration
	// OnStage, when set, is called as the vacuum moves between stages. Returning
	// an error aborts the vacuum (e.g. a progress sender that lost its worker).
	OnStage func(stage, message string) error
}

func (o RunOptions) stage(stage, message string) error {
	if o.OnStage == nil {
		return nil
	}
	return o.OnStage(stage, message)
}

// Outcome reports what a single-volume vacuum did.
type Outcome struct {
	VolumeID      uint32
	Skipped       bool // the volume had no live entries; nothing was re-encoded
	OldShardBytes int64
	NewShardBytes int64
	Reclaimed     int64
}

// VacuumVolume runs one EC volume vacuum against a live cluster: it collects the
// volume's shards onto this host, folds every holder's delete journal, decodes
// and compacts the volume locally, re-encodes a fresh shard set, and redistributes
// it. If redistribution fails partway, the collected originals are pushed back so
// the holders are not left on mixed generations.
//
// It is the shared core used both by the ec_vacuum plugin worker and the
// `ec.vacuum` shell command.
func VacuumVolume(ctx context.Context, transport ShardTransport, target VacuumTarget, opts RunOptions) (Outcome, error) {
	out := Outcome{VolumeID: target.VolumeID}
	base := ShardBaseName(target.Collection, target.VolumeID)

	root := opts.WorkingDir
	if root == "" {
		root = os.TempDir()
	}
	root = filepath.Join(root, fmt.Sprintf("ec_vacuum_%s_%d", target.Collection, target.VolumeID))
	origDir := filepath.Join(root, "orig")
	scratchDir := filepath.Join(root, "work")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return out, err
	}
	defer os.RemoveAll(root)

	execCtx := ctx
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	if err := opts.stage("collecting", fmt.Sprintf("Collecting %d shard(s) for volume %d", len(target.Holders), target.VolumeID)); err != nil {
		return out, err
	}
	collected, err := transport.Collect(execCtx, target, origDir, base)
	if err != nil {
		return out, fmt.Errorf("collect shards for volume %d: %w", target.VolumeID, err)
	}
	if !HasAllDataShards(shardBitsFromIDs(collected), target.DataShards) {
		return out, fmt.Errorf("collected shards %v are missing a data shard for volume %d; leaving it for ec.rebuild", collected, target.VolumeID)
	}

	vacOpts, err := LoadVacuumOptions(origDir, base, target)
	if err != nil {
		return out, fmt.Errorf("read volume %d metadata: %w", target.VolumeID, err)
	}

	// Compact a scratch copy so the collected originals survive untouched: a
	// failed verify or redistribute can roll the holders back to them.
	if err := copyEcArtifacts(origDir, scratchDir, base, target.DataShards+target.ParityShards); err != nil {
		return out, fmt.Errorf("stage volume %d for compaction: %w", target.VolumeID, err)
	}

	if err := opts.stage("compacting", "Decoding and compacting the volume locally"); err != nil {
		return out, err
	}
	rebuildStart := time.Now()
	vacRes, err := VacuumLocalDir(scratchDir, base, vacOpts)
	stats.ECVacuumShardRebuildHistogram.Observe(time.Since(rebuildStart).Seconds())
	if err != nil && strings.Contains(err.Error(), erasure_coding.EcNoLiveEntriesSubstring) {
		stats.ECVacuumJobsSkippedCounter.WithLabelValues("no_live_entries").Inc()
		stats.ECVacuumLastSuccessTimestamp.SetToCurrentTime()
		out.Skipped = true
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("vacuum volume %d: %w", target.VolumeID, err)
	}

	if err := opts.stage("redistributing", "Distributing the compacted shards"); err != nil {
		return out, err
	}
	if err := transport.Redistribute(execCtx, target, scratchDir, base, true); err != nil {
		// The compacted set is verified, but the push failed partway, leaving some
		// holders on the new generation and others on the old. Restore the
		// collected originals so the volume is not left mixed-generation. The
		// rollback keeps the holders' journals: the original generation's .ecx
		// still contains those needles, so its deletes are real.
		if rbErr := transport.Redistribute(execCtx, target, origDir, base, false); rbErr != nil {
			glog.Errorf("ec_vacuum: volume %d: redistribute failed (%v) and rollback failed (%v); run ec.rebuild", target.VolumeID, err, rbErr)
			return out, fmt.Errorf("redistribute shards for volume %d: %w (rollback also failed: %v; run ec.rebuild)", target.VolumeID, err, rbErr)
		}
		stats.ECVacuumRolledBackCounter.Inc()
		return out, fmt.Errorf("redistribute shards for volume %d: %w (rolled back to the original shards)", target.VolumeID, err)
	}

	out.OldShardBytes = vacRes.OldShardBytes
	out.NewShardBytes = vacRes.NewShardBytes
	out.Reclaimed = vacRes.OldShardBytes - vacRes.NewShardBytes
	if out.Reclaimed < 0 {
		out.Reclaimed = 0
	}
	stats.ECVacuumJobsExecutedCounter.Inc()
	if out.Reclaimed > 0 {
		stats.ECVacuumBytesReclaimedCounter.Add(float64(out.Reclaimed))
	}
	stats.ECVacuumLastSuccessTimestamp.SetToCurrentTime()
	return out, nil
}
