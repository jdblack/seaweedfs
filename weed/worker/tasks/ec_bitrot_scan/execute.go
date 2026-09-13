package ec_bitrot_scan

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
)

// Execute scrubs one EC volume's shards against the bitrot checksum sidecar,
// holder by holder, and — when auto repair is enabled — quarantines
// Reed-Solomon-confirmed corrupt shards (parity-bounded) so ec.rebuild
// regenerates them.
func (h *BitrotScanHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	if request == nil || request.GetJob() == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	job := request.GetJob()
	if jt := job.GetJobType(); jt != "" && jt != jobType {
		return fmt.Errorf("job type %q is not handled by ec_bitrot_scan worker", jt)
	}

	volumeID, collection, diskType, err := decodeJobParams(job)
	if err != nil {
		return err
	}
	cfg := deriveConfig(request.GetAdminConfigValues(), request.GetWorkerConfigValues())

	if err := sendProgress(sender, job, plugin_pb.JobState_JOB_STATE_ASSIGNED, 0, "assigned", "EC bitrot scan job accepted"); err != nil {
		return err
	}

	masters := masterAddresses(request.GetClusterContext())
	topo, err := h.fetchTopology(ctx, masters)
	if err != nil {
		return failJob(sender, job, fmt.Errorf("fetch topology: %w", err))
	}

	holders := holdersForVolume(topo, volumeID, collection, diskType)
	if len(holders) == 0 {
		return sendCompleted(sender, job, true,
			fmt.Sprintf("EC volume %d has no reachable shard holders; nothing to scan", volumeID),
			map[string]*plugin_pb.ConfigValue{
				"volume_id": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)}},
				"scanned":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
			})
	}

	parity := resolveParity(topo, volumeID, collection)

	scanCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.ScanTimeoutSeconds)*time.Second)
	defer cancel()

	var (
		totalFiles      int64
		brokenRefs      []ec.ShardRef
		integrityErrors []string
		indexErrors     []string
	)
	for i, holder := range holders {
		// Index check first (cheap): validate the .ecx needle index. A server
		// that cannot serve the INDEX mode is degraded, not failed.
		if cfg.CheckIndex {
			iresp, ierr := scrubVolumeOnHolder(scanCtx, h.grpcDialOption, holder, volumeID, volume_server_pb.VolumeScrubMode_INDEX)
			if ierr != nil {
				glog.V(1).Infof("ec_bitrot_scan: index check on %s for volume %d skipped: %v", holder, volumeID, ierr)
			} else {
				for _, d := range iresp.GetDetails() {
					indexErrors = append(indexErrors, fmt.Sprintf("[%s] %s", holder, d))
				}
			}
		}

		resp, scrubErr := scrubVolumeOnHolder(scanCtx, h.grpcDialOption, holder, volumeID, volume_server_pb.VolumeScrubMode_CHECKSUM)
		if scrubErr != nil {
			return failJob(sender, job, fmt.Errorf("scrub volume %d on %s: %w", volumeID, holder, scrubErr))
		}
		totalFiles += int64(resp.GetTotalFiles())
		for _, si := range resp.GetBrokenShardInfos() {
			if si.GetVolumeId() != volumeID {
				continue
			}
			brokenRefs = append(brokenRefs, ec.ShardRef{
				ShardID:     si.GetShardId(),
				Collection:  collection,
				NodeAddress: holder,
			})
		}
		for _, d := range resp.GetDetails() {
			integrityErrors = append(integrityErrors, fmt.Sprintf("[%s] %s", holder, d))
		}

		pct := float64(i+1) / float64(len(holders)) * 90.0
		_ = sendProgress(sender, job, plugin_pb.JobState_JOB_STATE_RUNNING, pct, "scanning",
			fmt.Sprintf("scanned %s (%d/%d holders)", holder, i+1, len(holders)))
	}

	brokenRefs = dedupeShardRefs(brokenRefs)
	return h.finishScan(ctx, sender, job, topo, masters, cfg, volumeID, collection, diskType,
		parity, totalFiles, brokenRefs, integrityErrors, indexErrors)
}

// finishScan applies opt-in auto repair and reports the run result.
func (h *BitrotScanHandler) finishScan(
	ctx context.Context,
	sender pluginworker.ExecutionSender,
	job *plugin_pb.JobSpec,
	topo *master_pb.TopologyInfo,
	masters []string,
	cfg *Config,
	volumeID uint32,
	collection, diskType string,
	parity int,
	totalFiles int64,
	brokenRefs []ec.ShardRef,
	integrityErrors []string,
	indexErrors []string,
) error {
	detected := len(brokenRefs)
	quarantined := 0

	if detected > 0 && cfg.AutoRepair {
		if detected > parity {
			// Parity bound: never quarantine more than EC can rebuild.
			integrityErrors = append(integrityErrors, fmt.Sprintf(
				"auto-repair skipped: %d broken shards exceed the volume's parity count %d", detected, parity))
		} else {
			n, qerr := h.repair.Quarantine(ctx, volumeID, brokenRefs)
			quarantined = n
			if qerr != nil {
				return failJob(sender, job, fmt.Errorf("quarantine volume %d: %w", volumeID, qerr))
			}
			if n > 0 {
				if rerr := h.repair.Rebuild(ctx, topo, masters, collection, volumeID, diskType); rerr != nil {
					return failJob(sender, job, fmt.Errorf("rebuild volume %d: %w", volumeID, rerr))
				}
			}
		}
	}

	unrepaired := detected - quarantined

	outputs := map[string]*plugin_pb.ConfigValue{
		"volume_id":          {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)}},
		"files_verified":     {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: totalFiles}},
		"broken_shards":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(detected)}},
		"quarantined_shards": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(quarantined)}},
		"integrity_errors":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(integrityErrors))}},
	}

	var activities []*plugin_pb.ActivityEvent
	for _, ref := range brokenRefs {
		activities = append(activities, pluginworker.BuildExecutorActivity("corruption",
			fmt.Sprintf("corrupt shard %d@%s", ref.ShardID, ref.NodeAddress)))
	}
	for _, e := range integrityErrors {
		activities = append(activities, pluginworker.BuildExecutorActivity("integrity", e))
	}

	// Index issues (.ecx), reported separately and kept out of the quarantine
	// decision: the remedy for a bad index is a rebuild / peer re-fetch, not
	// shard deletion.
	outputs["index_errors"] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(indexErrors))}}
	for _, e := range indexErrors {
		activities = append(activities, pluginworker.BuildExecutorActivity("index", e))
	}

	summary := summarizeRun(volumeID, totalFiles, detected, quarantined, len(integrityErrors))
	if unrepaired > 0 {
		summary += fmt.Sprintf("; %d shard(s) still corrupt (auto-repair off or beyond parity)", unrepaired)
	}
	if len(indexErrors) > 0 {
		summary += fmt.Sprintf("; %d index integrity issue(s)", len(indexErrors))
	}
	// A completed scan is a SUCCESSFUL run even when it finds corruption: the
	// finding is a diagnosis, not an execution failure. Surfacing it through the
	// activities/output values (above) keeps the cadence gate — which anchors on
	// successful runs — from being poisoned, so a fully corrupt fleet is not
	// re-scanned continuously. Success=false stays reserved for execution errors
	// (scrub/quarantine/rebuild RPC failures), which return via failJob earlier.
	return emitCompleted(sender, job, true, summary, "", outputs, activities)
}
