package ec_vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// Execute runs one EC volume job in the mode the proposal carried.
//
// A vacuum collects the volume's shards onto the worker, compacts them locally and
// distributes them back (the shared ecvacuum.VacuumVolume, so this worker and
// `ec.vacuum` behave identically). A decode regenerates a regular volume from the
// shards and drops them, entirely on a volume server, so it needs no worker scratch
// space. See decodeEcVolume for what the decode deliberately does not check.
func (h *VacuumHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	if request == nil || request.GetJob() == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	job := request.GetJob()
	if jt := job.GetJobType(); jt != "" && jt != jobType {
		return fmt.Errorf("job type %q is not handled by %s worker", jt, jobType)
	}

	mode, volumeID, collection, diskType, err := parseJobParams(job)
	if err != nil {
		return err
	}
	cfg := deriveConfig(request.GetAdminConfigValues(), request.GetWorkerConfigValues())

	accepted := "EC volume maintenance job accepted"
	if mode == modeDecode {
		accepted = "EC volume decode job accepted"
	}
	if err := sendProgress(sender, job, plugin_pb.JobState_JOB_STATE_ASSIGNED, 0, "assigned", accepted); err != nil {
		return err
	}

	masters := masterAddresses(request.GetClusterContext())
	topo, _, err := h.fetchTopology(ctx, masters)
	if err != nil {
		return failJob(sender, job, fmt.Errorf("fetch topology: %w", err))
	}

	if mode == modeDecode {
		return h.executeDecode(ctx, sender, job, masters, topo, volumeID, collection, diskType)
	}

	target, err := buildVacuumTarget(topo, volumeID, collection, diskType)
	if err != nil {
		return failJob(sender, job, err)
	}

	out, err := ecvacuum.VacuumVolume(ctx, h.transport, target, ecvacuum.RunOptions{
		WorkingDir: h.workingDir,
		Timeout:    time.Duration(cfg.ScanTimeoutSeconds) * time.Second,
		OnStage: func(stage, message string) error {
			return sendProgress(sender, job, plugin_pb.JobState_JOB_STATE_RUNNING, vacuumStagePercent(stage), stage, message)
		},
	})
	if err != nil {
		return failJob(sender, job, err)
	}

	if out.Skipped {
		summary := fmt.Sprintf("EC volume %d has no live entries; nothing to vacuum", volumeID)
		return emitCompleted(sender, job, true, summary, "", vacuumOutputs(volumeID, 0, 0),
			[]*plugin_pb.ActivityEvent{pluginworker.BuildExecutorActivity("completed", summary)})
	}

	summary := fmt.Sprintf("Vacuumed EC volume %d: %d -> %d shard bytes (reclaimed %d)",
		volumeID, out.OldShardBytes, out.NewShardBytes, out.Reclaimed)
	glog.Infof("ec_vacuum: %s", summary)
	return emitCompleted(sender, job, true, summary, "", vacuumOutputs(volumeID, out.Reclaimed, out.NewShardBytes),
		[]*plugin_pb.ActivityEvent{pluginworker.BuildExecutorActivity("completed", summary)})
}

// executeDecode decodes one undersized EC volume back to a regular volume. There is
// no worker-local work to report on, so the progress figures are coarse: the
// decode's own trace goes to stdout rather than being parsed for stage boundaries.
func (h *VacuumHandler) executeDecode(ctx context.Context, sender pluginworker.ExecutionSender, job *plugin_pb.JobSpec, masters []string, topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) error {
	if err := sendProgress(sender, job, plugin_pb.JobState_JOB_STATE_RUNNING, 10, "decoding",
		fmt.Sprintf("decoding EC volume %d back to a regular volume", volumeID)); err != nil {
		return err
	}
	if err := h.decodeVolume(ctx, masters, topo, volumeID, collection, diskType); err != nil {
		return failJob(sender, job, fmt.Errorf("decode EC volume %d: %w", volumeID, err))
	}
	stats.ECVacuumJobsDecodedCounter.Inc()

	summary := fmt.Sprintf("Decoded undersized EC volume %d back to a regular volume", volumeID)
	glog.Infof("ec_vacuum: %s", summary)
	return emitCompleted(sender, job, true, summary, "", decodeOutputs(volumeID),
		[]*plugin_pb.ActivityEvent{pluginworker.BuildExecutorActivity("completed", summary)})
}

// vacuumStagePercent maps a vacuum stage to the progress percentage the worker
// reports, matching the historical stage percentages.
func vacuumStagePercent(stage string) float64 {
	switch stage {
	case "collecting":
		return 10
	case "compacting":
		return 45
	case "redistributing":
		return 80
	}
	return 0
}
