package ec_vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
)

// Execute collects one EC volume's shards onto the worker, compacts them locally,
// and distributes the compacted shards back. The heavy decode/compact/re-encode
// work happens entirely in the worker's working directory; the volume servers
// only serve shard transfers. The vacuum itself is the shared
// ecvacuum.VacuumVolume, so this worker and the `ec.vacuum` shell command behave
// identically.
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

	volumeID, collection, diskType, err := decodeJobParams(job)
	if err != nil {
		return err
	}
	cfg := deriveConfig(request.GetAdminConfigValues(), request.GetWorkerConfigValues())

	if err := sendProgress(sender, job, plugin_pb.JobState_JOB_STATE_ASSIGNED, 0, "assigned", "EC vacuum job accepted"); err != nil {
		return err
	}

	masters := masterAddresses(request.GetClusterContext())
	topo, err := h.fetchTopology(ctx, masters)
	if err != nil {
		return failJob(sender, job, fmt.Errorf("fetch topology: %w", err))
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
	glog.V(1).Infof("ec_vacuum: %s", summary)
	return emitCompleted(sender, job, true, summary, "", vacuumOutputs(volumeID, out.Reclaimed, out.NewShardBytes),
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
