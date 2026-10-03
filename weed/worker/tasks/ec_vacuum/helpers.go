package ec_vacuum

import (
	"fmt"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// The job has two actions, carried in the proposal parameters so one job type can
// order either: vacuum compacts a volume's shards and keeps it erasure-coded,
// decode regenerates a regular volume from the shards and drops them.
const (
	modeVacuum = "vacuum"
	modeDecode = "decode"
	fieldMode  = "mode"
)

// parseJobParams extracts the action and target from a job proposal's parameters.
// An absent mode means vacuum, so proposals queued by an older worker still run.
func parseJobParams(job *plugin_pb.JobSpec) (mode string, volumeID uint32, collection, diskType string, err error) {
	params := job.GetParameters()
	mode = pluginworker.ReadStringConfig(params, fieldMode, modeVacuum)
	if mode != modeVacuum && mode != modeDecode {
		return "", 0, "", "", fmt.Errorf("unknown job mode %q", mode)
	}
	vid := pluginworker.ReadInt64Config(params, "volume_id", -1)
	if vid < 0 || vid > math.MaxUint32 {
		return "", 0, "", "", fmt.Errorf("invalid volume_id %d", vid)
	}
	collection = pluginworker.ReadStringConfig(params, "collection", "")
	diskType = pluginworker.ReadStringConfig(params, "disk_type", "")
	return mode, uint32(vid), collection, diskType, nil
}

// dedupeByVolume keeps the first candidate for each volume id, preserving order.
// The shared enumeration is per (volume, collection, disk type), which for one
// volume is several candidates; an action that works on the whole volume needs
// one.
func dedupeByVolume(candidates []ecvacuum.Candidate) []ecvacuum.Candidate {
	if len(candidates) < 2 {
		return candidates
	}
	seen := make(map[uint32]bool, len(candidates))
	kept := make([]ecvacuum.Candidate, 0, len(candidates))
	for _, c := range candidates {
		if seen[c.VolumeID] {
			continue
		}
		seen[c.VolumeID] = true
		kept = append(kept, c)
	}
	return kept
}

// sendProgress emits a progress update with a matching activity event.
func sendProgress(sender pluginworker.ExecutionSender, job *plugin_pb.JobSpec, state plugin_pb.JobState, pct float64, stage, message string) error {
	return sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           job.GetJobId(),
		JobType:         job.GetJobType(),
		State:           state,
		ProgressPercent: pct,
		Stage:           stage,
		Message:         message,
		Activities:      []*plugin_pb.ActivityEvent{pluginworker.BuildExecutorActivity(stage, message)},
	})
}

// emitCompleted sends the final job completion with its result and activities.
func emitCompleted(
	sender pluginworker.ExecutionSender,
	job *plugin_pb.JobSpec,
	success bool,
	summary, errMsg string,
	outputs map[string]*plugin_pb.ConfigValue,
	activities []*plugin_pb.ActivityEvent,
) error {
	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:        job.GetJobId(),
		JobType:      job.GetJobType(),
		Success:      success,
		ErrorMessage: errMsg,
		Result: &plugin_pb.JobResult{
			Summary:      summary,
			OutputValues: outputs,
		},
		Activities: activities,
	})
}

// failJob reports a transport/execution failure and returns the error so the
// framework records the job as failed.
func failJob(sender pluginworker.ExecutionSender, job *plugin_pb.JobSpec, err error) error {
	stats.ECVacuumJobsFailedCounter.Inc()
	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           job.GetJobId(),
		JobType:         job.GetJobType(),
		State:           plugin_pb.JobState_JOB_STATE_FAILED,
		ProgressPercent: 100,
		Stage:           "failed",
		Message:         err.Error(),
		Activities:      []*plugin_pb.ActivityEvent{pluginworker.BuildExecutorActivity("failed", err.Error())},
	})
	return err
}
