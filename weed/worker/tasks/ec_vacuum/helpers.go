package ec_vacuum

import (
	"fmt"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// decodeJobParams extracts the vacuum target from a job proposal's parameters.
func decodeJobParams(job *plugin_pb.JobSpec) (volumeID uint32, collection, diskType string, err error) {
	params := job.GetParameters()
	vid := pluginworker.ReadInt64Config(params, "volume_id", -1)
	if vid < 0 || vid > math.MaxUint32 {
		return 0, "", "", fmt.Errorf("invalid volume_id %d", vid)
	}
	collection = pluginworker.ReadStringConfig(params, "collection", "")
	diskType = pluginworker.ReadStringConfig(params, "disk_type", "")
	return uint32(vid), collection, diskType, nil
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
