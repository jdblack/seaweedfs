package ec_bitrot_scan

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/grpc"
)

// scrubVolumeOnHolder runs a read-only scrub (CHECKSUM for shard bytes, INDEX
// for the .ecx index) for one volume on one shard holder. These are the same
// verifications behind "ec.scrub -mode checksum" and "-mode index".
func scrubVolumeOnHolder(ctx context.Context, dialOpt grpc.DialOption, holder string, volumeID uint32, mode volume_server_pb.VolumeScrubMode) (*volume_server_pb.ScrubEcVolumeResponse, error) {
	var resp *volume_server_pb.ScrubEcVolumeResponse
	err := operation.WithVolumeServerClient(false, pb.ServerAddress(holder), dialOpt,
		func(client volume_server_pb.VolumeServerClient) error {
			var callErr error
			resp, callErr = client.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
				Mode:      mode,
				VolumeIds: []uint32{volumeID},
			})
			return callErr
		})
	return resp, err
}

// decodeJobParams extracts the scan target from a job proposal's parameters.
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

// resolveParity returns the volume's parity shard count from the topology,
// falling back to the build default when the topology does not record it.
func resolveParity(topo *master_pb.TopologyInfo, volumeID uint32, collection string) int {
	_, parity := shardRatioForVolume(topo, volumeID, collection)
	if parity <= 0 {
		return erasure_coding.ParityShardsCount
	}
	return parity
}

// dedupeShardRefs removes duplicate (holder, shard) references and returns a
// stable order. A shard reported by more than one holder in the same run (e.g.
// an over-replicated volume) is quarantined once per copy.
func dedupeShardRefs(refs []ec.ShardRef) []ec.ShardRef {
	seen := make(map[ec.ShardRef]struct{}, len(refs))
	out := make([]ec.ShardRef, 0, len(refs))
	for _, ref := range refs {
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeAddress != out[j].NodeAddress {
			return out[i].NodeAddress < out[j].NodeAddress
		}
		return out[i].ShardID < out[j].ShardID
	})
	return out
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

// sendCompleted reports a successful/unsuccessful run with a single completed
// activity.
func sendCompleted(sender pluginworker.ExecutionSender, job *plugin_pb.JobSpec, success bool, summary string, outputs map[string]*plugin_pb.ConfigValue) error {
	return emitCompleted(sender, job, success, summary, "", outputs,
		[]*plugin_pb.ActivityEvent{pluginworker.BuildExecutorActivity("completed", summary)})
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

// summarizeRun renders a one-line run summary.
func summarizeRun(volumeID uint32, filesVerified int64, detected, quarantined, integrityErrors int) string {
	summary := fmt.Sprintf("EC volume %d: verified %d file(s); %d broken shard(s)", volumeID, filesVerified, detected)
	if quarantined > 0 {
		summary += fmt.Sprintf(", quarantined %d for rebuild", quarantined)
	}
	if integrityErrors > 0 {
		summary += fmt.Sprintf(", %d integrity error(s)", integrityErrors)
	}
	return summary
}
