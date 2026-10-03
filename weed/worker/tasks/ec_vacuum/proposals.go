package ec_vacuum

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// buildProposals converts candidates into job proposals of one mode. The caller
// concatenates the modes and applies the result cap, so one budget covers both.
func buildProposals(candidates []ecvacuum.Candidate, mode string, sizeLimitMb uint64) []*plugin_pb.JobProposal {
	now := time.Now()
	proposals := make([]*plugin_pb.JobProposal, 0, len(candidates))
	for _, c := range candidates {
		key := proposalKey(c, mode)
		proposals = append(proposals, &plugin_pb.JobProposal{
			ProposalId: key,
			DedupeKey:  key,
			JobType:    jobType,
			Priority:   jobPriority(c, mode),
			Summary:    describeSummary(c, mode, sizeLimitMb),
			Detail:     describeCandidate(c, mode, sizeLimitMb),
			NotBefore:  timestamppb.New(now),
			Parameters: map[string]*plugin_pb.ConfigValue{
				fieldMode: {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: mode},
				},
				"volume_id": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(c.VolumeID)},
				},
				"collection": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: c.Collection},
				},
				"disk_type": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: c.DiskType},
				},
			},
			Labels: map[string]string{
				"task_type":  jobType,
				"action":     mode,
				"volume_id":  fmt.Sprintf("%d", c.VolumeID),
				"collection": c.Collection,
			},
		})
	}
	return proposals
}

// proposalKey identifies a proposal for the admin's dedupe. The action is part of
// the key: Candidate.DedupeKey alone is (job type, volume, collection, disk type),
// so a decode and a vacuum of one volume would collide and the admin would drop the
// second as a duplicate of the first (filterProposalsWithActiveJobs).
func proposalKey(c ecvacuum.Candidate, mode string) string {
	return mode + ":" + c.DedupeKey()
}

// jobPriority bumps a heavily-deleted volume ahead of a marginal one, and an
// emptied volume — the one whose whole footprint is freed — ahead of a merely
// undersized one.
func jobPriority(c ecvacuum.Candidate, mode string) plugin_pb.JobPriority {
	if mode == modeDecode {
		if c.LiveBytes == 0 {
			return plugin_pb.JobPriority_JOB_PRIORITY_HIGH
		}
		return plugin_pb.JobPriority_JOB_PRIORITY_NORMAL
	}
	if c.DeletedRatio() >= 0.6 {
		return plugin_pb.JobPriority_JOB_PRIORITY_HIGH
	}
	return plugin_pb.JobPriority_JOB_PRIORITY_NORMAL
}

// decodeFullnessPercent is a candidate's live data as a percentage of the master's
// volume size limit — the number the decode threshold is compared against.
func decodeFullnessPercent(c ecvacuum.Candidate, sizeLimitMb uint64) float64 {
	if sizeLimitMb == 0 {
		return 0
	}
	return float64(c.LiveBytes) / float64(uint64(sizeLimitMb)<<20) * 100
}

func describeSummary(c ecvacuum.Candidate, mode string, sizeLimitMb uint64) string {
	if mode == modeDecode {
		return fmt.Sprintf("Decode EC volume %d (%.0f%% full)", c.VolumeID, decodeFullnessPercent(c, sizeLimitMb))
	}
	return fmt.Sprintf("Vacuum EC volume %d (%.0f%% deleted)", c.VolumeID, c.DeletedRatio()*100)
}

func describeCandidate(c ecvacuum.Candidate, mode string, sizeLimitMb uint64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "volume=%d", c.VolumeID)
	if c.Collection != "" {
		fmt.Fprintf(&b, " collection=%s", c.Collection)
	}
	if c.DiskType != "" {
		fmt.Fprintf(&b, " disk_type=%s", c.DiskType)
	}
	fmt.Fprintf(&b, " deleted=%d/%d (%.1f%%) shards=%d/%d",
		c.DeleteCount, c.FileCount, c.DeletedRatio()*100, c.PresentShardCount(), c.DataShards+c.ParityShards)
	if mode == modeDecode {
		fmt.Fprintf(&b, " live=%.1fMB of %.1fMB encoded fullness=%.1f%%",
			float64(c.LiveBytes)/(1024*1024), float64(c.DataBytes)/(1024*1024), decodeFullnessPercent(c, sizeLimitMb))
	}
	return b.String()
}

// masterAddresses extracts the master gRPC addresses from a detection/execution
// cluster context.
func masterAddresses(cc *plugin_pb.ClusterContext) []string {
	if cc == nil {
		return nil
	}
	return cc.GetMasterGrpcAddresses()
}
