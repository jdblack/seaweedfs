package ec_vacuum

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// buildProposals converts candidates into plugin job proposals, honoring the
// detection result cap. hasMore reports that the cap truncated the list.
func buildProposals(candidates []ecvacuum.Candidate, maxResults int) ([]*plugin_pb.JobProposal, bool) {
	hasMore := false
	if maxResults > 0 && len(candidates) > maxResults {
		candidates = candidates[:maxResults]
		hasMore = true
	}

	now := time.Now()
	proposals := make([]*plugin_pb.JobProposal, 0, len(candidates))
	for _, c := range candidates {
		proposals = append(proposals, &plugin_pb.JobProposal{
			ProposalId: c.DedupeKey(),
			DedupeKey:  c.DedupeKey(),
			JobType:    jobType,
			Priority:   jobPriority(c.DeletedRatio()),
			Summary:    fmt.Sprintf("Vacuum EC volume %d (%.0f%% deleted)", c.VolumeID, c.DeletedRatio()*100),
			Detail:     describeCandidate(c),
			NotBefore:  timestamppb.New(now),
			Parameters: map[string]*plugin_pb.ConfigValue{
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
				"volume_id":  fmt.Sprintf("%d", c.VolumeID),
				"collection": c.Collection,
			},
		})
	}
	return proposals, hasMore
}

// jobPriority bumps a heavily-deleted volume ahead of a marginal one.
func jobPriority(ratio float64) plugin_pb.JobPriority {
	if ratio >= 0.6 {
		return plugin_pb.JobPriority_JOB_PRIORITY_HIGH
	}
	return plugin_pb.JobPriority_JOB_PRIORITY_NORMAL
}

func describeCandidate(c ecvacuum.Candidate) string {
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
