package ec_bitrot_scan

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ecVolumeCandidate is one scrub target: a distinct (volume_id, collection,
// disk_type) tuple, matching the documented "one scrub candidate per distinct
// (volume ID, collection, disk type)".
type ecVolumeCandidate struct {
	VolumeID   uint32
	Collection string
	DiskType   string
}

// dedupeKey uniquely identifies a candidate across the cluster. The scheduler
// uses it to avoid stacking duplicate scans of the same target.
func (c ecVolumeCandidate) dedupeKey() string {
	return fmt.Sprintf("%s:%d:%s:%s", jobType, c.VolumeID, c.Collection, c.DiskType)
}

// enumerateEcVolumes walks a master topology and returns one candidate per
// distinct (volume_id, collection, disk_type) that holds at least one EC shard,
// honoring an optional collection filter. It does not pre-judge shard health —
// the scrub itself finds corruption.
func enumerateEcVolumes(topo *master_pb.TopologyInfo, collectionFilter string) ([]ecVolumeCandidate, error) {
	if topo == nil {
		return nil, fmt.Errorf("topology is nil")
	}

	matcher, err := wildcard.CompileCollectionMatcher(collectionFilter)
	if err != nil {
		return nil, fmt.Errorf("compile collection filter %q: %w", collectionFilter, err)
	}

	seen := make(map[ecVolumeCandidate]struct{})
	var candidates []ecVolumeCandidate

	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				for diskType, diskInfo := range node.GetDiskInfos() {
					if diskInfo == nil {
						continue
					}
					for _, eci := range diskInfo.GetEcShardInfos() {
						if eci == nil {
							continue
						}
						if !matcher.Matches(eci.GetCollection()) {
							continue
						}
						c := ecVolumeCandidate{
							VolumeID:   eci.GetId(),
							Collection: eci.GetCollection(),
							DiskType:   diskType,
						}
						if _, ok := seen[c]; ok {
							continue
						}
						seen[c] = struct{}{}
						candidates = append(candidates, c)
					}
				}
			}
		}
	}

	// Stable order so paging is deterministic across cycles.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].VolumeID != candidates[j].VolumeID {
			return candidates[i].VolumeID < candidates[j].VolumeID
		}
		if candidates[i].Collection != candidates[j].Collection {
			return candidates[i].Collection < candidates[j].Collection
		}
		return candidates[i].DiskType < candidates[j].DiskType
	})

	return candidates, nil
}

// holdersForVolume returns the gRPC addresses of the volume servers that report
// at least one EC shard for the given volume + collection (optionally narrowed
// to a disk type). The master topology is the authority: the scrub and the
// quarantine both target these holders.
func holdersForVolume(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) []string {
	seen := make(map[string]struct{})
	var holders []string
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				address := string(pb.NewServerAddressFromDataNode(node))
				for dt, diskInfo := range node.GetDiskInfos() {
					if diskInfo == nil {
						continue
					}
					if diskType != "" && dt != diskType {
						continue
					}
					for _, eci := range diskInfo.GetEcShardInfos() {
						if eci.GetId() != volumeID || eci.GetCollection() != collection {
							continue
						}
						if _, ok := seen[address]; ok {
							continue
						}
						seen[address] = struct{}{}
						holders = append(holders, address)
					}
				}
			}
		}
	}
	sort.Strings(holders)
	return holders
}

// shardRatioForVolume returns the volume's (data, parity) shard counts as
// recorded in the topology, falling back to the build defaults when unset.
func shardRatioForVolume(topo *master_pb.TopologyInfo, volumeID uint32, collection string) (data, parity int) {
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				for _, diskInfo := range node.GetDiskInfos() {
					if diskInfo == nil {
						continue
					}
					for _, eci := range diskInfo.GetEcShardInfos() {
						if eci.GetId() == volumeID && eci.GetCollection() == collection {
							return int(eci.GetDataShards()), int(eci.GetParityShards())
						}
					}
				}
			}
		}
	}
	return 0, 0
}

// buildProposals converts candidates into plugin job proposals, honoring the
// detection result cap. hasMore reports that the cap truncated the list.
func buildProposals(candidates []ecVolumeCandidate, maxResults int) ([]*plugin_pb.JobProposal, bool) {
	hasMore := false
	if maxResults > 0 && len(candidates) > maxResults {
		candidates = candidates[:maxResults]
		hasMore = true
	}

	now := time.Now()
	proposals := make([]*plugin_pb.JobProposal, 0, len(candidates))
	for _, c := range candidates {
		proposals = append(proposals, &plugin_pb.JobProposal{
			ProposalId: c.dedupeKey(),
			DedupeKey:  c.dedupeKey(),
			JobType:    jobType,
			Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
			Summary:    fmt.Sprintf("Verify EC volume %d checksums", c.VolumeID),
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
		})
	}
	return proposals, hasMore
}

func describeCandidate(c ecVolumeCandidate) string {
	var b strings.Builder
	fmt.Fprintf(&b, "volume=%d", c.VolumeID)
	if c.Collection != "" {
		fmt.Fprintf(&b, " collection=%s", c.Collection)
	}
	if c.DiskType != "" {
		fmt.Fprintf(&b, " disk_type=%s", c.DiskType)
	}
	return b.String()
}
