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

	// Deterministic roster order, shared with the bookmark lookup below: a
	// rotation can only resume correctly if both use the same comparator.
	sort.Slice(candidates, func(i, j int) bool {
		return lessCandidate(candidates[i], candidates[j])
	})

	return candidates, nil
}

// lessCandidate orders the roster: volume id first, then collection, then disk
// type. bookmarkPosition uses the same ordering, so a resumed cycle lands
// exactly where the previous one stopped.
func lessCandidate(a, b ecVolumeCandidate) bool {
	if a.VolumeID != b.VolumeID {
		return a.VolumeID < b.VolumeID
	}
	if a.Collection != b.Collection {
		return a.Collection < b.Collection
	}
	return a.DiskType < b.DiskType
}

// bookmarkPosition returns the index of the first candidate that sorts strictly
// after the bookmark — the bookmark names the last volume the previous cycle
// proposed, so the next cycle must resume past it. The result is 0 (wrap to the
// front) when there is no bookmark, or when it lies at or past the end of a
// shrunken roster. It never fails: a stale cursor costs one uneven cycle.
func bookmarkPosition(candidates []ecVolumeCandidate, bm *scanBookmark) int {
	if bm == nil || len(candidates) == 0 {
		return 0
	}
	target := ecVolumeCandidate{VolumeID: bm.VolumeID, Collection: bm.Collection, DiskType: bm.DiskType}
	idx := sort.Search(len(candidates), func(i int) bool {
		return lessCandidate(target, candidates[i])
	})
	if idx >= len(candidates) {
		return 0
	}
	return idx
}

// selectWindow returns the next `slice` candidates starting at start, wrapping
// to the front of the roster. Wrapping is what keeps every cycle the same size
// when the roster does not divide evenly by the slice: the first
// (slice - remainder) volumes are proposed once more at each pass seam. That is
// the price of a uniform batch instead of a short one, and it is bounded by
// (slice-1)/N per pass. A non-positive slice means every candidate.
func selectWindow(candidates []ecVolumeCandidate, start, slice int) []ecVolumeCandidate {
	if len(candidates) == 0 {
		return nil
	}
	if slice <= 0 || slice > len(candidates) {
		slice = len(candidates)
	}
	if start < 0 || start >= len(candidates) {
		start = 0
	}
	window := make([]ecVolumeCandidate, 0, slice)
	for i := 0; i < slice; i++ {
		window = append(window, candidates[(start+i)%len(candidates)])
	}
	return window
}

// scanSliceSize derives the per-cycle slice and the number of cycles one full
// pass takes, from the target pass length and the admin's detection interval.
//
// Deriving the slice rather than configuring it is what keeps the pass length
// from drifting as the roster grows: 822 volumes with a weekly target and a
// 4-hour interval is 42 cycles of 20; 2000 volumes is 42 cycles of 48. The
// admin's maxJobsPerDetection stays what upstream intends it to be — a cap that
// binds only once the roster has outgrown the configured interval, and which
// then stretches the pass instead of truncating coverage.
func scanSliceSize(candidateCount, scanIntervalMinutes, detectionIntervalMinutes, maxResults int) (slice, cycles int) {
	cycles = 1
	if scanIntervalMinutes > 0 && detectionIntervalMinutes > 0 {
		cycles = (scanIntervalMinutes + detectionIntervalMinutes - 1) / detectionIntervalMinutes
	}
	if cycles < 1 {
		cycles = 1
	}
	if candidateCount <= 0 {
		return 0, cycles
	}
	slice = (candidateCount + cycles - 1) / cycles
	if maxResults > 0 && slice > maxResults {
		slice = maxResults
	}
	if slice < 1 {
		slice = 1
	}
	return slice, cycles
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

// buildProposals converts one rotation window into plugin job proposals: the
// next `slice` candidates from start, wrapping to the front of the roster.
// hasMore reports that a full pass needs more than this one cycle.
func buildProposals(candidates []ecVolumeCandidate, slice, start int) ([]*plugin_pb.JobProposal, bool) {
	window := selectWindow(candidates, start, slice)
	hasMore := len(candidates) > len(window)

	now := time.Now()
	proposals := make([]*plugin_pb.JobProposal, 0, len(window))
	for _, c := range window {
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
