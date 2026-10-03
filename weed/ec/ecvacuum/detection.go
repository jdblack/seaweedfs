package ecvacuum

import (
	"fmt"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

// jobType mirrors the plugin job type this core serves; it only shapes the
// candidate dedupe key (so the worker and shell agree on one key per volume).
const jobType = "ec_vacuum"

// Candidate is one vacuum target: a distinct (volume_id, collection,
// disk_type) whose deleted-needle ratio has crossed the configured threshold and
// which still has enough shards present to be decoded.
type Candidate struct {
	VolumeID     uint32
	Collection   string
	DiskType     string
	FileCount    uint64
	DeleteCount  uint64
	DataShards   int
	ParityShards int
	// PresentShards holds the shard ids present for the newest encode
	// generation, used both for the health gate and to plan the collect.
	PresentShards erasure_coding.ShardBits
	// DataBytes is the volume's encode-time data size: one shard's length times the
	// data-shard count. Zero when no holder reported a shard size.
	DataBytes int64
	// SizeReported is false when no holder reported a shard size. `shard_sizes` is
	// an optional heartbeat field, so an older volume server reports nothing — and
	// then DataBytes/LiveBytes are 0 for a reason that has nothing to do with the
	// volume being empty. Callers that judge size must require this.
	SizeReported bool
	// LiveBytes is DataBytes with the holders' delete journals folded in — what the
	// volume holds now, since an EC volume's shards do not shrink when needles are
	// deleted. Exact at both extremes (nothing deleted, everything deleted) and an
	// estimate in between, as the heartbeat carries counts, not sizes. Only
	// meaningful when SizeReported is true.
	LiveBytes int64
}

// DeletedRatio is the fraction of the volume's recorded needles that have been
// deleted since encode. fileCount is the sealed .ecx record count (identical on
// every shard holder); deleteCount is the sum of the holders' .ecj journal
// lengths (a delete lands on exactly one holder).
func (c Candidate) DeletedRatio() float64 {
	if c.FileCount == 0 {
		return 0
	}
	return float64(c.DeleteCount) / float64(c.FileCount)
}

// PresentShardCount is the number of shards present in the newest generation.
func (c Candidate) PresentShardCount() int {
	return c.PresentShards.Count()
}

// DedupeKey uniquely identifies a candidate across the cluster so the scheduler
// does not stack duplicate vacuums of the same target.
func (c Candidate) DedupeKey() string {
	return fmt.Sprintf("%s:%d:%s:%s", jobType, c.VolumeID, c.Collection, c.DiskType)
}

// EnumerateGarbageEcVolumes walks a master topology and returns one candidate per
// distinct (volume_id, collection, disk_type) whose deleted-needle ratio is at
// least threshold, which still has at least dataShards shards present (below
// that the volume cannot be decoded, and vacuum must never touch it), and which
// still holds live needles (a fully-deleted volume can only be purged, which is
// the decode route's job).
func EnumerateGarbageEcVolumes(topo *master_pb.TopologyInfo, collectionFilter string, threshold float64) ([]Candidate, error) {
	if topo == nil {
		return nil, fmt.Errorf("topology is nil")
	}

	candidates, err := enumerateValidEcCandidates(topo, collectionFilter)
	if err != nil {
		return nil, err
	}

	// The deleted-ratio gate lives here rather than in the aggregate builder, so
	// the decode enumeration can reuse the same aggregate without it.
	qualified := make([]Candidate, 0, len(candidates))
	for _, c := range candidates {
		// A volume with no live needles has nothing to compact: the executor can
		// only answer "no live entries" — after collecting every shard of the
		// volume onto the worker. Reclaiming it is the decode route's job
		// (EnumerateDecodeCandidates), which purges it, so leaving it out here
		// keeps the detector from proposing a whole shard set every cycle for no
		// result. FileCount 0 means no needles were ever recorded, which the
		// ratio gate below already drops.
		if c.FileCount > 0 && c.DeleteCount >= c.FileCount {
			stats.ECVacuumJobsSkippedCounter.WithLabelValues("no_live_entries").Inc()
			continue
		}
		if c.DeletedRatio() < threshold {
			stats.ECVacuumJobsSkippedCounter.WithLabelValues("below_threshold").Inc()
			continue
		}
		qualified = append(qualified, c)
	}
	sortCandidates(qualified)
	return qualified, nil
}

// EnumerateDecodeCandidates walks a master topology and returns one candidate per
// distinct (volume, collection, disk type) EC volume whose live data is at most
// fullness of the master's volume size limit — too small to be worth keeping
// erasure-coded. It shares the vacuum enumeration's walk; the deleted-ratio
// threshold does not apply, because a volume can be nearly empty of live data
// without carrying much garbage.
//
// An empty volume reports 0 and so qualifies at any fullness above 0; DoEcDecode
// purges it without generating one. A wrong estimate can therefore only nominate a
// volume, since the decode re-checks emptiness against the authoritative index.
func EnumerateDecodeCandidates(topo *master_pb.TopologyInfo, collectionFilter string, fullness float64, sizeLimitMb uint64) ([]Candidate, error) {
	if topo == nil {
		return nil, fmt.Errorf("topology is nil")
	}
	if fullness <= 0 {
		return nil, nil
	}
	if sizeLimitMb == 0 {
		return nil, fmt.Errorf("volume size limit is unknown, cannot judge EC fullness")
	}

	candidates, err := enumerateValidEcCandidates(topo, collectionFilter)
	if err != nil {
		return nil, err
	}

	limitBytes := int64(float64(uint64(sizeLimitMb)<<20) * fullness)
	small := make([]Candidate, 0, len(candidates))
	for _, c := range candidates {
		// No holder reported a shard size: LiveBytes is 0 for lack of information,
		// not lack of needles. Counting the skip keeps a cluster whose volume
		// servers do not send shard_sizes from looking like one with nothing to
		// decode.
		if !c.SizeReported {
			stats.ECVacuumJobsSkippedCounter.WithLabelValues("unsized").Inc()
			continue
		}
		if c.LiveBytes <= limitBytes {
			small = append(small, c)
		}
	}
	sortCandidates(small)
	return small, nil
}

// ecShardGroupKey identifies one EC volume's shard entries on one disk type.
type ecShardGroupKey struct {
	volumeID   uint32
	collection string
	diskType   string
}

// groupEcShardEntries collects every EC shard entry in topo, grouped by
// (volume, collection, disk type), so per-holder delete counts can be summed and
// the newest encode generation selected before judging a volume. order is the
// traversal order, kept so paging stays deterministic across cycles.
func groupEcShardEntries(topo *master_pb.TopologyInfo, matcher *wildcard.CollectionMatcher) (map[ecShardGroupKey][]*master_pb.VolumeEcShardInformationMessage, []ecShardGroupKey) {
	groups := make(map[ecShardGroupKey][]*master_pb.VolumeEcShardInformationMessage)
	order := make([]ecShardGroupKey, 0)
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				for diskType, diskInfo := range node.GetDiskInfos() {
					if diskInfo == nil {
						continue
					}
					for _, eci := range diskInfo.GetEcShardInfos() {
						if eci == nil || !matcher.Matches(eci.GetCollection()) {
							continue
						}
						key := ecShardGroupKey{volumeID: eci.GetId(), collection: eci.GetCollection(), diskType: diskType}
						if _, ok := groups[key]; !ok {
							order = append(order, key)
						}
						groups[key] = append(groups[key], eci)
					}
				}
			}
		}
	}
	return groups, order
}

// enumerateValidEcCandidates returns every EC volume in topo that could be acted
// on at all: a valid data/parity ratio and every data shard present. It applies
// no policy gate, so the vacuum and decode enumerations share one walk and one
// set of skip metrics.
func enumerateValidEcCandidates(topo *master_pb.TopologyInfo, collectionFilter string) ([]Candidate, error) {
	matcher, err := wildcard.CompileCollectionMatcher(collectionFilter)
	if err != nil {
		return nil, fmt.Errorf("compile collection filter %q: %w", collectionFilter, err)
	}
	groups, order := groupEcShardEntries(topo, matcher)
	candidates := make([]Candidate, 0, len(order))
	for _, key := range order {
		if c, ok := candidateForGroup(key.volumeID, key.collection, key.diskType, groups[key]); ok {
			candidates = append(candidates, c)
		}
	}
	return candidates, nil
}

// sortCandidates puts candidates in a stable order so paging is deterministic
// across cycles.
func sortCandidates(candidates []Candidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].VolumeID != candidates[j].VolumeID {
			return candidates[i].VolumeID < candidates[j].VolumeID
		}
		if candidates[i].Collection != candidates[j].Collection {
			return candidates[i].Collection < candidates[j].Collection
		}
		return candidates[i].DiskType < candidates[j].DiskType
	})
}

// candidateForGroup aggregates one (volume, collection, disk type) group's shard
// entries into a candidate, selecting only the newest encode generation. It
// applies only the validity gates — a usable ratio and every data shard present;
// policy gates (deleted ratio, live fullness) belong to the enumerators.
func candidateForGroup(volumeID uint32, collection, diskType string, entries []*master_pb.VolumeEcShardInformationMessage) (Candidate, bool) {
	newest := int64(0)
	for _, eci := range entries {
		if eci.GetEncodeTsNs() > newest {
			newest = eci.GetEncodeTsNs()
		}
	}

	c := Candidate{
		VolumeID:   volumeID,
		Collection: collection,
		DiskType:   diskType,
	}
	var shardSize int64
	for _, eci := range entries {
		if eci.GetEncodeTsNs() != newest {
			continue
		}
		if fc := eci.GetFileCount(); fc > c.FileCount {
			c.FileCount = fc
		}
		// A delete lands on one holder, so the count is the sum, not the max.
		c.DeleteCount += eci.GetDeleteCount()
		// Every shard is the same length: one size gives the volume's data size.
		for _, size := range eci.GetShardSizes() {
			if size > shardSize {
				shardSize = size
			}
		}
		c.PresentShards |= erasure_coding.ShardBits(eci.GetEcIndexBits())
		if ds := int(eci.GetDataShards()); ds > c.DataShards {
			c.DataShards = ds
		}
		if ps := int(eci.GetParityShards()); ps > c.ParityShards {
			c.ParityShards = ps
		}
	}

	if c.DataShards <= 0 {
		c.DataShards = erasure_coding.DataShardsCount
	}
	if c.ParityShards <= 0 {
		c.ParityShards = erasure_coding.ParityShardsCount
	}
	if !erasure_coding.ValidEcShardCounts(uint32(c.DataShards), uint32(c.ParityShards)) {
		stats.ECVacuumJobsSkippedCounter.WithLabelValues("invalid_ratio").Inc()
		return c, false
	}
	// WriteDatFile reconstructs a .dat from the data shards alone, so every data
	// shard (ids 0..dataShards-1) must be present. A volume short of one is a
	// repair problem for ec.rebuild, not a vacuum candidate.
	if !HasAllDataShards(c.PresentShards, c.DataShards) {
		stats.ECVacuumJobsSkippedCounter.WithLabelValues("missing_data_shard").Inc()
		return c, false
	}

	// SizeReported separates "no shard size reported" from "the volume is empty":
	// both leave the byte counts at zero.
	c.SizeReported = shardSize > 0
	c.DataBytes = shardSize * int64(c.DataShards)
	switch {
	case c.FileCount == 0 || c.DeleteCount >= c.FileCount:
		c.LiveBytes = 0
	case c.DeleteCount == 0:
		c.LiveBytes = c.DataBytes
	default:
		c.LiveBytes = c.DataBytes * int64(c.FileCount-c.DeleteCount) / int64(c.FileCount)
	}
	return c, true
}

// FilterCandidatesByDiskType keeps only candidates on the requested disk type.
func FilterCandidatesByDiskType(candidates []Candidate, diskType string) []Candidate {
	filtered := candidates[:0:0]
	for _, c := range candidates {
		if c.DiskType == diskType {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// shardHolders returns the gRPC address holding each present shard of the
// volume's newest encode generation, keyed by shard id. It re-reads the topology
// at execution time so a shard that moved between detection and execution is
// still found, and so a shard that vanished causes the plan to be recomputed
// rather than trusted.
// duplicateShardIDs returns the newest generation's shard ids that more than one
// node reports. A shard belongs on exactly one node, so a duplicate is the
// signature of an interrupted ec.balance — which copies before it deletes the
// source, so a crash mid-move leaves a surplus copy by design.
//
// This matters because shardHolders keeps only the first holder it sees for an id:
// Collect would never read the second copy's .ecj and Redistribute would never clear
// it, so the deletes it holds would vanish from the compacted generation — a
// silently reverted delete. Refuse, as the mixed-generation check above does.
func duplicateShardIDs(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) []uint32 {
	newest := newestEncodeTsNs(topo, volumeID, collection, diskType)
	seen := make(map[uint32]string)
	dups := make(map[uint32]struct{})
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				address := string(pb.NewServerAddressFromDataNode(node))
				for dt, diskInfo := range node.GetDiskInfos() {
					if diskInfo == nil || (diskType != "" && dt != diskType) {
						continue
					}
					for _, eci := range diskInfo.GetEcShardInfos() {
						if eci.GetId() != volumeID || eci.GetCollection() != collection || eci.GetEncodeTsNs() != newest {
							continue
						}
						for shardID := range erasure_coding.ShardBits(eci.GetEcIndexBits()).All() {
							id := uint32(shardID)
							if first, ok := seen[id]; ok {
								if first != address {
									dups[id] = struct{}{}
								}
								continue
							}
							seen[id] = address
						}
					}
				}
			}
		}
	}
	if len(dups) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(dups))
	for id := range dups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// shardHolders returns, for the newest encode generation, each present shard id's
// holder address and the disk that shard sits on (as the volume server counts its
// own -dir list). The disk id is what lets Redistribute push a compacted shard
// back to where it came from instead of letting the server auto-select a disk.
func shardHolders(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) (map[uint32]string, map[uint32]uint32) {
	newest := int64(0)
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				for dt, diskInfo := range node.GetDiskInfos() {
					if diskInfo == nil || (diskType != "" && dt != diskType) {
						continue
					}
					for _, eci := range diskInfo.GetEcShardInfos() {
						if eci.GetId() != volumeID || eci.GetCollection() != collection {
							continue
						}
						if eci.GetEncodeTsNs() > newest {
							newest = eci.GetEncodeTsNs()
						}
					}
				}
			}
		}
	}

	holders := make(map[uint32]string)
	diskIDs := make(map[uint32]uint32)
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				address := string(pb.NewServerAddressFromDataNode(node))
				for dt, diskInfo := range node.GetDiskInfos() {
					if diskInfo == nil || (diskType != "" && dt != diskType) {
						continue
					}
					for _, eci := range diskInfo.GetEcShardInfos() {
						if eci.GetId() != volumeID || eci.GetCollection() != collection || eci.GetEncodeTsNs() != newest {
							continue
						}
						for shardID := range erasure_coding.ShardBits(eci.GetEcIndexBits()).All() {
							id := uint32(shardID)
							if _, ok := holders[id]; !ok {
								holders[id] = address
								diskIDs[id] = eci.GetDiskId()
							}
						}
					}
				}
			}
		}
	}
	return holders, diskIDs
}
