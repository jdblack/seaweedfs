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
	EncodeTsNs   int64
	// PresentShards holds the shard ids present for the newest encode
	// generation, used both for the health gate and to plan the collect.
	PresentShards erasure_coding.ShardBits
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
// least threshold and which still has at least dataShards shards present (below
// that the volume cannot be decoded, and vacuum must never touch it).
func EnumerateGarbageEcVolumes(topo *master_pb.TopologyInfo, collectionFilter string, threshold float64) ([]Candidate, error) {
	if topo == nil {
		return nil, fmt.Errorf("topology is nil")
	}

	matcher, err := wildcard.CompileCollectionMatcher(collectionFilter)
	if err != nil {
		return nil, fmt.Errorf("compile collection filter %q: %w", collectionFilter, err)
	}

	type groupKey struct {
		volumeID   uint32
		collection string
		diskType   string
	}

	// Collect every EC shard entry grouped by (volume, collection, disk type),
	// so per-node delete counts can be aggregated and the newest generation
	// selected before judging health.
	groups := make(map[groupKey][]*master_pb.VolumeEcShardInformationMessage)
	order := make([]groupKey, 0)
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
						key := groupKey{volumeID: eci.GetId(), collection: eci.GetCollection(), diskType: diskType}
						if _, ok := groups[key]; !ok {
							order = append(order, key)
						}
						groups[key] = append(groups[key], eci)
					}
				}
			}
		}
	}

	candidates := make([]Candidate, 0, len(order))
	for _, key := range order {
		if c, ok := candidateForGroup(key.volumeID, key.collection, key.diskType, groups[key], threshold); ok {
			candidates = append(candidates, c)
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

// candidateForGroup aggregates one (volume, collection, disk type) group's shard
// entries into a candidate, selecting only the newest encode generation, and
// reports whether it qualifies for vacuum.
func candidateForGroup(volumeID uint32, collection, diskType string, entries []*master_pb.VolumeEcShardInformationMessage, threshold float64) (Candidate, bool) {
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
		EncodeTsNs: newest,
	}
	for _, eci := range entries {
		if eci.GetEncodeTsNs() != newest {
			continue
		}
		if fc := eci.GetFileCount(); fc > c.FileCount {
			c.FileCount = fc
		}
		c.DeleteCount += eci.GetDeleteCount()
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
	if c.DeletedRatio() < threshold {
		stats.ECVacuumJobsSkippedCounter.WithLabelValues("below_threshold").Inc()
		return c, false
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
func shardHolders(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) map[uint32]string {
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
							if _, ok := holders[uint32(shardID)]; !ok {
								holders[uint32(shardID)] = address
							}
						}
					}
				}
			}
		}
	}
	return holders
}
