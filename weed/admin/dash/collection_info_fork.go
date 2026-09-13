package dash

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// Fork-local addition (not present in upstream SeaweedFS). Kept in its own file
// so upstream edits to collection_management.go do not conflict with it.

// collectCollectionInfo aggregates per-collection volume counts, EC volume
// counts, disk types and data centers from a topology listing. The logical size
// and chunk count are filled in from collectCollectionStats, which counts EC
// volumes and nets out tombstones, replicated copies and EC parity shards, so a
// collection that has been erasure coded does not read as a fraction of its
// real size.
func collectCollectionInfo(topologyInfo *master_pb.TopologyInfo) (map[string]*CollectionInfo, int, int) {
	collectionMap := make(map[string]*CollectionInfo)
	// An EC volume's shards are spread across disks and nodes, so count the
	// volume once cluster-wide: a volume split over three nodes is one EC
	// volume, not three. Deduping per disk (the previous behaviour) counted
	// each shard-holding disk as another EC volume. Every shard holder still
	// contributes its disk type and data center, so the collection's reported
	// footprint stays complete.
	ecVolumesSeen := make(map[uint32]*CollectionInfo)
	totalVolumes, totalEcVolumes := 0, 0

	// getOrCreate returns the collection, defaulting its data center to the one
	// that first reports it and widening to "multi" when it spans several.
	getOrCreate := func(name, dataCenter string) *CollectionInfo {
		if collection, exists := collectionMap[name]; exists {
			if collection.DataCenter != dataCenter && collection.DataCenter != "multi" {
				collection.DataCenter = "multi"
			}
			return collection
		}
		collection := &CollectionInfo{Name: name, DataCenter: dataCenter}
		collectionMap[name] = collection
		return collection
	}

	addDiskType := func(collection *CollectionInfo, diskType string) {
		if diskType == "" {
			diskType = "hdd"
		}
		for _, existing := range collection.DiskTypes {
			if existing == diskType {
				return
			}
		}
		collection.DiskTypes = append(collection.DiskTypes, diskType)
	}

	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, diskInfo := range node.DiskInfos {
					for _, volInfo := range diskInfo.VolumeInfos {
						name := volInfo.Collection
						if name == "" {
							name = "default"
						}
						collection := getOrCreate(name, dc.Id)
						collection.VolumeCount++
						addDiskType(collection, volInfo.DiskType)
						totalVolumes++
					}
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						name := ecShardInfo.Collection
						if name == "" {
							name = "default"
						}
						collection := getOrCreate(name, dc.Id)
						addDiskType(collection, diskInfo.Type)
						if _, seen := ecVolumesSeen[ecShardInfo.Id]; seen {
							continue
						}
						ecVolumesSeen[ecShardInfo.Id] = collection
						collection.EcVolumeCount++
						totalEcVolumes++
					}
				}
			}
		}
	}

	// Sizes and chunk counts come from the shared aggregation: EC volumes are
	// included, deleted bytes are netted out, and each byte/chunk is counted
	// once no matter how many replicas or shard holders report it.
	for name, stats := range collectCollectionStats(topologyInfo) {
		if collection, exists := collectionMap[name]; exists {
			collection.TotalSize = stats.LogicalSize
			collection.ChunkCount = stats.FileCount
		}
	}

	return collectionMap, totalVolumes, totalEcVolumes
}
