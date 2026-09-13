package dash

import (
	"context"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// GetClusterCollections retrieves cluster collections data
func (s *AdminServer) GetClusterCollections() (*ClusterCollectionsData, error) {
	var topologyInfo *master_pb.TopologyInfo

	// Get actual collection information from volume data
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := pb.CollectVolumeList(context.Background(), client, &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		topologyInfo = resp.TopologyInfo

		return nil
	})

	if err != nil {
		return nil, err
	}

	var collections []CollectionInfo
	var totalVolumes, totalEcVolumes int
	var totalChunks, totalSize int64
	if topologyInfo != nil {
		collectionMap, volumes, ecVolumes := collectCollectionInfo(topologyInfo)
		totalVolumes, totalEcVolumes = volumes, ecVolumes
		for _, collection := range collectionMap {
			collections = append(collections, *collection)
			totalChunks += collection.ChunkCount
			totalSize += collection.TotalSize
		}
	}

	// Sort collections alphabetically by name
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].Name < collections[j].Name
	})

	// If no collections found, show a message indicating no collections exist
	if len(collections) == 0 {
		// Return empty collections data instead of creating fake ones
		return &ClusterCollectionsData{
			Collections:      []CollectionInfo{},
			TotalCollections: 0,
			TotalVolumes:     0,
			TotalEcVolumes:   0,
			TotalChunks:      0,
			TotalSize:        0,
			LastUpdated:      time.Now(),
		}, nil
	}

	return &ClusterCollectionsData{
		Collections:      collections,
		TotalCollections: len(collections),
		TotalVolumes:     totalVolumes,
		TotalEcVolumes:   totalEcVolumes,
		TotalChunks:      totalChunks,
		TotalSize:        totalSize,
		LastUpdated:      time.Now(),
	}, nil
}

// GetCollectionDetails retrieves detailed information for a specific collection including volumes and EC volumes
func (s *AdminServer) GetCollectionDetails(collectionName string, page int, pageSize int, sortBy string, sortOrder string) (*CollectionDetailsData, error) {
	// Set defaults
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 1000 {
		pageSize = 25
	}
	if sortBy == "" {
		sortBy = "volume_id"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}

	var regularVolumes []VolumeWithTopology
	var ecVolumes []EcVolumeWithShards
	var totalSize int64
	dataCenters := make(map[string]bool)
	diskTypes := make(map[string]bool)

	// Get regular volumes for this collection
	regularVolumeData, err := s.GetClusterVolumes(1, 10000, "volume_id", "asc", collectionName) // Get all volumes
	if err != nil {
		return nil, err
	}

	regularVolumes = regularVolumeData.Volumes

	// Chunk counts come from the shared collection aggregation, which nets out
	// tombstones and counts a chunk once no matter how many volume replicas or
	// EC shard holders report it. Fail rather than render a zero that reads
	// like an empty collection.
	stats, err := s.getCollectionStats()
	if err != nil {
		return nil, err
	}
	totalChunks := stats[collectionName].FileCount
	// The logical size spans regular and EC volumes, netting out tombstones,
	// replicated copies and EC parity. GetClusterVolumes instead mixes raw
	// per-replica regular sizes with raw EC shard bytes, which reads as a
	// fraction of the collection once most of it has been erasure coded.
	totalSize = stats[collectionName].LogicalSize

	// Collect data centers and disk types from regular volumes
	for _, vol := range regularVolumes {
		dataCenters[vol.DataCenter] = true
		diskTypes[vol.DiskType] = true
	}

	// Get EC volumes for this collection
	ecVolumeData, err := s.GetClusterEcVolumes(1, 10000, "volume_id", "asc", collectionName) // Get all EC volumes
	if err != nil {
		return nil, err
	}

	ecVolumes = ecVolumeData.EcVolumes

	// Collect data centers from EC volumes
	for _, ecVol := range ecVolumes {
		for _, dc := range ecVol.DataCenters {
			dataCenters[dc] = true
		}
	}

	// Combine all volumes for sorting and pagination
	type VolumeForSorting struct {
		Type          string // "regular" or "ec"
		RegularVolume *VolumeWithTopology
		EcVolume      *EcVolumeWithShards
	}

	var allVolumes []VolumeForSorting
	for i := range regularVolumes {
		allVolumes = append(allVolumes, VolumeForSorting{
			Type:          "regular",
			RegularVolume: &regularVolumes[i],
		})
	}
	for i := range ecVolumes {
		allVolumes = append(allVolumes, VolumeForSorting{
			Type:     "ec",
			EcVolume: &ecVolumes[i],
		})
	}

	// Sort all volumes
	sort.Slice(allVolumes, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "volume_id":
			var idI, idJ uint32
			if allVolumes[i].Type == "regular" {
				idI = allVolumes[i].RegularVolume.Id
			} else {
				idI = allVolumes[i].EcVolume.VolumeID
			}
			if allVolumes[j].Type == "regular" {
				idJ = allVolumes[j].RegularVolume.Id
			} else {
				idJ = allVolumes[j].EcVolume.VolumeID
			}
			less = idI < idJ
		case "type":
			// Sort by type first (regular before ec), then by volume ID
			if allVolumes[i].Type == allVolumes[j].Type {
				var idI, idJ uint32
				if allVolumes[i].Type == "regular" {
					idI = allVolumes[i].RegularVolume.Id
				} else {
					idI = allVolumes[i].EcVolume.VolumeID
				}
				if allVolumes[j].Type == "regular" {
					idJ = allVolumes[j].RegularVolume.Id
				} else {
					idJ = allVolumes[j].EcVolume.VolumeID
				}
				less = idI < idJ
			} else {
				less = allVolumes[i].Type < allVolumes[j].Type // "ec" < "regular"
			}
		default:
			// Default to volume ID sort
			var idI, idJ uint32
			if allVolumes[i].Type == "regular" {
				idI = allVolumes[i].RegularVolume.Id
			} else {
				idI = allVolumes[i].EcVolume.VolumeID
			}
			if allVolumes[j].Type == "regular" {
				idJ = allVolumes[j].RegularVolume.Id
			} else {
				idJ = allVolumes[j].EcVolume.VolumeID
			}
			less = idI < idJ
		}

		if sortOrder == "desc" {
			return !less
		}
		return less
	})

	// Apply pagination
	totalVolumesAndEc := len(allVolumes)
	totalPages := (totalVolumesAndEc + pageSize - 1) / pageSize
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if endIndex > totalVolumesAndEc {
		endIndex = totalVolumesAndEc
	}

	if startIndex >= totalVolumesAndEc {
		startIndex = 0
		endIndex = 0
	}

	// Extract paginated results
	var paginatedRegularVolumes []VolumeWithTopology
	var paginatedEcVolumes []EcVolumeWithShards

	for i := startIndex; i < endIndex; i++ {
		if allVolumes[i].Type == "regular" {
			paginatedRegularVolumes = append(paginatedRegularVolumes, *allVolumes[i].RegularVolume)
		} else {
			paginatedEcVolumes = append(paginatedEcVolumes, *allVolumes[i].EcVolume)
		}
	}

	// Convert maps to slices
	var dcList []string
	for dc := range dataCenters {
		dcList = append(dcList, dc)
	}
	sort.Strings(dcList)

	var diskTypeList []string
	for diskType := range diskTypes {
		diskTypeList = append(diskTypeList, diskType)
	}
	sort.Strings(diskTypeList)

	return &CollectionDetailsData{
		CollectionName: collectionName,
		RegularVolumes: paginatedRegularVolumes,
		EcVolumes:      paginatedEcVolumes,
		TotalVolumes:   len(regularVolumes),
		TotalEcVolumes: len(ecVolumes),
		TotalChunks:    totalChunks,
		TotalSize:      totalSize,
		DataCenters:    dcList,
		DiskTypes:      diskTypeList,
		LastUpdated:    time.Now(),
		Page:           page,
		PageSize:       pageSize,
		TotalPages:     totalPages,
		SortBy:         sortBy,
		SortOrder:      sortOrder,
	}, nil
}
