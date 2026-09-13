package ecvacuum

import (
	"fmt"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// BuildVacuumTarget resolves the latest shard placement for a volume and refuses
// to vacuum a volume that cannot be decoded locally.
func BuildVacuumTarget(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) (VacuumTarget, error) {
	// Refuse a volume whose shards span encode generations: that is the signature
	// of a prior vacuum or encode that died mid-distribution. Compacting such a
	// set would silently mix two shard layouts, so it is left for investigation.
	if gens := encodeGenerations(topo, volumeID, collection, diskType); len(gens) > 1 {
		return VacuumTarget{}, fmt.Errorf("EC volume %d spans %d encode generations; a prior operation may be incomplete, run ec.rebuild before vacuuming", volumeID, len(gens))
	}

	holders := shardHolders(topo, volumeID, collection, diskType)
	if len(holders) == 0 {
		return VacuumTarget{}, fmt.Errorf("no shards found for EC volume %d collection %q", volumeID, collection)
	}
	dataShards, parityShards := shardRatio(topo, volumeID, collection, diskType)
	if dataShards <= 0 || parityShards <= 0 {
		dataShards, parityShards = erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount
	}
	ids := make([]uint32, 0, len(holders))
	for id := range holders {
		ids = append(ids, id)
	}
	if !HasAllDataShards(shardBitsFromIDs(ids), dataShards) {
		return VacuumTarget{}, fmt.Errorf("EC volume %d is missing a data shard (%v present); leaving it for ec.rebuild", volumeID, ids)
	}
	return VacuumTarget{
		VolumeID:     volumeID,
		Collection:   collection,
		DiskType:     diskType,
		DataShards:   dataShards,
		ParityShards: parityShards,
		EncodeTsNs:   newestEncodeTsNs(topo, volumeID, collection, diskType),
		Holders:      holders,
	}, nil
}

// LoadVacuumOptions fills the vacuum inputs from the collected .vif, falling back
// to the topology's ratio and the legacy block layout when the .vif is absent.
func LoadVacuumOptions(workDir, base string, target VacuumTarget) (Options, error) {
	opts := Options{
		VolumeID:     target.VolumeID,
		Collection:   target.Collection,
		DataShards:   target.DataShards,
		ParityShards: target.ParityShards,
		DiskType:     types.ToDiskType(target.DiskType),
	}
	vi, _, found, err := volume_info.MaybeLoadVolumeInfo(filepath.Join(workDir, base+".vif"))
	if err != nil {
		// A present-but-unreadable .vif is a hard error: guessing the layout would
		// risk decoding the shards in the wrong block order.
		return opts, err
	}
	if !found || vi == nil {
		return opts, nil
	}
	opts.Version = vi.GetVersion()
	opts.ExpireAtSec = vi.GetExpireAtSec()
	opts.EncodedDatFileSize = vi.GetDatFileSize()
	if cfg := vi.GetEcShardConfig(); cfg != nil {
		if erasure_coding.ValidEcShardCounts(cfg.GetDataShards(), cfg.GetParityShards()) {
			opts.DataShards = int(cfg.GetDataShards())
			opts.ParityShards = int(cfg.GetParityShards())
		}
		opts.BlockSize = cfg.GetBlockSize()
	}
	return opts, nil
}

func shardBitsFromIDs(ids []uint32) erasure_coding.ShardBits {
	var bits erasure_coding.ShardBits
	for _, id := range ids {
		bits = bits.Set(erasure_coding.ShardId(id))
	}
	return bits
}

// shardRatio returns the volume's (data, parity) shard counts from the newest
// encode generation in the topology.
func shardRatio(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) (int, int) {
	newest := newestEncodeTsNs(topo, volumeID, collection, diskType)
	dataShards, parityShards := 0, 0
	eachDiskInfo(topo, func(dt string, diskInfo *master_pb.DiskInfo) {
		if diskType != "" && dt != diskType {
			return
		}
		for _, eci := range diskInfo.GetEcShardInfos() {
			if eci.GetId() != volumeID || eci.GetCollection() != collection || eci.GetEncodeTsNs() != newest {
				continue
			}
			if ds := int(eci.GetDataShards()); ds > dataShards {
				dataShards = ds
			}
			if ps := int(eci.GetParityShards()); ps > parityShards {
				parityShards = ps
			}
		}
	})
	return dataShards, parityShards
}

// newestEncodeTsNs returns the newest encode generation for a volume.
func newestEncodeTsNs(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) int64 {
	newest := int64(0)
	eachDiskInfo(topo, func(dt string, diskInfo *master_pb.DiskInfo) {
		if diskType != "" && dt != diskType {
			return
		}
		for _, eci := range diskInfo.GetEcShardInfos() {
			if eci.GetId() != volumeID || eci.GetCollection() != collection {
				continue
			}
			if eci.GetEncodeTsNs() > newest {
				newest = eci.GetEncodeTsNs()
			}
		}
	})
	return newest
}

// EncodeGenerations returns the distinct encode generations (EncodeTsNs) the
// topology reports for a volume. A healthy volume has exactly one; more than one
// means shards from different encodes coexist, which vacuum must not compact.
func encodeGenerations(topo *master_pb.TopologyInfo, volumeID uint32, collection, diskType string) map[int64]struct{} {
	gens := make(map[int64]struct{})
	eachDiskInfo(topo, func(dt string, diskInfo *master_pb.DiskInfo) {
		if diskType != "" && dt != diskType {
			return
		}
		for _, eci := range diskInfo.GetEcShardInfos() {
			if eci.GetId() != volumeID || eci.GetCollection() != collection {
				continue
			}
			gens[eci.GetEncodeTsNs()] = struct{}{}
		}
	})
	return gens
}

// eachDiskInfo visits every disk info in the topology.
func eachDiskInfo(topo *master_pb.TopologyInfo, fn func(diskType string, diskInfo *master_pb.DiskInfo)) {
	for _, dc := range topo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				for dt, diskInfo := range node.GetDiskInfos() {
					if diskInfo != nil {
						fn(dt, diskInfo)
					}
				}
			}
		}
	}
}
