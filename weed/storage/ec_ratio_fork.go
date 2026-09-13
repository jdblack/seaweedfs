package storage

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// Fork-local addition (not present in upstream SeaweedFS). Per-volume EC ratio
// helpers for the volume server, which does not otherwise know the cluster's EC
// ratio. Kept in its own file so upstream edits to the storage core do not
// conflict with them.

// ecDataShardsOrDefault returns the data-shard count of the EC volumes mounted on
// this location (they normally share one ratio), or the build default when it
// holds none. Used for shard<->volume-slot maths on the volume server, which does
// not otherwise know the cluster's EC ratio.
func (l *DiskLocation) ecDataShardsOrDefault() int {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()
	for _, ecVolume := range l.ecVolumes {
		if ds := ecVolume.ECDataShards(); ds > 0 {
			return ds
		}
	}
	return erasure_coding.DataShardsCount
}

// EcShardVolumeDataShards returns the data-shard count of a volume's EC layout,
// resolving it from a locally mounted EcVolume, else the volume's .vif on any
// disk, else the build default. EC shard placement (FindEcShardTargetLocation)
// uses it for free-slot maths, where the caller only has the volume id.
func (s *Store) EcShardVolumeDataShards(collection string, vid needle.VolumeId) int {
	if ev, found := s.FindEcVolume(vid); found {
		if ds := ev.ECDataShards(); ds > 0 {
			return ds
		}
	}
	for _, location := range s.Locations {
		if location.Directory != "" {
			if ds := ecDataShardsFromVifDir(collection, location.Directory, vid); ds > 0 {
				return ds
			}
		}
		if location.IdxDirectory != "" && location.IdxDirectory != location.Directory {
			if ds := ecDataShardsFromVifDir(collection, location.IdxDirectory, vid); ds > 0 {
				return ds
			}
		}
	}
	return erasure_coding.DataShardsCount
}
