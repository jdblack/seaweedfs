package storage

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Fork-local addition (not present in upstream SeaweedFS). Kept in its own file
// so upstream updates to disk_location.go never conflict with it.

// NewScratchDiskLocation describes a transient scratch directory: it resolves the
// paths and records the disk type, but starts no disk-health prober, writes no
// vol_dir.uuid, and owns no volumes.
//
// NewDiskLocation is the volume server's constructor, not a value constructor: it
// starts a goroutine that statfs's the directory immediately and then every
// minute until DiskLocation.Close(), and it writes <dir>/vol_dir.uuid. That
// lifetime is correct for a real volume server, whose locations live as long as
// the process. It is wrong for a directory the caller is about to delete — e.g.
// the offline compaction of a working copy in weed/ec/ecvacuum, where an unclosed
// prober reports the removed directory as a dead disk forever and pins
// volumeServer_disk_error_status{<dir>,type="error"} = 1 on the process.
//
// Use this for callers that only need Directory, IdxDirectory and DiskType (what
// Store.CompactVolumeFiles reads). It deliberately does not call GenerateDirUuid,
// which keeps a stray vol_dir.uuid out of the scratch path and keeps the
// glog.Fatalf in that call away from non-server processes.
//
// Close() is safe on the returned location (it iterates empty maps and closes
// closeCh), but nothing needs to call it: no goroutine was started.
func NewScratchDiskLocation(dir, idxDir string, diskType types.DiskType) *DiskLocation {
	dir = util.ResolvePath(dir)
	if idxDir == "" {
		idxDir = dir
	} else {
		idxDir = util.ResolvePath(idxDir)
	}
	location := &DiskLocation{
		Directory:    dir,
		IdxDirectory: idxDir,
		DiskType:     diskType,
	}
	location.volumes = make(map[needle.VolumeId]*Volume)
	location.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
	location.closeCh = make(chan struct{})
	return location
}
