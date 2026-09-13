package storage

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The scratch constructor must not start the process-lifetime disk prober: its
// caller is about to delete the directory it describes (the EC vacuum working
// copy), and a prober would report that removed directory as a dead disk on
// every tick, forever.
func TestNewScratchDiskLocationStartsNoProber(t *testing.T) {
	original := newDiskStatus
	defer func() { newDiskStatus = original }()

	var probes atomic.Int32
	newDiskStatus = func(path string, _ stats.DiskIOProbeConfig) *volume_server_pb.DiskStatus {
		probes.Add(1)
		return &volume_server_pb.DiskStatus{Dir: path}
	}

	dir := t.TempDir()
	loc := NewScratchDiskLocation(dir, dir, types.HardDriveType)
	require.NotNil(t, loc)

	// A prober probes once immediately, so any leak shows up right away and the
	// 60s tick never comes into it.
	time.Sleep(50 * time.Millisecond)
	require.Never(t, func() bool { return probes.Load() != 0 }, 150*time.Millisecond, 25*time.Millisecond,
		"scratch location started a disk prober")

	// The seam is live: the volume server's own constructor does probe.
	serverDir := t.TempDir()
	server := NewDiskLocation(serverDir, 1, util.MinFreeSpace{}, serverDir, types.HardDriveType, nil, stats.DiskIOProbeConfig{})
	defer server.Close()
	defer cleanupDiskProbeGauges(serverDir)
	require.Eventually(t, func() bool { return probes.Load() > 0 }, time.Second, 10*time.Millisecond,
		"NewDiskLocation did not probe; the newDiskStatus seam is not counting")
}

// The scratch location carries no directory UUID and writes no vol_dir.uuid:
// nothing in the offline compaction path reads DirectoryUuid, and skipping the
// write also keeps GenerateDirUuid's glog.Fatalf out of non-server processes.
func TestNewScratchDiskLocationWritesNoDirUuid(t *testing.T) {
	dir := t.TempDir()
	loc := NewScratchDiskLocation(dir, dir, types.HardDriveType)

	require.Equal(t, dir, loc.Directory)
	require.Equal(t, dir, loc.IdxDirectory)
	require.Empty(t, loc.DirectoryUuid)
	require.NoFileExists(t, filepath.Join(dir, UUIDFileName))

	// Close is safe even though nothing needs it (no prober was started).
	loc.Close()
}

// An explicit idxDir is honored, and unlike NewDiskLocation it is not created:
// the scratch caller owns its directories.
func TestNewScratchDiskLocationHonorsIdxDir(t *testing.T) {
	dir := t.TempDir()
	idxDir := filepath.Join(dir, "idx")

	loc := NewScratchDiskLocation(dir, idxDir, types.HardDriveType)
	require.Equal(t, dir, loc.Directory)
	require.Equal(t, idxDir, loc.IdxDirectory)
}

func cleanupDiskProbeGauges(dir string) {
	stats.VolumeServerDiskErrorGauge.DeleteLabelValues(dir, "error")
	for _, k := range []string{"all", "used", "free", "avail"} {
		stats.VolumeServerResourceGauge.DeleteLabelValues(dir, k)
	}
}
