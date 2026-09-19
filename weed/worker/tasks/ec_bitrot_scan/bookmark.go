package ec_bitrot_scan

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// bookmarkFileName is the rotation cursor, kept in the worker's working
// directory alongside worker.id — the location `weed worker -workingDir`
// documents as "working directory for persistent worker state", so the cursor
// survives a container restart.
//
// It records the last volume a detection cycle proposed, so the next cycle
// resumes after it. Deliberately a *key* (volume id, collection, disk type)
// rather than an index: the roster changes between cycles as volumes are
// created, deleted, and re-encoded, and a positional index would then point at
// a different volume. Losing the file is safe — a pod recreation wipes the
// working directory and the next cycle starts at the front of the roster,
// which costs one uneven pass and never skips a volume.
const bookmarkFileName = "ec_bitrot_scan.bookmark"

// scanBookmark identifies the volume a rotation cycle last proposed.
type scanBookmark struct {
	VolumeID   uint32 `json:"volume_id"`
	Collection string `json:"collection"`
	DiskType   string `json:"disk_type"`
}

// readBookmark loads the rotation cursor. A missing cursor is the ordinary
// first-cycle state; an unreadable or malformed one is a real (if cosmetic)
// problem, so it warns. Either way the caller starts at the front.
func readBookmark(workingDir string) *scanBookmark {
	if workingDir == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(workingDir, bookmarkFileName))
	if err != nil {
		if os.IsNotExist(err) {
			glog.V(1).Infof("ec_bitrot_scan: no bookmark at %s, starting this sweep at the front of the roster", workingDir)
		} else {
			glog.Warningf("ec_bitrot_scan: read bookmark: %v", err)
		}
		return nil
	}
	var bm scanBookmark
	if err := json.Unmarshal(data, &bm); err != nil {
		glog.Warningf("ec_bitrot_scan: unreadable bookmark, starting this sweep at the front of the roster: %v", err)
		return nil
	}
	return &bm
}

// writeBookmark persists the rotation cursor. Best-effort: detection must not
// fail because the cursor could not be written, and a lost cursor only costs a
// restart of the rotation. Written through a temp file and renamed so a crash
// mid-write cannot leave a torn cursor for the next cycle to parse.
func writeBookmark(workingDir string, bm *scanBookmark) {
	if workingDir == "" || bm == nil {
		return
	}
	if err := os.MkdirAll(workingDir, 0o755); err != nil {
		glog.Warningf("ec_bitrot_scan: prepare bookmark dir: %v", err)
		return
	}
	data, err := json.Marshal(bm)
	if err != nil {
		glog.Warningf("ec_bitrot_scan: marshal bookmark: %v", err)
		return
	}
	path := filepath.Join(workingDir, bookmarkFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		glog.Warningf("ec_bitrot_scan: write bookmark: %v", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		glog.Warningf("ec_bitrot_scan: commit bookmark: %v", err)
	}
}
