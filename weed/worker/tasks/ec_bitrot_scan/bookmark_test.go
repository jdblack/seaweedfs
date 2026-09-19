package ec_bitrot_scan

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBookmarkRoundTripAndRecovery(t *testing.T) {
	dir := t.TempDir()

	// A missing cursor means "start at the front", not an error.
	require.Nil(t, readBookmark(dir))

	bm := &scanBookmark{VolumeID: 3354, Collection: "movies-archive", DiskType: "hdd"}
	writeBookmark(dir, bm)
	require.Equal(t, bm, readBookmark(dir))

	// No temp file is left behind by the rename.
	_, err := os.Stat(filepath.Join(dir, bookmarkFileName+".tmp"))
	require.True(t, os.IsNotExist(err))

	// A torn or hand-edited cursor must not stop detection.
	require.NoError(t, os.WriteFile(filepath.Join(dir, bookmarkFileName), []byte("{"), 0o644))
	require.Nil(t, readBookmark(dir))

	// An empty working directory disables the cursor without failing.
	require.Nil(t, readBookmark(""))
	writeBookmark("", bm)
	writeBookmark(dir, nil)
}

func TestBookmarkPositionResumesAndWraps(t *testing.T) {
	candidates := []ecVolumeCandidate{
		{VolumeID: 10, Collection: "a", DiskType: "hdd"},
		{VolumeID: 20, Collection: "a", DiskType: "hdd"},
		{VolumeID: 30, Collection: "a", DiskType: "hdd"},
	}

	// No cursor: the pass starts at the front.
	require.Equal(t, 0, bookmarkPosition(candidates, nil))
	require.Equal(t, 0, bookmarkPosition(nil, &scanBookmark{VolumeID: 20}))

	// The cursor names the last volume proposed, so the pass resumes *after* it.
	require.Equal(t, 2, bookmarkPosition(candidates, &scanBookmark{VolumeID: 20, Collection: "a", DiskType: "hdd"}))

	// A deleted volume resumes at whatever now follows its old position.
	require.Equal(t, 1, bookmarkPosition(candidates, &scanBookmark{VolumeID: 15, Collection: "a"}))

	// A cursor at or past the end of a shrunken roster wraps to the front.
	// (The cursor always carries the exact disk type the cycle proposed, so the
	// ordering tie-breaks line up with the roster.)
	require.Equal(t, 0, bookmarkPosition(candidates, &scanBookmark{VolumeID: 30, Collection: "a", DiskType: "hdd"}))
	require.Equal(t, 0, bookmarkPosition(candidates, &scanBookmark{VolumeID: 99, Collection: "a", DiskType: "hdd"}))

	// Collection and disk type break ties the same way the roster sorts them.
	two := []ecVolumeCandidate{
		{VolumeID: 10, Collection: "a", DiskType: "hdd"},
		{VolumeID: 10, Collection: "b", DiskType: "hdd"},
	}
	require.Equal(t, 1, bookmarkPosition(two, &scanBookmark{VolumeID: 10, Collection: "a", DiskType: "hdd"}))
	require.Equal(t, 0, bookmarkPosition(two, &scanBookmark{VolumeID: 10, Collection: "b", DiskType: "hdd"}))
}
