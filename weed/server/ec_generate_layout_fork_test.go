package weed_server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// writeVifWithLayout records a layout the way a previous encode would have.
func writeVifWithLayout(t *testing.T, dir string, dataShards, parityShards int) string {
	t.Helper()
	vifPath := filepath.Join(dir, "vif.vif")
	require.NoError(t, volume_info.SaveVolumeInfo(vifPath, &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: 1024,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   uint32(dataShards),
			ParityShards: uint32(parityShards),
		},
	}))
	return vifPath
}

// The caller's request is the cluster's policy: it must win, because that is how
// a volume with no recorded layout gets the operator's ratio.
func TestResolveGenerateEcLayoutPrefersRequest(t *testing.T) {
	dir := t.TempDir()
	vifPath := writeVifWithLayout(t, dir, 5, 1) // an older layout on disk

	ctx, source, err := resolveGenerateEcLayout(needle.VolumeId(3606), "blender-blender",
		&volume_server_pb.EcShardConfig{DataShards: 3, ParityShards: 2}, vifPath)
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromRequest, source)
	assert.Equal(t, 3, ctx.DataShards)
	assert.Equal(t, 2, ctx.ParityShards)
	assert.Equal(t, "blender-blender", ctx.Collection)
}

// No request: the .vif decides, so a volume encoded before the policy existed is
// regenerated the way it was.
func TestResolveGenerateEcLayoutUsesVifWithoutRequest(t *testing.T) {
	dir := t.TempDir()
	vifPath := writeVifWithLayout(t, dir, 5, 1)

	ctx, source, err := resolveGenerateEcLayout(needle.VolumeId(3606), "c", nil, vifPath)
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromVif, source)
	assert.Equal(t, 5, ctx.DataShards)
	assert.Equal(t, 1, ctx.ParityShards)
}

// A request that names no layout at all (an empty message, or a caller that
// fills only unrelated fields) is "unset", not "0+0": fall through to the .vif.
func TestResolveGenerateEcLayoutTreatsEmptyRequestAsUnset(t *testing.T) {
	dir := t.TempDir()
	vifPath := writeVifWithLayout(t, dir, 5, 1)

	ctx, source, err := resolveGenerateEcLayout(needle.VolumeId(3606), "c", &volume_server_pb.EcShardConfig{}, vifPath)
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromVif, source)
	assert.Equal(t, 5, ctx.DataShards)
	assert.Equal(t, 1, ctx.ParityShards)
}

// Neither request nor .vif: the build default, unchanged from before this field
// existed.
func TestResolveGenerateEcLayoutFallsBackToBuildDefault(t *testing.T) {
	dir := t.TempDir()

	ctx, source, err := resolveGenerateEcLayout(needle.VolumeId(3606), "c", nil, filepath.Join(dir, "missing.vif"))
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromDefault, source)
	assert.Equal(t, erasure_coding.DataShardsCount, ctx.DataShards)
	assert.Equal(t, erasure_coding.ParityShardsCount, ctx.ParityShards)
}

// A .vif recording an impossible layout is ignored (it predates validation, or
// is corrupt), and the default applies — the pre-existing behavior.
func TestResolveGenerateEcLayoutIgnoresInvalidVif(t *testing.T) {
	dir := t.TempDir()
	vifPath := writeVifWithLayout(t, dir, 0, 2)

	ctx, source, err := resolveGenerateEcLayout(needle.VolumeId(3606), "c", nil, vifPath)
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromDefault, source)
	assert.Equal(t, erasure_coding.DataShardsCount, ctx.DataShards)
	assert.Equal(t, erasure_coding.ParityShardsCount, ctx.ParityShards)
}

// An impossible requested layout fails loudly instead of being encoded at some
// other ratio.
func TestResolveGenerateEcLayoutRejectsInvalidRequest(t *testing.T) {
	dir := t.TempDir()
	vifPath := writeVifWithLayout(t, dir, 5, 1)

	for _, requested := range []*volume_server_pb.EcShardConfig{
		{DataShards: 0, ParityShards: 2},
		{DataShards: 3, ParityShards: 0},
		{DataShards: 32, ParityShards: 1},
	} {
		_, source, err := resolveGenerateEcLayout(needle.VolumeId(3606), "c", requested, vifPath)
		require.Errorf(t, err, "requested %d+%d must be rejected", requested.DataShards, requested.ParityShards)
		assert.Empty(t, source)
		assert.Contains(t, err.Error(), "invalid requested EC config")
	}
}

// The maximum legal layout is accepted (the bound is MaxShardCount, not the
// build's 14).
func TestResolveGenerateEcLayoutAcceptsMaximumLayout(t *testing.T) {
	dir := t.TempDir()

	ctx, source, err := resolveGenerateEcLayout(needle.VolumeId(3606), "c",
		&volume_server_pb.EcShardConfig{DataShards: 16, ParityShards: 16}, filepath.Join(dir, "missing.vif"))
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromRequest, source)
	assert.Equal(t, 16, ctx.DataShards)
	assert.Equal(t, 16, ctx.ParityShards)
}

// The .vif lookup must not blow up when the directory does not exist at all
// (a volume whose disk is gone, or a fresh store).
func TestResolveGenerateEcLayoutHandlesMissingVifPath(t *testing.T) {
	ctx, source, err := resolveGenerateEcLayout(needle.VolumeId(1), "c", nil, "/nonexistent/dir/volume.vif")
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromDefault, source)
	assert.NotNil(t, ctx)

	// an unreadable-but-existing path is the same story
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "broken.vif"), []byte("{ not json"), 0o644))
	ctx, source, err = resolveGenerateEcLayout(needle.VolumeId(1), "c", nil, filepath.Join(dir, "broken.vif"))
	require.NoError(t, err)
	assert.Equal(t, ecLayoutFromDefault, source)
	assert.NotNil(t, ctx)
}
