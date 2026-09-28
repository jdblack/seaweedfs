package erasure_coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The cluster EC policy: a collection override beats the global default, the
// default beats "no policy", and removing an override falls back to the default.
// This is the resolution `ec.encode`, the worker and the admin all rely on, and
// it mirrors the enterprise edition's GetECConfig.
func TestECConfigResolutionOrder(t *testing.T) {
	ResetECConfig()
	t.Cleanup(ResetECConfig)

	// No policy at all: callers keep the build default (they see "not found").
	_, found := ResolveECConfig("blender-blender")
	assert.False(t, found)
	assert.Nil(t, GetECConfig("blender-blender"))

	require.NoError(t, SetGlobalECConfig(3, 2))
	globally, found := ResolveECConfig("blender-blender")
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 3, ParityShards: 2}, globally)

	require.NoError(t, SetCollectionECConfig("blender-blender", 5, 1))
	overridden, found := ResolveECConfig("blender-blender")
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 5, ParityShards: 1}, overridden)

	// A sibling collection still sees the global default.
	other, found := ResolveECConfig("other")
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 3, ParityShards: 2}, other)

	require.NoError(t, DeleteCollectionECConfig("blender-blender"))
	back, found := ResolveECConfig("blender-blender")
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 3, ParityShards: 2}, back)
}

// GetECConfig hands the encode path a mutable context (the encoder stamps
// BlockSize on it), so it must never hand out the registry's own value.
func TestECConfigGetReturnsPrivateCopy(t *testing.T) {
	ResetECConfig()
	t.Cleanup(ResetECConfig)

	require.NoError(t, SetGlobalECConfig(3, 2))
	ctx := GetECConfig("blender-blender")
	require.NotNil(t, ctx)
	assert.Equal(t, 3, ctx.DataShards)
	assert.Equal(t, 2, ctx.ParityShards)

	ctx.DataShards = 10
	ctx.ParityShards = 4
	again := GetECConfig("blender-blender")
	require.NotNil(t, again)
	assert.Equal(t, 3, again.DataShards, "mutating the returned context must not change the policy")
	assert.Equal(t, 2, again.ParityShards)
}

// The same bounds the encoder and the .vif reader enforce, so a policy can
// never name a layout the rest of the pipeline would reject as corrupt.
func TestECConfigValidation(t *testing.T) {
	ResetECConfig()
	t.Cleanup(ResetECConfig)

	assert.Error(t, SetGlobalECConfig(0, 4), "zero data shards")
	assert.Error(t, SetGlobalECConfig(10, 0), "zero parity shards")
	assert.Error(t, SetGlobalECConfig(-1, 4), "negative")
	require.NoError(t, SetGlobalECConfig(31, 1), "total 32 is the maximum and is legal")
	require.NoError(t, SetGlobalECConfig(30, 2), "total 32 is legal")
	assert.Error(t, SetGlobalECConfig(32, 1), "total 33 exceeds MaxShardCount")
	assert.Error(t, SetCollectionECConfig("", 3, 2), "an override needs a collection name")
	require.NoError(t, SetCollectionECConfig("c", 1, 1), "1+1 is the smallest legal layout")
}

// What the shell writes and the admin reads is one JSON document; a round trip
// must reproduce the policy exactly.
func TestECConfigJSONRoundTrip(t *testing.T) {
	ResetECConfig()
	t.Cleanup(ResetECConfig)

	require.NoError(t, SetGlobalECConfig(3, 2))
	require.NoError(t, SetCollectionECConfig("blender-blender", 5, 1))
	require.NoError(t, SetCollectionECConfig("aaa", 4, 4))

	doc, err := MarshalECConfigJSON()
	require.NoError(t, err)

	// A fresh process (or another component) installs the document.
	ResetECConfig()
	_, found := ResolveECConfig("blender-blender")
	require.False(t, found)
	require.NoError(t, ApplyECConfigJSON(doc))

	global, found := GlobalECConfig()
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 3, ParityShards: 2}, global)
	collections := CollectionECConfigs()
	require.Len(t, collections, 2)
	assert.Equal(t, "aaa", collections[0].Collection, "snapshot is sorted for display")
	assert.Equal(t, ECConfigEntry{DataShards: 4, ParityShards: 4}, collections[0].ECConfigEntry)
	assert.Equal(t, "blender-blender", collections[1].Collection)
	assert.Equal(t, ECConfigEntry{DataShards: 5, ParityShards: 1}, collections[1].ECConfigEntry)
}

// A bad document must not half-apply: a partially loaded policy would encode
// some volumes with a layout the operator never chose.
func TestECConfigApplyRejectsBadDocumentAtomically(t *testing.T) {
	ResetECConfig()
	t.Cleanup(ResetECConfig)

	require.NoError(t, SetGlobalECConfig(3, 2))
	require.NoError(t, SetCollectionECConfig("keep", 4, 4))

	require.Error(t, ApplyECConfigJSON([]byte(`{"global":{"data_shards":9,"parity_shards":9},"collections":{"bad":{"data_shards":0,"parity_shards":2}}}`)))

	global, found := GlobalECConfig()
	require.True(t, found, "the previous policy must survive a rejected document")
	assert.Equal(t, ECConfigEntry{DataShards: 3, ParityShards: 2}, global)
	kept, found := ResolveECConfig("keep")
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 4, ParityShards: 4}, kept)
	// "bad" was never installed: with a global default in place it resolves to
	// that default, not to the rejected 0+2 override.
	bad, found := ResolveECConfig("bad")
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 3, ParityShards: 2}, bad)

	require.Error(t, ApplyECConfigJSON([]byte(`{"collections":{"c":null}}`)), "a null entry is not a ratio")
	require.Error(t, ApplyECConfigJSON([]byte(`not json`)))
}

// "No policy anywhere" is a legal state: it has to marshal to an empty document
// so a shell that saves an empty policy clears the file rather than erroring.
func TestECConfigEmptyState(t *testing.T) {
	ResetECConfig()
	t.Cleanup(ResetECConfig)

	doc, err := MarshalECConfigJSON()
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(doc))

	require.NoError(t, ApplyECConfigJSON([]byte(`{}`)))
	_, found := ResolveECConfig("anything")
	assert.False(t, found)
}

// The dashboard's "clear" — and an operator returning a cluster to the build
// default — removes the global entry without touching the overrides.
func TestECConfigDeleteGlobalKeepsOverrides(t *testing.T) {
	ResetECConfig()
	t.Cleanup(ResetECConfig)

	require.NoError(t, SetGlobalECConfig(3, 2))
	require.NoError(t, SetCollectionECConfig("blender-blender", 5, 1))

	DeleteGlobalECConfig()

	_, found := GlobalECConfig()
	assert.False(t, found)
	override, found := ResolveECConfig("blender-blender")
	require.True(t, found)
	assert.Equal(t, ECConfigEntry{DataShards: 5, ParityShards: 1}, override, "an override outlives the global default")
	_, found = ResolveECConfig("other")
	assert.False(t, found, "with no global and no override, callers fall back to the build default")

	DeleteGlobalECConfig() // idempotent
	assert.False(t, func() bool { _, f := GlobalECConfig(); return f }())
}
