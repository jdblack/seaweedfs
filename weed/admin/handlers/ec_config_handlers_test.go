package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/admin/view/app"
)

// The set endpoints validate before anything reaches the filer, and the bounds
// are the store's own, so a policy that could never be encoded cannot be stored
// through the dashboard either.
func TestDecodeECConfigSetRequest(t *testing.T) {
	request, err := decodeECConfigSetRequest(strings.NewReader(`{"dataShards":3,"parityShards":2}`), false)
	require.NoError(t, err)
	assert.Equal(t, 3, request.DataShards)
	assert.Equal(t, 2, request.ParityShards)

	request, err = decodeECConfigSetRequest(strings.NewReader(`{"collection":"  blender-blender ","dataShards":5,"parityShards":1}`), true)
	require.NoError(t, err)
	assert.Equal(t, "blender-blender", request.Collection, "the collection name is trimmed")

	_, err = decodeECConfigSetRequest(strings.NewReader(`{"dataShards":5,"parityShards":1}`), true)
	require.ErrorContains(t, err, "collection is required")

	_, err = decodeECConfigSetRequest(strings.NewReader(`{"collection":"   ","dataShards":5,"parityShards":1}`), true)
	require.ErrorContains(t, err, "collection is required")

	_, err = decodeECConfigSetRequest(strings.NewReader(`not json at all`), false)
	require.ErrorContains(t, err, "invalid JSON body")

	for _, body := range []string{
		`{"dataShards":0,"parityShards":2}`,
		`{"dataShards":3,"parityShards":0}`,
		`{"dataShards":-1,"parityShards":2}`,
		`{"dataShards":32,"parityShards":1}`,
	} {
		_, err := decodeECConfigSetRequest(strings.NewReader(body), false)
		require.Error(t, err, "body %s must be rejected", body)
	}

	// The maximum legal layout is accepted; the bound is 32 shards, not the build's 14.
	request, err = decodeECConfigSetRequest(strings.NewReader(`{"dataShards":16,"parityShards":16}`), false)
	require.NoError(t, err)
	assert.Equal(t, 32, request.DataShards+request.ParityShards)
}

// The page must render: a templ runtime error (a bad attribute, a missing
// context) only shows up when it is actually rendered.
func TestECConfigPageRenders(t *testing.T) {
	data := dash.ECConfigForkView{
		Global:            &dash.ECConfigForkEntry{DataShards: 3, ParityShards: 2},
		Collections:       []dash.ECConfigForkEntry{{Collection: "blender-blender", DataShards: 5, ParityShards: 1}},
		BuildDataShards:   10,
		BuildParityShards: 4,
		FilerAvailable:    true,
		FilerAddress:      "filer:8888.18888",
	}

	var rendered bytes.Buffer
	require.NoError(t, app.ECConfigPage(data).Render(context.Background(), &rendered))

	page := rendered.String()
	assert.Contains(t, page, "EC Configuration")
	assert.Contains(t, page, "new EC volumes only")
	assert.Contains(t, page, "blender-blender")
	assert.Contains(t, page, "5+1")
	assert.Contains(t, page, `data-collection="blender-blender"`)
	assert.Contains(t, page, "3+2")

	// Without a filer the page says so and disables the write buttons.
	noFiler := dash.ECConfigForkView{Collections: []dash.ECConfigForkEntry{}, BuildDataShards: 10, BuildParityShards: 4}
	rendered.Reset()
	require.NoError(t, app.ECConfigPage(noFiler).Render(context.Background(), &rendered))
	assert.Contains(t, rendered.String(), "No filer discovered")
	assert.Contains(t, rendered.String(), "disabled")
}

func TestECConfigForkViewMarshalsApiKeys(t *testing.T) {
	view := dash.ECConfigForkView{
		Global:            &dash.ECConfigForkEntry{DataShards: 3, ParityShards: 2},
		Collections:       []dash.ECConfigForkEntry{{Collection: "blender-blender", DataShards: 5, ParityShards: 1}},
		BuildDataShards:   10,
		BuildParityShards: 4,
		FilerAvailable:    true,
	}
	body, err := json.Marshal(view)
	require.NoError(t, err)

	text := string(body)
	assert.Contains(t, text, `"global":{"data_shards":3,"parity_shards":2}`)
	assert.Contains(t, text, `"collection":"blender-blender","data_shards":5,"parity_shards":1`)
	assert.Contains(t, text, `"build_data_shards":10`)
	assert.Contains(t, text, `"filer_available":true`)
}
