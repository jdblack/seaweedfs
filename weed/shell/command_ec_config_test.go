package shell

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// Argument errors must surface before any filer RPC: a typo should not depend on
// the cluster being reachable, and a rejected command must never leave a half
// edit of the policy document. A zero CommandEnv has no filer, so any path that
// reached the filer would fail with a different error than the one asserted.
func TestEcConfigCommandValidatesArgumentsFirst(t *testing.T) {
	command := &commandEcConfig{}
	commandEnv := &CommandEnv{}

	err := command.Do(nil, commandEnv, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "specify one of -get, -set or -delete")

	err = command.Do([]string{"-get", "-set"}, commandEnv, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-get cannot be combined")

	err = command.Do([]string{"-set", "-delete", "-collection=x", "-dataShards=3", "-parityShards=2"}, commandEnv, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

	err = command.Do([]string{"-set", "-dataShards=3"}, commandEnv, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid EC ratio")

	err = command.Do([]string{"-set", "-dataShards=32", "-parityShards=1"}, commandEnv, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid EC ratio")

	err = command.Do([]string{"-delete"}, commandEnv, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-delete requires -collection")
}

// The `-get` view is what an operator compares against what volumes actually
// are, so it has to state the resolved policy and be explicit when there is none.
func TestEcConfigCommandPrintShowsPolicyAndUnsetDefault(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	var unset bytes.Buffer
	printECConfig(&unset)
	assert.Contains(t, unset.String(), "(unset: build default 10+4)")
	assert.Contains(t, unset.String(), "No EC policy is set")
	assert.Contains(t, unset.String(), "Applies to new EC volumes only")

	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))
	require.NoError(t, erasure_coding.SetCollectionECConfig("blender-blender", 5, 1))

	var configured bytes.Buffer
	printECConfig(&configured)
	assert.Contains(t, configured.String(), "Global default: 3+2")
	assert.Contains(t, configured.String(), "blender-blender: 5+1")
	assert.NotContains(t, configured.String(), "No EC policy is set")
}
