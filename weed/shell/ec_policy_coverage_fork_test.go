package shell

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// The flat *Infos view is what ec.CollectVolumeIdToCollection walks, which is the
// same topology the encode path itself uses.
func ecPolicyTestTopology() *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			RackInfos: []*master_pb.RackInfo{{
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {
							VolumeInfos: []*master_pb.VolumeInformationMessage{
								{Id: 3606, Collection: "blender-blender"},
								{Id: 4119, Collection: "photos"},
								{Id: 2874, Collection: ""},
							},
						},
					},
				}},
			}},
		}},
	}
}

// A run with no policy loaded still encodes, at the layout each volume's .vif
// records and then the build default. That is legitimate but must be visible:
// silently producing 10+4 volumes is what this warning exists to prevent.
func TestEcPolicyCoverageWarningWithoutPolicy(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	buildDefault := fmt.Sprintf("%d+%d", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)

	t.Run("names the uncovered volumes and the filer", func(t *testing.T) {
		got := ecPolicyCoverageWarning("seaweedfs-filer:8888", ecPolicyTestTopology(), []needle.VolumeId{3606})

		assert.Contains(t, got, "blender-blender (1 volumes)")
		assert.Contains(t, got, "1 of 1 volume(s)")
		assert.Contains(t, got, "seaweedfs-filer:8888")
		assert.Contains(t, got, buildDefault)
		assert.Contains(t, got, "ec.config -set")
	})

	t.Run("says so when no filer was discovered at all", func(t *testing.T) {
		got := ecPolicyCoverageWarning("", ecPolicyTestTopology(), []needle.VolumeId{3606})

		assert.Contains(t, got, "no filer discovered")
		assert.Contains(t, got, "no EC ratio policy was loaded")
	})

	t.Run("groups the nameless collection", func(t *testing.T) {
		got := ecPolicyCoverageWarning("filer:8888", ecPolicyTestTopology(), []needle.VolumeId{2874})

		assert.Contains(t, got, "(no collection) (1 volumes)")
	})
}

// A global policy covers every collection, so nothing is worth warning about.
func TestEcPolicyCoverageWarningWithGlobalPolicy(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	require.NoError(t, erasure_coding.SetGlobalECConfig(3, 2))

	assert.Empty(t, ecPolicyCoverageWarning("filer:8888", ecPolicyTestTopology(),
		[]needle.VolumeId{3606, 4119, 2874}))
}

// A per-collection override leaves the other collections uncovered: the warning
// must name only those, so it stays actionable.
func TestEcPolicyCoverageWarningWithCollectionOverride(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)
	require.NoError(t, erasure_coding.SetCollectionECConfig("photos", 5, 1))

	got := ecPolicyCoverageWarning("filer:8888", ecPolicyTestTopology(), []needle.VolumeId{4119, 2874})

	assert.Contains(t, got, "1 of 2 volume(s)")
	assert.Contains(t, got, "(no collection) (1 volumes)")
	assert.NotContains(t, got, "photos (")
}

func TestEcPolicyCoverageWarningNothingToSay(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	assert.Empty(t, ecPolicyCoverageWarning("filer:8888", ecPolicyTestTopology(), nil))
	assert.Empty(t, ecPolicyCoverageWarning("filer:8888", nil, []needle.VolumeId{3606}))
}

// The warning is a message, not a policy engine: it must never fail the command.
func TestEcPolicyCoverageWarningForUnknownVolume(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	// A volume the topology does not know maps to the nameless collection, which
	// no policy names, so it is reported rather than silently skipped.
	got := ecPolicyCoverageWarning("filer:8888", ecPolicyTestTopology(), []needle.VolumeId{999999})

	assert.True(t, strings.Contains(got, "(no collection)"), "got %q", got)
}
