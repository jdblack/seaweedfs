package ec_vacuum

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func doubleConfig(v float64) map[string]*plugin_pb.ConfigValue {
	return map[string]*plugin_pb.ConfigValue{
		fieldGarbageThreshold: {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: v}},
	}
}

// TestDeriveConfigRejectsAZeroGarbageThreshold pins that 0 is not "every volume"
// here, unlike the shell's -garbageThreshold: the admin descriptor advertises a 0
// minimum, so an operator can set it, and the value is ignored (with a warning)
// rather than silently loosening the filter to everything. A volume with no live
// needles is not vacuumable at all, so "every volume" does not exist as a setting.
func TestDeriveConfigRejectsAZeroGarbageThreshold(t *testing.T) {
	require.Equal(t, defaultGarbageThreshold, deriveConfig(doubleConfig(0), nil).GarbageThreshold)
	require.Equal(t, defaultGarbageThreshold, deriveConfig(doubleConfig(1.5), nil).GarbageThreshold)
	require.Equal(t, 0.6, deriveConfig(doubleConfig(0.6), nil).GarbageThreshold)
}

// TestDeriveConfigAcceptsAZeroDecodeThreshold pins the other range: 0 disables the
// decode route, so it must be honored rather than treated as "unset".
func TestDeriveConfigAcceptsAZeroDecodeThreshold(t *testing.T) {
	cfg := deriveConfig(map[string]*plugin_pb.ConfigValue{
		fieldDecodeBelowFullness: {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
	}, nil)
	require.Zero(t, cfg.DecodeBelowFullnessPercent)
}
