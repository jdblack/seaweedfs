package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func TestConfigResolveShardCounts(t *testing.T) {
	// No policy loaded: a fresh default config (0 = inherit) resolves to the build
	// defaults, so an unconfigured cluster behaves exactly as before.
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	c := NewDefaultConfig()
	if got := c.ResolveDataShards(); got != erasure_coding.DataShardsCount {
		t.Fatalf("default data shards = %d, want %d", got, erasure_coding.DataShardsCount)
	}
	if got := c.ResolveParityShards(); got != erasure_coding.ParityShardsCount {
		t.Fatalf("default parity shards = %d, want %d", got, erasure_coding.ParityShardsCount)
	}

	// An explicit ratio wins.
	c.DataShards = 6
	c.ParityShards = 3
	if got := c.ResolveDataShards(); got != 6 {
		t.Fatalf("configured data shards = %d, want 6", got)
	}
	if got := c.ResolveParityShards(); got != 3 {
		t.Fatalf("configured parity shards = %d, want 3", got)
	}

	// A zero-valued config (or nil) falls back rather than returning 0.
	var zero Config
	if got := zero.ResolveDataShards(); got != erasure_coding.DataShardsCount {
		t.Fatalf("zero-config data shards = %d, want %d", got, erasure_coding.DataShardsCount)
	}
	var nilConfig *Config
	if got := nilConfig.ResolveDataShards(); got != erasure_coding.DataShardsCount {
		t.Fatalf("nil-config data shards = %d, want %d", got, erasure_coding.DataShardsCount)
	}
	if got := nilConfig.ResolveParityShards(); got != erasure_coding.ParityShardsCount {
		t.Fatalf("nil-config parity shards = %d, want %d", got, erasure_coding.ParityShardsCount)
	}
}

// The resolution order the whole EC path depends on: an explicit job-type config
// value, else the cluster's EC ratio policy for that collection, else the build
// default. Matching the enterprise edition, the policy is what "unset" means.
func TestResolveShardCountsPrefersExplicitThenPolicy(t *testing.T) {
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	// A global policy covers every collection...
	if err := erasure_coding.SetGlobalECConfig(3, 2); err != nil {
		t.Fatal(err)
	}
	// ...and one collection overrides it.
	if err := erasure_coding.SetCollectionECConfig("blender-blender", 6, 6); err != nil {
		t.Fatal(err)
	}

	unset := NewDefaultConfig()
	if got := unset.ResolveDataShardsFor("blender-blender"); got != 6 {
		t.Fatalf("collection policy data shards = %d, want 6", got)
	}
	if got := unset.ResolveParityShardsFor("blender-blender"); got != 6 {
		t.Fatalf("collection policy parity shards = %d, want 6", got)
	}
	if got := unset.ResolveDataShardsFor("other"); got != 3 {
		t.Fatalf("global policy data shards = %d, want 3", got)
	}
	if got := unset.ResolveParityShardsFor("other"); got != 2 {
		t.Fatalf("global policy parity shards = %d, want 2", got)
	}

	// An explicit config value overrides the policy, and the collection-less
	// resolvers see the global policy.
	explicit := &Config{DataShards: 8, ParityShards: 4}
	if got := explicit.ResolveDataShardsFor("blender-blender"); got != 8 {
		t.Fatalf("explicit data shards = %d, want 8", got)
	}
	if got := explicit.ResolveParityShards(); got != 4 {
		t.Fatalf("explicit parity shards = %d, want 4", got)
	}
	if got := NewDefaultConfig().ResolveDataShards(); got != 3 {
		t.Fatalf("collection-less data shards = %d, want the global policy's 3", got)
	}

	erasure_coding.ResetECConfig()
	if got := unset.ResolveDataShardsFor("blender-blender"); got != erasure_coding.DataShardsCount {
		t.Fatalf("no policy data shards = %d, want the build default %d", got, erasure_coding.DataShardsCount)
	}
}

func TestDeriveErasureCodingWorkerConfigReadsShardCounts(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"data_shards":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 6}},
		"parity_shards": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 3}},
	}
	cfg := deriveErasureCodingWorkerConfig(values).TaskConfig
	if cfg.DataShards != 6 || cfg.ParityShards != 3 {
		t.Fatalf("derived ratio = %d+%d, want 6+3", cfg.DataShards, cfg.ParityShards)
	}
}

func TestDeriveErasureCodingWorkerConfigRejectsOversizedRatio(t *testing.T) {
	// data + parity above MaxShardCount must fall back to "inherit" rather than
	// hand an impossible ratio to placement and the encoder. With no policy
	// loaded, inheriting resolves to the build default.
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	values := map[string]*plugin_pb.ConfigValue{
		"data_shards":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(erasure_coding.MaxShardCount)}},
		"parity_shards": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 4}},
	}
	cfg := deriveErasureCodingWorkerConfig(values).TaskConfig
	if cfg.DataShards != 0 || cfg.ParityShards != 0 {
		t.Fatalf("oversized ratio = %d+%d, want inherit 0+0", cfg.DataShards, cfg.ParityShards)
	}
	if got := cfg.ResolveDataShards(); got != erasure_coding.DataShardsCount {
		t.Fatalf("inherited data shards = %d, want the build default %d", got, erasure_coding.DataShardsCount)
	}
}

func TestDeriveErasureCodingWorkerConfigClampsNonPositive(t *testing.T) {
	// Non-positive values mean "inherit": they must not be stored as a literal
	// ratio, so the policy (or the build default) decides instead.
	erasure_coding.ResetECConfig()
	t.Cleanup(erasure_coding.ResetECConfig)

	values := map[string]*plugin_pb.ConfigValue{
		"data_shards":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"parity_shards": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: -2}},
	}
	cfg := deriveErasureCodingWorkerConfig(values).TaskConfig
	if cfg.DataShards != 0 || cfg.ParityShards != 0 {
		t.Fatalf("non-positive ratio = %d+%d, want inherit 0+0", cfg.DataShards, cfg.ParityShards)
	}

	// An inherit config follows the cluster policy.
	if err := erasure_coding.SetGlobalECConfig(3, 2); err != nil {
		t.Fatal(err)
	}
	if got := cfg.ResolveDataShards(); got != 3 {
		t.Fatalf("inherited data shards = %d, want the policy's 3", got)
	}
	if got := cfg.ResolveParityShards(); got != 2 {
		t.Fatalf("inherited parity shards = %d, want the policy's 2", got)
	}
}

func TestDescriptorExposesShardCountDefaults(t *testing.T) {
	descriptor := NewErasureCodingHandler(nil, "").Descriptor()
	if descriptor.WorkerConfigForm == nil {
		t.Fatal("worker config form is nil")
	}
	hasField := func(name string) bool {
		for _, section := range descriptor.WorkerConfigForm.Sections {
			for _, field := range section.Fields {
				if field.Name == name {
					return true
				}
			}
		}
		return false
	}
	for _, name := range []string{"data_shards", "parity_shards"} {
		if !hasField(name) {
			t.Fatalf("worker config form missing %q field", name)
		}
		if descriptor.WorkerConfigForm.DefaultValues[name] == nil {
			t.Fatalf("worker config form missing %q default", name)
		}
	}
}
