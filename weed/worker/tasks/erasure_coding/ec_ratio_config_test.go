package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func TestConfigResolveShardCounts(t *testing.T) {
	// Fresh default config carries the build defaults.
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
	// data + parity above MaxShardCount must fall back to the defaults rather
	// than hand an impossible ratio to placement and the encoder.
	values := map[string]*plugin_pb.ConfigValue{
		"data_shards":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(erasure_coding.MaxShardCount)}},
		"parity_shards": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 4}},
	}
	cfg := deriveErasureCodingWorkerConfig(values).TaskConfig
	if cfg.DataShards != erasure_coding.DataShardsCount || cfg.ParityShards != erasure_coding.ParityShardsCount {
		t.Fatalf("oversized ratio = %d+%d, want fallback %d+%d",
			cfg.DataShards, cfg.ParityShards, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	}
}

func TestDeriveErasureCodingWorkerConfigClampsNonPositive(t *testing.T) {
	values := map[string]*plugin_pb.ConfigValue{
		"data_shards":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"parity_shards": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: -2}},
	}
	cfg := deriveErasureCodingWorkerConfig(values).TaskConfig
	if cfg.DataShards != erasure_coding.DataShardsCount || cfg.ParityShards != erasure_coding.ParityShardsCount {
		t.Fatalf("non-positive ratio = %d+%d, want fallback %d+%d",
			cfg.DataShards, cfg.ParityShards, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
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
