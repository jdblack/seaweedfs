package erasure_coding

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// Fork-local addition (not present in upstream SeaweedFS). The cluster's EC
// ratio policy, mirroring the enterprise edition's erasure_coding/ec_config.go:
// one global default plus per-collection overrides, resolved per collection at
// encode time. The value is persisted by the *callers* (shell command and admin
// dashboard) as a JSON document in the filer — this file owns validation, the
// registry, and the codec, and deliberately has no filer dependency.
//
// Semantics, matching the enterprise edition (its admin UI states them):
//   - a per-collection entry overrides the global entry;
//   - both counts must be positive and data+parity must be <= MaxShardCount;
//   - the policy applies to volumes encoded *after* it is set: an existing EC
//     volume keeps the ratio recorded in its own .vif, which every reader in
//     this fork already resolves per volume (DataShardsOrDefault,
//     EcShardsVolumeDataShards, ecbalancer.VolumeShardRatio).

// ECConfigPath is the filer path holding the policy document. The enterprise
// edition stores its own file (it also writes a .pb mirror) somewhere under the
// same /etc/seaweedfs convention it uses for seal.conf and tiering.conf; the
// exact name there is not observable from its binary, so this fork picks a JSON
// file next to those. Only the content matters for us: an enterprise process
// would have to agree on both name and schema to read it.
const ECConfigPath = "/etc/seaweedfs/ec_ratio.json"

// ECConfigEntry is one (data, parity) pair: the EC ratio a policy names.
type ECConfigEntry struct {
	DataShards   int `json:"data_shards"`
	ParityShards int `json:"parity_shards"`
}

// Total is the number of shards a volume encoded under this entry has.
func (e ECConfigEntry) Total() int {
	return e.DataShards + e.ParityShards
}

// Validate reports whether the pair could describe a real EC volume. It reuses
// ValidEcShardCounts so the policy cannot name a layout the encoder would
// refuse (and the .vif reader would reject as corrupt).
func (e ECConfigEntry) Validate() error {
	if !ValidEcShardCounts(uint32(e.DataShards), uint32(e.ParityShards)) {
		return fmt.Errorf("invalid EC ratio %d+%d: both counts must be positive and the total must not exceed %d",
			e.DataShards, e.ParityShards, MaxShardCount)
	}
	return nil
}

// ECConfigJSON is the on-disk shape of the policy: the global default plus the
// per-collection overrides. An absent (nil) global means "no policy": callers
// keep the build default, exactly as before this feature existed.
type ECConfigJSON struct {
	Global      *ECConfigEntry            `json:"global,omitempty"`
	Collections map[string]*ECConfigEntry `json:"collections,omitempty"`
}

var (
	ecConfigMu        sync.RWMutex
	globalECConfig    *ECConfigEntry
	collectionConfigs map[string]ECConfigEntry
)

// SetGlobalECConfig replaces the global default. Callers persist afterwards.
func SetGlobalECConfig(dataShards, parityShards int) error {
	entry := ECConfigEntry{DataShards: dataShards, ParityShards: parityShards}
	if err := entry.Validate(); err != nil {
		return err
	}
	ecConfigMu.Lock()
	defer ecConfigMu.Unlock()
	globalECConfig = &entry
	return nil
}

// SetCollectionECConfig installs (or replaces) a collection's override.
func SetCollectionECConfig(collection string, dataShards, parityShards int) error {
	if collection == "" {
		return fmt.Errorf("collection name is required for a collection override")
	}
	entry := ECConfigEntry{DataShards: dataShards, ParityShards: parityShards}
	if err := entry.Validate(); err != nil {
		return err
	}
	ecConfigMu.Lock()
	defer ecConfigMu.Unlock()
	if collectionConfigs == nil {
		collectionConfigs = make(map[string]ECConfigEntry)
	}
	collectionConfigs[collection] = entry
	return nil
}

// DeleteCollectionECConfig drops a collection's override, so it falls back to
// the global default. Deleting an absent override is not an error: the caller
// asked for the post-state, which holds.
func DeleteCollectionECConfig(collection string) error {
	if collection == "" {
		return fmt.Errorf("collection name is required")
	}
	ecConfigMu.Lock()
	defer ecConfigMu.Unlock()
	delete(collectionConfigs, collection)
	return nil
}

// DeleteGlobalECConfig clears the global default (the dashboard's "clear", and
// how an operator returns a cluster to the build default). Not an error when it
// is already unset.
func DeleteGlobalECConfig() {
	ecConfigMu.Lock()
	defer ecConfigMu.Unlock()
	globalECConfig = nil
}

// GetECConfig resolves the policy for a collection: the collection's override,
// else the global default, else nil. A non-nil result is a private copy that the
// caller may mutate (the encode path stamps BlockSize on the context it hands to
// the encoder).
func GetECConfig(collection string) *ECContext {
	entry, found := ResolveECConfig(collection)
	if !found {
		return nil
	}
	return &ECContext{
		Collection:   collection,
		DataShards:   entry.DataShards,
		ParityShards: entry.ParityShards,
	}
}

// ResolveECConfig is GetECConfig without the encode-time wrapper: the raw pair
// plus whether any policy applied, for display and for callers that only need
// the numbers.
func ResolveECConfig(collection string) (ECConfigEntry, bool) {
	ecConfigMu.RLock()
	defer ecConfigMu.RUnlock()
	if collection != "" {
		if entry, found := collectionConfigs[collection]; found {
			return entry, true
		}
	}
	if globalECConfig != nil {
		return *globalECConfig, true
	}
	return ECConfigEntry{}, false
}

// GlobalECConfig returns the global default, if one is set.
func GlobalECConfig() (ECConfigEntry, bool) {
	ecConfigMu.RLock()
	defer ecConfigMu.RUnlock()
	if globalECConfig == nil {
		return ECConfigEntry{}, false
	}
	return *globalECConfig, true
}

// CollectionECConfig is one override, for display.
type CollectionECConfig struct {
	Collection string
	ECConfigEntry
}

// CollectionECConfigs returns a sorted snapshot of the overrides.
func CollectionECConfigs() []CollectionECConfig {
	ecConfigMu.RLock()
	defer ecConfigMu.RUnlock()
	res := make([]CollectionECConfig, 0, len(collectionConfigs))
	for collection, entry := range collectionConfigs {
		res = append(res, CollectionECConfig{Collection: collection, ECConfigEntry: entry})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Collection < res[j].Collection })
	return res
}

// ECConfigToJSON snapshots the policy for persistence.
func ECConfigToJSON() *ECConfigJSON {
	ecConfigMu.RLock()
	defer ecConfigMu.RUnlock()
	doc := &ECConfigJSON{}
	if globalECConfig != nil {
		entry := *globalECConfig
		doc.Global = &entry
	}
	if len(collectionConfigs) > 0 {
		doc.Collections = make(map[string]*ECConfigEntry, len(collectionConfigs))
		for collection, entry := range collectionConfigs {
			copied := entry
			doc.Collections[collection] = &copied
		}
	}
	return doc
}

// MarshalECConfigJSON renders the policy as the document persisted in the filer.
// A policy with nothing set marshals to "{}".
func MarshalECConfigJSON() ([]byte, error) {
	doc, err := json.MarshalIndent(ECConfigToJSON(), "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal EC config to JSON: %w", err)
	}
	return doc, nil
}

// ApplyECConfigJSON replaces the in-memory policy with the document's contents.
// It validates everything first: a document with one bad entry is rejected as a
// whole rather than half-applied, because a partially loaded policy is worse
// than the previous one — it would encode some volumes with a layout the
// operator never chose.
func ApplyECConfigJSON(data []byte) error {
	doc := &ECConfigJSON{}
	if err := json.Unmarshal(data, doc); err != nil {
		return fmt.Errorf("unmarshal EC config JSON: %w", err)
	}
	return ApplyECConfigDocument(doc)
}

// ApplyECConfigDocument is ApplyECConfigJSON for an already-decoded document.
func ApplyECConfigDocument(doc *ECConfigJSON) error {
	if doc == nil {
		doc = &ECConfigJSON{}
	}
	if doc.Global != nil {
		if err := doc.Global.Validate(); err != nil {
			return fmt.Errorf("global EC ratio: %w", err)
		}
	}
	for collection, entry := range doc.Collections {
		if collection == "" {
			return fmt.Errorf("collection override has an empty collection name")
		}
		if entry == nil {
			return fmt.Errorf("collection %q has no ratio", collection)
		}
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("collection %q: %w", collection, err)
		}
	}

	ecConfigMu.Lock()
	defer ecConfigMu.Unlock()
	globalECConfig = nil
	if doc.Global != nil {
		entry := *doc.Global
		globalECConfig = &entry
	}
	collectionConfigs = nil
	if len(doc.Collections) > 0 {
		collectionConfigs = make(map[string]ECConfigEntry, len(doc.Collections))
		for collection, entry := range doc.Collections {
			collectionConfigs[collection] = *entry
		}
	}
	return nil
}

// ResetECConfig clears the policy. Tests and the "no document in the filer"
// path need it; production callers install whatever the filer holds.
func ResetECConfig() {
	ecConfigMu.Lock()
	defer ecConfigMu.Unlock()
	globalECConfig = nil
	collectionConfigs = nil
}
