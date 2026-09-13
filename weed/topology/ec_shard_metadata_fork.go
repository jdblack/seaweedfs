package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// Fork-local addition (not present in upstream SeaweedFS). Kept in its own file
// so upstream edits to data_node_ec.go do not conflict with it.

// ecShardMetadataChanged reports whether any incoming EC shard entry differs
// from the one already tracked on the node in a field the master serves:
// FileCount, DeleteCount, EncodeTsNs, DataShards or ParityShards.
//
// The full EC heartbeat is the only carrier of a volume's metadata, and the shard
// bitmap does not change when only that metadata does — e.g. an ec.vacuum that
// unmounts and remounts the same shard ids. DataNode.UpdateEcShards applies the
// incoming list whenever this is true, or the master keeps serving stale counts
// and the vacuum's garbage detection reads a deleted-ratio of 0.
func ecShardMetadataChanged(existingByKey, actualByKey map[ecShardKey]*erasure_coding.EcVolumeInfo) bool {
	for key, actual := range actualByKey {
		existing, ok := existingByKey[key]
		if !ok {
			continue
		}
		if existing.FileCount != actual.FileCount ||
			existing.DeleteCount != actual.DeleteCount ||
			existing.EncodeTsNs != actual.EncodeTsNs ||
			existing.DataShards != actual.DataShards ||
			existing.ParityShards != actual.ParityShards {
			return true
		}
	}
	return false
}
