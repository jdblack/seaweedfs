package ec_vacuum

import "github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"

func vacuumOutputs(volumeID uint32, reclaimedBytes, newShardBytes int64) map[string]*plugin_pb.ConfigValue {
	return map[string]*plugin_pb.ConfigValue{
		"volume_id":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)}},
		"bytes_reclaimed": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: reclaimedBytes}},
		"new_shard_bytes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: newShardBytes}},
	}
}

// decodeOutputs reports a decode, which has no byte figures to give: the volume
// servers do the work, so the worker never sees the sizes.
func decodeOutputs(volumeID uint32) map[string]*plugin_pb.ConfigValue {
	return map[string]*plugin_pb.ConfigValue{
		"volume_id": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)}},
		"action":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: modeDecode}},
	}
}
