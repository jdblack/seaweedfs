package ec_vacuum

import "github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"

func vacuumOutputs(volumeID uint32, reclaimedBytes, newShardBytes int64) map[string]*plugin_pb.ConfigValue {
	return map[string]*plugin_pb.ConfigValue{
		"volume_id":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)}},
		"bytes_reclaimed": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: reclaimedBytes}},
		"new_shard_bytes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: newShardBytes}},
	}
}
