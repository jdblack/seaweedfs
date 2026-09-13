package ec

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/assert"
)

// TestEcVolumeShardRatioFromTopology covers the helper the encode/decode/rebuild
// paths use to stop assuming 10+4: a volume's own ratio is honored, and an
// unset/absent one reads as the build default.
func TestEcVolumeShardRatioFromTopology(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			RackInfos: []*master_pb.RackInfo{{
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id: "n1",
					DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {
							Type: "hdd",
							EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
								{Id: 100, EcIndexBits: 0b11111, DataShards: 3, ParityShards: 2},
								{Id: 200, EcIndexBits: 0x3fff},
							},
						},
					},
				}},
			}},
		}},
	}

	custom, parity := ecVolumeShardRatio(topo, needle.VolumeId(100))
	assert.Equal(t, 3, custom)
	assert.Equal(t, 2, parity)

	unset, parity := ecVolumeShardRatio(topo, needle.VolumeId(200))
	assert.Equal(t, erasure_coding.DataShardsCount, unset)
	assert.Equal(t, erasure_coding.ParityShardsCount, parity)

	absent, parity := ecVolumeShardRatio(topo, needle.VolumeId(999))
	assert.Equal(t, erasure_coding.DataShardsCount, absent)
	assert.Equal(t, erasure_coding.ParityShardsCount, parity)
}
