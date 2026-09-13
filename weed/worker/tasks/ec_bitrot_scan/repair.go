package ec_bitrot_scan

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	storagetypes "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
)

// repairer performs the destructive half of an auto-repair run. It is an
// interface so unit tests can assert the quarantine/rebuild calls without a
// live cluster; the production implementation drives the volume-server RPCs the
// same way the "ec.shard.unmount --delete" and "ec.rebuild" shell commands do.
type repairer interface {
	// Quarantine unmounts and deletes the given shard copies on their holders.
	// It returns the number of shards actually quarantined.
	Quarantine(ctx context.Context, volumeID uint32, refs []ec.ShardRef) (int, error)
	// Rebuild regenerates the missing shards for a volume so Reed-Solomon
	// restores a clean, parity-verified copy.
	Rebuild(ctx context.Context, topo *master_pb.TopologyInfo, masters []string, collection string, volumeID uint32, diskType string) error
}

// clusterRepairer is the production repairer.
type clusterRepairer struct {
	dialOpt grpc.DialOption
}

func newClusterRepairer(dialOpt grpc.DialOption) *clusterRepairer {
	return &clusterRepairer{dialOpt: dialOpt}
}

// Quarantine mirrors ec.shard.unmount --delete: unmount the shard on its holder
// (so serving stops) then delete the shard file, so ec.rebuild treats it as
// missing and regenerates it.
func (r *clusterRepairer) Quarantine(ctx context.Context, volumeID uint32, refs []ec.ShardRef) (int, error) {
	done := 0
	for _, ref := range refs {
		ref := ref
		err := operation.WithVolumeServerClient(false, pb.ServerAddress(ref.NodeAddress), r.dialOpt,
			func(client volume_server_pb.VolumeServerClient) error {
				if _, err := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
					VolumeId: volumeID,
					ShardIds: []uint32{ref.ShardID},
				}); err != nil {
					return fmt.Errorf("unmount shard %d@%s: %w", ref.ShardID, ref.NodeAddress, err)
				}
				if _, err := client.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
					VolumeId:   volumeID,
					Collection: ref.Collection,
					ShardIds:   []uint32{ref.ShardID},
				}); err != nil {
					return fmt.Errorf("delete shard %d@%s: %w", ref.ShardID, ref.NodeAddress, err)
				}
				return nil
			})
		if err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

// Rebuild reuses the shared ec.RebuildEcVolumes planner (the same code behind
// the "ec.rebuild" shell command) so placement and RPC sequencing cannot drift
// from the operator-facing tool. The Env is caller-decoupled; the worker has no
// cluster admin lock, so IsLocked reports true, matching how other worker tasks
// (e.g. ec_balance) mutate shards via RPCs.
func (r *clusterRepairer) Rebuild(ctx context.Context, topo *master_pb.TopologyInfo, masters []string, collection string, volumeID uint32, diskType string) error {
	dt := storagetypes.ToDiskType(diskType)
	ecNodes, _ := ec.CollectEcVolumeServersByDc(topo, "", dt)
	if len(ecNodes) == 0 {
		return fmt.Errorf("no EC nodes found to rebuild volume %d", volumeID)
	}

	env := &ec.Env{
		GrpcDialOption: r.dialOpt,
		FetchTopology: func(delay time.Duration) (*master_pb.TopologyInfo, uint64, error) {
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, 0, ctx.Err()
				}
			}
			return fetchTopologyFromMasters(ctx, masters, r.dialOpt)
		},
		IsLocked: func() bool { return true },
	}

	return ec.RebuildEcVolumes(env, ecNodes, io.Discard,
		[]string{collection}, []needle.VolumeId{needle.VolumeId(volumeID)}, dt, 1, true)
}
