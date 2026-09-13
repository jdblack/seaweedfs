package ec_vacuum

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// encodeNeedleIds packs needle ids into the raw 8-byte-per-entry .ecj layout.
func encodeNeedleIds(ids ...uint64) []byte {
	buf := make([]byte, 0, len(ids)*types.NeedleIdSize)
	b := make([]byte, types.NeedleIdSize)
	for _, id := range ids {
		types.NeedleIdToBytes(b, types.NeedleId(id))
		buf = append(buf, b...)
	}
	return buf
}

// TestClusterTransportCollectMergesEveryHoldersEcj proves the delete journal is
// merged from every shard holder. A delete is tombstoned on exactly one holder
// (the needle's primary data-shard owner), so the per-holder .ecj journals are
// disjoint; folding only one holder's journal would leave the rest of the
// volume's deletes applied to the shards but invisible to the re-encode, and the
// volume would never shrink by more than one holder's worth.
func TestClusterTransportCollectMergesEveryHoldersEcj(t *testing.T) {
	const (
		collection = "c1"
		vid        = uint32(7)
	)
	base := shardBaseName(collection, vid)

	holderA := pluginworkers.NewVolumeServer(t, "")
	holderB := pluginworkers.NewVolumeServer(t, "")

	journalA := encodeNeedleIds(101, 102)
	journalB := encodeNeedleIds(201, 202, 203)
	require.NoError(t, os.WriteFile(filepath.Join(holderA.BaseDir(), fmt.Sprintf("%d.ecj", vid)), journalA, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(holderB.BaseDir(), fmt.Sprintf("%d.ecj", vid)), journalB, 0o644))

	transport := newClusterTransport(grpc.WithTransportCredentials(insecure.NewCredentials()))
	target := vacuumTarget{
		VolumeID:     vid,
		Collection:   collection,
		DataShards:   1,
		ParityShards: 1,
		// Shard 0 on holderA, shard 1 on holderB: distinct journals per holder.
		Holders: map[uint32]string{0: holderA.Address(), 1: holderB.Address()},
	}

	workDir := t.TempDir()
	_, err := transport.Collect(context.Background(), target, workDir, base)
	require.NoError(t, err)

	merged, err := os.ReadFile(filepath.Join(workDir, base+".ecj"))
	require.NoError(t, err)
	require.Equal(t, append(append([]byte{}, journalA...), journalB...), merged,
		"the .ecj from every shard holder must be merged, not just one holder's")
}

// stageEcFilesOnFake copies a fixture's EC artifact set into the fake volume
// server's directory under the <vid><ext> names the fake serves (the fake keys
// files by volume id, not collection_vid).
func stageEcFilesOnFake(t *testing.T, fixtureDir, base, fakeBaseDir string, vid uint32) {
	t.Helper()
	entries, err := os.ReadDir(fixtureDir)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, base) {
			continue
		}
		if strings.HasSuffix(name, ".dat") || strings.HasSuffix(name, ".idx") {
			continue
		}
		ext := strings.TrimPrefix(name, base)
		data, err := os.ReadFile(filepath.Join(fixtureDir, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(fakeBaseDir, fmt.Sprintf("%d%s", vid, ext)), data, 0o644))
	}
}

// TestClusterTransportCollectAndRedistribute drives the real clusterTransport
// against the shared fake volume server: Collect streams every shard plus the
// sidecars into the worker dir, the volume is compacted locally, and
// Redistribute pushes the compacted shards back and mounts them.
func TestClusterTransportCollectAndRedistribute(t *testing.T) {
	const (
		collection = "c1"
		vid        = uint32(1)
	)

	fixtureDir := t.TempDir()
	buildEncodedFixture(t, fixtureDir, collection, vid, 20, 10) // 20 needles, 10 deleted
	base := shardBaseName(collection, vid)

	fake := pluginworkers.NewVolumeServer(t, "")
	stageEcFilesOnFake(t, fixtureDir, base, fake.BaseDir(), vid)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	transport := newClusterTransport(dialOption)

	target := vacuumTarget{
		VolumeID:     vid,
		Collection:   collection,
		DataShards:   10,
		ParityShards: 4,
		Holders:      make(map[uint32]string),
	}
	for i := 0; i < 14; i++ {
		target.Holders[uint32(i)] = fake.Address()
	}

	workDir := t.TempDir()
	ctx := context.Background()

	collected, err := transport.Collect(ctx, target, workDir, base)
	require.NoError(t, err)
	require.Len(t, collected, 14, "every shard must be collected")
	for i := 0; i < 14; i++ {
		require.FileExists(t, filepath.Join(workDir, fmt.Sprintf("%s.ec%02d", base, i)))
	}
	require.FileExists(t, filepath.Join(workDir, base+".ecx"))
	require.FileExists(t, filepath.Join(workDir, base+".vif"))
	require.FileExists(t, filepath.Join(workDir, base+".ecj"), "the delete journal must travel with the shards")

	opts, err := loadVacuumOptions(workDir, base, target)
	require.NoError(t, err)
	vacRes, err := vacuumLocalDir(workDir, base, opts)
	require.NoError(t, err)
	require.Greater(t, vacRes.OldShardBytes, vacRes.NewShardBytes)

	require.NoError(t, transport.Redistribute(ctx, target, workDir, base, true))

	require.NotEmpty(t, fake.MountRequests(), "compacted shards must be mounted on the holder")

	received := fake.ReceivedFiles()
	require.NotEmpty(t, received)
	// The compacted shard 0 must have been pushed back to the holder verbatim.
	localShard0 := filepath.Join(workDir, base+".ec00")
	fi, err := os.Stat(localShard0)
	require.NoError(t, err)
	require.EqualValues(t, fi.Size(), received[filepath.Join(fake.BaseDir(), fmt.Sprintf("%d.ec00", vid))])
	// The vacuum's success push must overwrite each holder's delete journal with
	// an empty one, so a spent delete is not reported as still pending.
	require.EqualValues(t, 0, received[filepath.Join(fake.BaseDir(), fmt.Sprintf("%d.ecj", vid))],
		"the compacted push must clear the holder's delete journal")
}
