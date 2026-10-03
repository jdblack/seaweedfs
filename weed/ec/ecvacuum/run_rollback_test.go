package ecvacuum

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// rollbackProbeTransport stages the collected shards from a prebuilt fixture and
// fails the first push, so VacuumVolume's rollback path runs; it records the
// context that rollback was handed.
type rollbackProbeTransport struct {
	srcDir         string
	failPushes     int
	rollbackCalled bool
	rollbackCtxErr error
}

func (t *rollbackProbeTransport) Collect(_ context.Context, target VacuumTarget, dir, base string) ([]uint32, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(t.srcDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		// Only the EC artifact set travels; .dat/.idx are not part of a holder's
		// shard inventory.
		switch filepath.Ext(e.Name()) {
		case ".dat", ".idx":
			continue
		}
		data, err := os.ReadFile(filepath.Join(t.srcDir, e.Name()))
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), data, 0o644); err != nil {
			return nil, err
		}
	}
	return target.sortedShardIDs(), nil
}

func (t *rollbackProbeTransport) Redistribute(ctx context.Context, _ VacuumTarget, _, _ string, clearJournal bool) error {
	if !clearJournal {
		t.rollbackCalled = true
		t.rollbackCtxErr = ctx.Err()
		return nil
	}
	if t.failPushes > 0 {
		t.failPushes--
		return fmt.Errorf("induced push failure")
	}
	return nil
}

// TestVacuumVolumeRollsBackOnAFreshContext verifies the rollback does not inherit
// the context the push failed on. A push usually fails *because* that context
// expired or was canceled (the per-volume timeout, the admin canceling the job,
// the worker shutting down), and a dead context would fail every rollback RPC
// instantly — leaving exactly the mixed-generation state the rollback exists to
// prevent.
func TestVacuumVolumeRollsBackOnAFreshContext(t *testing.T) {
	srcDir := t.TempDir()
	buildEncodedFixture(t, srcDir, "c1", 7, 20, 10) // 20 live needles, 10 deleted

	transport := &rollbackProbeTransport{srcDir: srcDir, failPushes: 1}
	target := redistributeTarget(deadHolder, func(uint32) uint32 { return 0 })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the push "fails because" the job was canceled

	_, err := VacuumVolume(ctx, transport, target, RunOptions{WorkingDir: t.TempDir()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rolled back")
	require.True(t, transport.rollbackCalled)
	require.NoError(t, transport.rollbackCtxErr,
		"the rollback must run on a context that is not already canceled")
}
