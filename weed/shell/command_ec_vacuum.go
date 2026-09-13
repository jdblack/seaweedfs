package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/ec/ecvacuum"
)

func init() {
	Commands = append(Commands, &commandEcVacuum{})
}

type commandEcVacuum struct {
}

func (c *commandEcVacuum) Name() string {
	return "ec.vacuum"
}

func (c *commandEcVacuum) Help() string {
	return `reclaim space by removing deleted needles from erasure-coded volumes

	ec.vacuum [-collection=<name>] [-volumeId=<id>] [-garbageThreshold=0.30] [-diskType=<type>] [-workingDir=<dir>] [-timeout=1h] [-maxParallelization=1] [-apply]

	An EC volume's .ecx is a sealed index written at encode time; runtime deletes
	are appended to a per-holder .ecj journal and masked out at read time, so the
	bytes of deleted needles stay allocated in the shards until the volume is
	re-encoded. This command finds EC volumes whose deleted-needle ratio has
	crossed the threshold, collects each volume's shards onto this machine,
	decodes them into a normal volume, compacts the deleted needles away,
	re-encodes a fresh shard set, and distributes it back -- so the volume servers
	never carry the compaction CPU or disk cost.

	This is the shell equivalent of the ec_vacuum plugin worker; both use the same
	core. Simulation is the default: nothing is changed unless -apply is given.

	The -collection parameter supports comma-separated names, wildcards, or regex
	patterns; empty matches every collection. Use _default for the collection with
	no name.

	Options:
	  -garbageThreshold: vacuum a volume once its deleted-needle ratio reaches
	    this fraction (0-1). Default 0.30 = 30%. Use 0 to consider every volume.
	  -volumeId: only vacuum this EC volume id (0 = all matching).
	  -diskType: only vacuum EC shards on this disk type (e.g. hdd, ssd; empty = all).
	  -workingDir: scratch directory for collect/compact/re-encode (default: the
	    system temp dir). Each volume needs room for its shards plus a decoded
	    copy while it is being vacuumed.
	  -timeout: bound one volume's collect -> compact -> redistribute cycle (0 = none).

`
}

func (c *commandEcVacuum) HasTag(tag CommandTag) bool {
	return tag == ResourceHeavy
}

func (c *commandEcVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	vacuumCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := vacuumCommand.String("collection", "", "comma-separated collection names, wildcards, or regex patterns; empty matches the collection with no name")
	volumeId := vacuumCommand.Uint("volumeId", 0, "the volume id (0 for all)")
	garbageThreshold := vacuumCommand.Float64("garbageThreshold", 0.30, "vacuum a volume when its deleted-needle ratio reaches this fraction (0-1)")
	diskTypeStr := vacuumCommand.String("diskType", "", "only vacuum EC shards on this disk type (empty = all)")
	workingDir := vacuumCommand.String("workingDir", "", "scratch directory for collect/compact/re-encode (default: system temp dir)")
	timeout := vacuumCommand.Duration("timeout", time.Hour, "bound one volume's vacuum cycle (0 = no bound)")
	applyChanges := vacuumCommand.Bool("apply", false, "apply the vacuum")
	// TODO: remove this alias
	applyChangesAlias := vacuumCommand.Bool("force", false, "apply the vacuum (alias for -apply)")
	maxParallelization := vacuumCommand.Int("maxParallelization", 1, "run up to X volumes in parallel")

	if err = vacuumCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, vacuumCommand, applyChangesAlias, applyChanges)
	infoAboutSimulationMode(writer, *applyChanges, "-apply")

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	// A specific -volumeId is an explicit target: consider it even when its
	// reported deleted ratio is under -garbageThreshold, because the ratio the
	// master reports is derived from the volume servers' mounted state and can lag
	// (or understate) the .ecj journal that volume.fsck reads.
	thresholdForEnumeration := *garbageThreshold
	if *volumeId != 0 {
		thresholdForEnumeration = 0
	}

	candidates, err := ecvacuum.EnumerateGarbageEcVolumes(topologyInfo, *collection, thresholdForEnumeration)
	if err != nil {
		return err
	}
	if *diskTypeStr != "" {
		candidates = ecvacuum.FilterCandidatesByDiskType(candidates, *diskTypeStr)
	}
	if vid := uint32(*volumeId); vid != 0 {
		filtered := candidates[:0:0]
		for _, candidate := range candidates {
			if candidate.VolumeID == vid {
				filtered = append(filtered, candidate)
			}
		}
		if len(filtered) == 0 {
			return fmt.Errorf("EC volume %d is not a vacuum candidate (it may be missing a data shard, span encode generations, or be under the %.0f%% deleted threshold)", vid, *garbageThreshold*100)
		}
		candidates = filtered
	}

	if len(candidates) == 0 {
		fmt.Fprintf(writer, "no EC volume at or above %.0f%% deleted for the selected collections\n", *garbageThreshold*100)
		return nil
	}

	fmt.Fprintf(writer, "EC volumes to vacuum (deleted ratio >= %.0f%%):\n", *garbageThreshold*100)
	for _, candidate := range candidates {
		fmt.Fprintf(writer, "  volume %d collection %q deleted=%d/%d (%.1f%%) shards=%d/%d\n",
			candidate.VolumeID, candidate.Collection, candidate.DeleteCount, candidate.FileCount,
			candidate.DeletedRatio()*100, candidate.PresentShardCount(), candidate.DataShards+candidate.ParityShards)
	}

	if !*applyChanges {
		fmt.Fprintf(writer, "simulation: re-run with -apply to vacuum the %d volume(s) above\n", len(candidates))
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	if *maxParallelization < 1 {
		*maxParallelization = 1
	}

	var (
		writeMu  sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		sem      = make(chan struct{}, *maxParallelization)
	)

	say := func(format string, a ...any) {
		writeMu.Lock()
		defer writeMu.Unlock()
		fmt.Fprintf(writer, format, a...)
	}

	for _, candidate := range candidates {
		candidate := candidate
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if err := vacuumOneEcVolume(commandEnv, candidate, *workingDir, *timeout, say); err != nil {
				say("volume %d: %v\n", candidate.VolumeID, err)
				writeMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				writeMu.Unlock()
			}
		}()
	}
	wg.Wait()

	return firstErr
}

// vacuumOneEcVolume vacuums one candidate volume using the shared vacuum core.
// The topology is re-read here so a shard that moved since enumeration is found
// (and a volume that is no longer vacuumable fails loudly rather than silently).
func vacuumOneEcVolume(commandEnv *CommandEnv, candidate ecvacuum.Candidate, workingDir string, timeout time.Duration, say func(string, ...any)) error {
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	target, err := ecvacuum.BuildVacuumTarget(topologyInfo, candidate.VolumeID, candidate.Collection, candidate.DiskType)
	if err != nil {
		return err
	}

	transport := ecvacuum.NewClusterTransport(commandEnv.option.GrpcDialOption)
	out, err := ecvacuum.VacuumVolume(context.Background(), transport, target, ecvacuum.RunOptions{
		WorkingDir: workingDir,
		Timeout:    timeout,
		OnStage: func(stage, message string) error {
			say("volume %d: %s: %s\n", candidate.VolumeID, stage, message)
			return nil
		},
	})
	if err != nil {
		return err
	}
	if out.Skipped {
		say("volume %d: no live entries; nothing to vacuum\n", candidate.VolumeID)
		return nil
	}
	say("volume %d: vacuumed, %d -> %d shard bytes (reclaimed %d)\n",
		candidate.VolumeID, out.OldShardBytes, out.NewShardBytes, out.Reclaimed)
	return nil
}
