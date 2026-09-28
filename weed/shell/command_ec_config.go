package shell

import (
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func init() {
	Commands = append(Commands, &commandEcConfig{})
}

type commandEcConfig struct {
}

func (c *commandEcConfig) Name() string {
	return "ec.config"
}

// Help matches the enterprise edition's `ec.config` wording, including the note
// that the policy applies to new volumes only.
func (c *commandEcConfig) Help() string {
	return `Manage erasure coding ratios. The ratio is a cluster policy stored in the filer:
one global default plus per-collection overrides. A volume that already records a
ratio in its .vif keeps it, so changes apply to volumes encoded afterwards.

Usage:
  ec.config -get                                  # View current global and collection-specific EC ratios
  ec.config -set [-collection=""] -dataShards=<N> -parityShards=<M> # Set global default or collection-specific EC ratio
  ec.config -delete -collection="<collection_name>" # Delete collection-specific EC ratio, falling back to global default

Examples:
  ec.config -get
  ec.config -set -dataShards=3 -parityShards=2
  ec.config -set -collection="blender-blender" -dataShards=5 -parityShards=1
  ec.config -delete -collection="blender-blender"

Notes:
  - Total shards (dataShards + parityShards) must be <= 32
  - Both dataShards and parityShards must be positive
  - Changes apply to NEW EC volumes only (existing volumes keep their configuration)
`
}

func (c *commandEcConfig) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcConfig) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	command := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	showConfig := command.Bool("get", false, "show the global and per-collection EC ratios")
	setConfig := command.Bool("set", false, "set the global default, or a collection override with -collection")
	deleteConfig := command.Bool("delete", false, "delete a collection override, falling back to the global default")
	collection := command.String("collection", "", "collection name for a per-collection override")
	dataShards := command.Int("dataShards", 0, "data shards")
	parityShards := command.Int("parityShards", 0, "parity shards")
	if err := command.Parse(args); err != nil {
		return err
	}

	// Validate the arguments before touching the filer: a typo should not depend
	// on the cluster being reachable, and it must never leave a half-edit.
	switch {
	case *showConfig:
		if *setConfig || *deleteConfig {
			return fmt.Errorf("-get cannot be combined with -set or -delete")
		}
	case *setConfig:
		if *deleteConfig {
			return fmt.Errorf("-set and -delete are mutually exclusive")
		}
		entry := erasure_coding.ECConfigEntry{DataShards: *dataShards, ParityShards: *parityShards}
		if err := entry.Validate(); err != nil {
			return err
		}
	case *deleteConfig:
		if *collection == "" {
			return fmt.Errorf("-delete requires -collection")
		}
	default:
		return fmt.Errorf("specify one of -get, -set or -delete")
	}

	// Both writers edit the document the filer holds (as the enterprise edition's
	// ec.config does), so a change never drops entries it does not mention.
	if err := ec.LoadECConfigFromFiler(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress); err != nil {
		return err
	}

	switch {
	case *showConfig:
		printECConfig(writer)
		return nil
	case *setConfig:
		var err error
		if *collection == "" {
			err = erasure_coding.SetGlobalECConfig(*dataShards, *parityShards)
		} else {
			err = erasure_coding.SetCollectionECConfig(*collection, *dataShards, *parityShards)
		}
		if err != nil {
			return err
		}
		if err := ec.SaveECConfigToFiler(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress); err != nil {
			return err
		}
		printECConfig(writer)
		return nil
	default:
		if err := erasure_coding.DeleteCollectionECConfig(*collection); err != nil {
			return err
		}
		if err := ec.SaveECConfigToFiler(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress); err != nil {
			return err
		}
		printECConfig(writer)
		return nil
	}
}

// printECConfig shows the resolved policy, and says so plainly when there is
// none — "no policy" is a real state (the build default applies), not an error.
func printECConfig(writer io.Writer) {
	global, hasGlobal := erasure_coding.GlobalECConfig()
	fmt.Fprintf(writer, "Global default: %s\n", ecConfigLine(global, hasGlobal))

	overrides := erasure_coding.CollectionECConfigs()
	if len(overrides) == 0 {
		fmt.Fprintf(writer, "Collection overrides: none\n")
	} else {
		fmt.Fprintf(writer, "Collection overrides:\n")
		for _, override := range overrides {
			fmt.Fprintf(writer, "  %s: %d+%d\n", override.Collection, override.DataShards, override.ParityShards)
		}
	}
	if !hasGlobal && len(overrides) == 0 {
		fmt.Fprintf(writer, "No EC policy is set: new volumes use the build default %d+%d.\n",
			erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	}
	fmt.Fprintf(writer, "Applies to new EC volumes only; existing volumes keep the ratio in their .vif.\n")
}

func ecConfigLine(entry erasure_coding.ECConfigEntry, found bool) string {
	if !found {
		return fmt.Sprintf("(unset: build default %d+%d)",
			erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	}
	return fmt.Sprintf("%d+%d", entry.DataShards, entry.ParityShards)
}
