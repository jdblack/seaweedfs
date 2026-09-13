package shell

import (
	"strings"
	"testing"
)

// TestEcVacuumCommandRegistered checks the ec.vacuum command is wired into the
// shell and advertises itself as a heavy, non-mutating-by-default command.
func TestEcVacuumCommandRegistered(t *testing.T) {
	var found *commandEcVacuum
	for _, c := range Commands {
		if c.Name() == "ec.vacuum" {
			found, _ = c.(*commandEcVacuum)
			break
		}
	}
	if found == nil {
		t.Fatalf("ec.vacuum command must be registered in Commands")
	}
	if !found.HasTag(ResourceHeavy) {
		t.Fatalf("ec.vacuum must be tagged resource heavy")
	}
	help := found.Help()
	for _, flagName := range []string{"garbageThreshold", "volumeId", "collection", "-apply"} {
		if !strings.Contains(help, flagName) {
			t.Errorf("help must document %q", flagName)
		}
	}
}
