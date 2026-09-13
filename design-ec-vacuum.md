# EC vacuum — design notes

EC vacuum reclaims the space that deleted needles still occupy in erasure-coded
volumes. EC has no separate delete: a delete is recorded in each holder's `.ecj`
journal, and the needle's bytes stay in the shards until the volume is
re-encoded. Vacuum does that re-encode offline, in a worker, without the volume
server paying the CPU or disk cost.

## Where the code lives

| Piece | Location |
|---|---|
| Shared core: detection, transport, local compaction | `weed/ec/ecvacuum` |
| Worker task (scheduler-driven) | `weed/worker/tasks/ec_vacuum` |
| Operator command | `weed/shell/command_ec_vacuum.go` |
| Metrics | `weed/stats/metrics_ec_vacuum.go` |

`weed/ec/ecvacuum` deliberately depends on neither `weed/plugin/worker` nor
`weed/shell`, so both callers can share one implementation without an import
cycle. The worker and the shell command are thin adapters over it.

## Flow (one volume)

1. **Detect** — `EnumerateGarbageEcVolumes` walks the master topology, derives
   each EC volume's deleted ratio from its `.ecx` and `.ecj`, and proposes the
   volumes above the threshold.
2. **Collect** — `ClusterTransport.Collect` fetches every shard plus the
   `.ecx`/`.ecj`/`.vif`/`.ecsum` sidecars from the holders into
   `<workingDir>/orig`.
3. **Compact** — `VacuumLocalDir` copies the set into `<workingDir>/work`,
   decodes the shards into a normal volume, folds the journals in, and compacts
   it with `Store.CompactVolumeFiles`.
4. **Re-encode** — a fresh shard generation is written with the same ratio, plus
   a new `.ecx` and a fresh `.vif` carrying a new encode timestamp so stale
   in-flight shards are rejected.
5. **Verify** — the new shards are decoded back and CRC-compared against the
   compacted `.dat` before anything is distributed.
6. **Redistribute** — the new generation is pushed back to the holders. If the
   push fails partway, the collected originals are pushed back (rollback) so the
   volume is never left mixed-generation.

`<workingDir>/ec_vacuum_<collection>_<id>` is removed when the step ends, success
or failure (`run.go`).

## The scratch directory and DiskLocation

Offline compaction needs a `*storage.DiskLocation`, because
`Store.CompactVolumeFiles` takes one. `storage` offers two constructors:

- `NewDiskLocation` — the **volume server's** constructor. It starts a goroutine
  that statfs's the directory immediately and then every 60 seconds until
  `DiskLocation.Close()`, and it writes `<dir>/vol_dir.uuid`. That lifetime is
  right for a volume server, whose locations live as long as the process.
- `NewScratchDiskLocation` — fork-local, in
  `weed/storage/disk_location_scratch_fork.go`. Resolves the paths, records the
  disk type, starts nothing, writes nothing.

EC vacuum uses the scratch constructor. The directory it compacts is a working
copy deleted as soon as the vacuum finishes, so a prober pointed at it would
report the removed directory as a dead disk on every tick for the remaining life
of the worker, pinning `volumeServer_disk_error_status{<dir>,type="error"} = 1`
on its `/metrics` — a permanently false disk-error signal, plus one leaked
goroutine and five metric series per vacuumed volume. Building the location
without the prober removes the goroutine and the series at the source, instead of
cleaning up after a constructor whose lifetime was never intended for scratch.

## Reuse, not reimplementation

Every step uses the primitives the volume server uses: `erasure_coding` for
decode/encode, `Store.CompactVolumeFiles` for compaction, `volume_info` for the
`.vif`. The vacuum never invents its own shard byte layout, so a shard it
produces is byte-compatible with one produced by `ec.encode`.

## Metrics

Definitions and registration live together in
`weed/stats/metrics_ec_vacuum.go`, leaving upstream `metrics.go` untouched.
Families: jobs detected / executed / failed / skipped, bytes reclaimed, rebuild
duration, verify failures, rollbacks, last-success timestamp.

## Tests

- `weed/ec/ecvacuum/vacuum_test.go` — decode/compact/re-encode, the
  no-live-needles sentinel, and the scratch-probe regression.
- `weed/storage/disk_location_scratch_fork_test.go` — the scratch constructor
  starts no prober, writes no `vol_dir.uuid`, and closes safely.
- `weed/worker/tasks/ec_vacuum/` — scheduling, detection, transport and metrics.

## Merge hygiene

See `FORK.md`. Short version: new behavior in new files, minimal hooks in
upstream files, `// fork:` markers, generated code regenerated rather than
hand-merged.
