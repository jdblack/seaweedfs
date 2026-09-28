# EC configuration parity with the Enterprise Edition

Tracking doc for bringing our fork's erasure-coding configuration in line with
SeaweedFS Enterprise (EE). Started 2026-09-28. Companion to `.clinerules/fork-delta.md`
(inventory + merge procedure) — that file must be updated when this lands.

**Reference for EE behavior:** `docker.io/chrislusf/seaweedfs-enterprise:4.47`
(same 4.47 line as our upstream merge-base), inspected via symbols, disassembly
and its embedded protobuf descriptors. Evidence is quoted inline below so this
doc does not depend on that binary later.

## Goal (behavior we are matching)

1. The EC ratio is a **cluster policy**: one global default plus per-collection
   overrides, stored **in the filer**, editable from the admin dashboard and from
   `weed shell`.
2. The policy applies to **new EC volumes only**; volumes that already record a
   ratio keep it (EE's own UI wording).
3. The ratio is **pushed to the volume server explicitly** on the generate RPC.
   The volume server does not read the filer.
4. The in-place mount after generation **follows the layout that was actually
   generated**, per volume (already done in phase 0).

Non-goals: EE's `min_tombstone_age_ns` generate field, EE-only job types
(`ec_repair`, `ec_bitrot_scrub`), and EE's `ec_ratio.pb` protobuf mirror (we store
JSON only — documented deviation).

## EE artifacts we mirror (evidence)

| Artifact | EE value | Source of evidence |
|---|---|---|
| generate request fields | `volume_id=1, collection=2, ec_shard_config=3, min_tombstone_age_ns=4` | EE's embedded `FileDescriptorProto` bytes |
| ratio on EC heartbeat | `data_shards=20, parity_shards=21` | already ours (`weed/pb/master.proto:196-197`) |
| registry | `GetECConfig(name)` → `collectionConfigs` map else `globalECConfig`, under `ecConfigMu` RWMutex; `SetGlobalECConfig` / `SetCollectionECConfig` / `DeleteCollectionECConfig` (`erasure_coding/ec_config.go`) | EE symbols + disassembly |
| persistence | JSON (`erasure_coding.ToJSON` + `json.MarshalIndent`) written with `pb.WithGrpcFilerClient` to a discovered filer; payload names `ec_ratio.pb` / `ec_ratio.json` | EE disassembly of `saveECConfigToFiler`; EE string pool |
| shell UX | `ec.config -get` / `ec.config -set [-collection=""] -dataShards=<N> -parityShards=<M>` / `ec.config -delete -collection=<name>`; "total ≤ 32, both positive, applies to NEW EC volumes only" | EE binary help text, verbatim |
| admin UI | page `/storage/ec-config` ("EC Configuration"), global form + "Add Collection Override", `POST /api/ec/config/global` (+ collection), same "new volumes only" note | EE embedded admin templ |
| volume server | never reads the filer; request config → in-process `GetECConfig` → `.vif` → default | EE disassembly of `VolumeEcShardsGenerate` |

## Work breakdown

Status: ☐ todo · 🚧 in progress · ✅ done (✅ = verified by the listed command)

| # | Phase | Deliverable | Files | Status |
|---|---|---|---|---|
| 0 | Mount follows generated layout | `mountGeneratedEcShards` + `generatedLayoutShardIds`; regression test | `weed/ec/ec_ratio_fork.go`, `weed/ec/ec_encode.go`, `weed/ec/ec_encode_mount_ratio_test.go` | ✅ `go test ./weed/ec/` |
| 1 | EC config store (fork-local) | global + per-collection registry, validation, JSON schema/codec, unit tests | new `weed/storage/erasure_coding/ec_config_fork.go`, `..._test.go` | ✅ `go test -race -run TestECConfig ./weed/storage/erasure_coding/` |
| 2 | Filer persistence | `LoadECConfigFromFiler` / `SaveECConfigToFiler` for `ECConfigPath` (`/etc/seaweedfs/ec_ratio.json`), shared by shell + admin (EE duplicates per caller; we share one helper); bootstraps `/etc/seaweedfs` on a fresh filer | new `weed/ec/ec_config_filer_fork.go`, `..._test.go` | ✅ `go test -run 'TestSaveAndLoadECConfig\|TestLoadECConfig' ./weed/ec/` |
| 3 | Shell command | `ec.config -get/-set/-delete` with EE's flags/help/notes; validates arguments before any filer RPC | new `weed/shell/command_ec_config.go`, `..._test.go` | ✅ `go test -run TestEcConfigCommand ./weed/shell/` |
| 4 | Generate field + Go server | `EcShardConfig ec_shard_config = 3` on the request; server precedence request → `.vif` → default, in a fork-local `resolveGenerateEcLayout` | `weed/pb/volume_server.proto` + regen (byte-identical toolchain: protoc 33.4 / protoc-gen-go v1.36.6), `seaweed-volume/proto/volume_server.proto`, `weed/server/ec_generate_layout_fork.go`, `weed/server/volume_grpc_erasure_coding.go` | ✅ `go test -run TestResolveGenerateEcLayout ./weed/server/` + full `./weed/server/` (121s) |
| 5 | Rust generate handler | honours the request's `ec_shard_config` (validated, error on impossible layouts); `.vif` records the resolved layout. **Parity only — the deployed image is Go**, see "Open items / risks" | `seaweed-volume/src/server/grpc_server.rs` (+ 3 new tests, 5 existing literals given `..Default::default()`) | ✅ `cargo test --lib ec_shards_generate` · full `cargo test --lib` 630 passed |
| 6 | `ec.encode` wiring | policy loaded from the filer once per run; generate request carries it; mount verifies the holder's layout against the policy and refuses a mismatch; capacity pre-flight charges the policy's shard count | `weed/ec/ec_encode.go`, `weed/ec/ec_ratio_fork.go`, `weed/shell/command_ec_encode.go` | ✅ `go test -run 'TestMountGeneratedEcShards\|TestGenerateEcShards\|TestEcLayoutTotalShards' ./weed/ec/` |
| 7 | Worker/admin inheritance | resolution order explicit config → collection policy → build default; worker loads the policy from the filer in its cluster context; job-type defaults become 0 = inherit | `weed/worker/tasks/erasure_coding/{config,plugin_handler,detection,ec_ratio_fork}.go` (+ tests) | ✅ `go test ./weed/worker/tasks/erasure_coding/` |
| 8 | Admin dashboard | `/storage/ec-config` page (global default + collection overrides) + `GET /api/ec/config`, `POST /api/ec/config/{global,global/delete,collection,collection/delete}` + nav entry; the page reloads the policy from the filer on open and after every save | new `weed/admin/dash/ec_config_fork.go`, `weed/admin/handlers/ec_config_handlers.go` (+tests), `weed/admin/view/app/ec_config.templ` (+ regen); route + nav lines in `admin_handlers.go`, `view/layout/layout.templ` (+ regen) | ✅ `go test -run 'TestDecodeECConfigSetRequest\|TestECConfigPageRenders\|TestECConfigForkViewMarshalsApiKeys' ./weed/admin/handlers/` |
| 9 | Bookkeeping | tracker, questions, fork-delta rows, templ-version note; the two hardening tests (worker policy load vs a fake filer; generate-with-request against a real volume server) | `weed/worker/tasks/erasure_coding/ec_policy_sync_test.go`, `test/volume_server/grpc/erasure_coding_test.go` | ✅ `go test ./weed/worker/tasks/erasure_coding/` (192s) · `go test -run TestEcGenerateRequestedShardConfigOverridesStaleVif ./test/volume_server/grpc/` |

Sequencing rationale: phases 1–3 deliver the policy end-to-end with **zero new
upstream surface**; 4–5 add the generation channel; 6–7 apply it; the dashboard
(8) is a pure consumer of the same API and is last because it carries the pinned
templ regeneration risk.

## Fork surface ledger (new upstream files we take on)

| File | Upstream lifetime commits | Why we must touch it |
|---|---|---|
| `weed/pb/volume_server.proto` | 134 | add `ec_shard_config = 3` (EE's field number) |
| `weed/pb/volume_server_pb/volume_server.pb.go` | 181 (generated) | regen only — never hand-merge. Toolchain that reproduces the committed file byte-for-byte: protoc 33.4 + protoc-gen-go v1.36.6 (header records `protoc v6.33.4`) |
| `seaweed-volume/proto/volume_server.proto` | (Rust parity copy) | `weed/pb/Makefile` keeps it in sync (note: the two files had already drifted before this work — the Rust copy lags several upstream fields) |
| `seaweed-volume/src/server/grpc_server.rs` | 77 | Rust generate handler honours the field; 5 existing test literals needed `..Default::default()` |
| `weed/shell/command_ec_encode.go` | (low) | one block: load the EC policy from the filer before a run |
| `weed/server/volume_grpc_erasure_coding.go` | 101 (ours: 5/5) | call the fork-local layout resolver (net −20 lines of logic in this file) |
| `weed/admin/handlers/admin_handlers.go` | 44 | one route line (phase 8) |
| `weed/admin/view/layout/layout.templ` (+ `_templ.go`) | 30 | nav entry (phase 8) |

Already ours, so incremental: `weed/ec/ec_encode.go` (7/4, upstream churn 5),
`weed/worker/tasks/erasure_coding/{config,plugin_handler,detection,ec_ratio_fork}.go`.

Everything else lives in **new fork-local files** (store, JSON codec, filer I/O,
shell command, layout resolver, admin handler + templ, tests).

## Verification

```sh
# Go
go build ./...
go test -count=1 ./weed/ec/ ./weed/storage/erasure_coding/ ./weed/shell/ ./weed/server/ ./weed/admin/dash/...

# Proto (pinned: header records protoc-gen-go v1.36.6 / protoc v7.36.1;
# local toolchain: protoc libprotoc 36.1, protoc-gen-go v1.36.6)
make -C weed/pb gen            # also cp's the .proto into seaweed-volume/proto/
git diff --stat -- weed/pb/volume_server_pb/   # expect field-only churn

# Rust (macOS: cargo 1.98.1, matching deploy_image.sh's rust:1.98.1 builder)
# Parity check only — the fork's image is Go (docker/Dockerfile.local); no deploy
# step builds this, so a skip here changes nothing that ships.
cd seaweed-volume && cargo check          # background log: /tmp/rust-volume-check.log
# linux parity when needed: podman run --rm -v <tree>:/src docker.io/library/rust:1.98.1-alpine3.23 ...

# templ (pinned v0.3.1001, run from weed/admin, one file per invocation)
cd weed/admin && for f in view/app/ec_config.templ view/layout/layout.templ; do templ generate -f $f; done
grep -n '<distinctive string from the .templ edit>' weed/admin/view/app/ec_config_templ.go
```

## Open items / risks

- **Proto regen** is the only piece with real blast radius: pinned versions,
  field-only diff, and the `seaweed-volume/proto` byte-identical copy.
- **The Rust leg is not part of this deployment.** This fork runs the Go volume
  server: `docker/Dockerfile.local` — the Dockerfile `deploy_image.sh` builds —
  contains no Rust at all, and `VOLUME_SERVER_IMPL=rust` appears only in tests and
  in upstream's own CI. `deploy_image.sh` with `WITH_RUST=0` even deletes the
  placeholder rust binaries from the build context (`rm -f "$WORKER_DEST"
  "$VOLUME_DEST"`). Phase 5 exists to keep the *tree* consistent —
  `weed/pb/Makefile` force-copies the proto into `seaweed-volume/proto/`, and the
  Rust struct literals have to compile — not because a deployed binary needs it.
  `cargo check` on macOS (and `podman` + `rust:1.98.1-alpine3.23` for linux, the
  image `deploy_image.sh` already uses) stays a courtesy gate for anyone who has
  a toolchain; nothing in the deploy path runs it.
- **EE's exact filer path is unverified** (its string pool did not expose an EC
  config path under `/etc/seaweedfs/`, unlike `seal.conf`, `tiering/tiering.conf`,
  `remote_events/subscriptions`). We pick `/etc/seaweedfs/ec_ratio.json`,
  following `filer.DirectoryEtcSeaweedFS`. Matching EE exactly only matters if an
  EE process must read our file.
- **Drift semantics**: a volume whose recorded layout differs from the policy is
  *not* re-encoded at the policy ratio (EE: "existing volumes keep their
  configuration"); `ec.encode` warns, and `ec.config -get` / the dashboard show
  policy vs. recorded so mismatches are visible.
- The incident this all started from (3+2 volume, 10+4 mount list) is fixed by
  phase 0; phases 1–9 make the ratio a policy so it cannot recur silently.

## Progress log

**2026-09-28 — phases 0–3 done (uncommitted)**
| change | file |
|---|---|
| ratio-aware in-place mount after generation | `weed/ec/ec_ratio_fork.go` (`mountGeneratedEcShards`, `generatedLayoutShardIds`), call site in `weed/ec/ec_encode.go` |
| EC policy store: global + per-collection, validation, JSON codec | `weed/storage/erasure_coding/ec_config_fork.go` |
| policy I/O on the filer (`/etc/seaweedfs/ec_ratio.json`), dir bootstrap | `weed/ec/ec_config_filer_fork.go` |
| `ec.config -get/-set/-delete` (EE's flags and wording) | `weed/shell/command_ec_config.go` |
| tests | `weed/ec/ec_encode_mount_ratio_test.go`, `weed/storage/erasure_coding/ec_config_fork_test.go`, `weed/ec/ec_config_filer_fork_test.go`, `weed/shell/command_ec_config_test.go` |

Verification run: `gofmt -l` clean on all of the above ·
`go build ./...` ok · `go test -count=1 ./weed/ec/ ./weed/storage/erasure_coding/ ./weed/shell/`
all ok · `go test -race -run TestECConfig ./weed/storage/erasure_coding/` ok.

Rust toolchain baseline: **`cargo check` in `seaweed-volume/` passes on macOS**
(cargo/rustc 1.98.1, matching `deploy_image.sh`'s `rust:1.98.1-alpine3.23`), 56s
warm. That is the local gate for phase 5.

Next: phase 8. The proto edit itself is two lines, but the regeneration must be
*targeted* (`protoc` for `volume_server.proto` only, then `cp` into
`seaweed-volume/proto/`) rather than `make -C weed/pb gen`, so master.proto and
the other generated files are not churned; the resulting `.pb.go` diff gets
reviewed for field-only changes before anything else moves.

**2026-09-28 (later) — phases 4–7 done**
| phase | change | verification |
|---|---|---|
| 4 | `ec_shard_config = 3` on `VolumeEcShardsGenerateRequest` (EE's number); regenerated with protoc 33.4 + protoc-gen-go v1.36.6, which reproduce the committed file **byte-for-byte** before the field; volume server resolves request → `.vif` → default via `resolveGenerateEcLayout` | `go test -run TestResolveGenerateEcLayout ./weed/server/`; full `./weed/server/` 121s |
| 5 | Rust generate handler honours the field, rejects impossible layouts, writes the resolved layout to the `.vif` | `cargo test --lib ec_shards_generate` (4 tests); full `cargo test --lib` 630 passed |
| 6 | `ec.encode` loads the policy from the filer once per run, sends it on generate, and refuses to bring a volume online whose holder reports a layout the policy does not name; capacity pre-flight charges the policy's shard count | `go test -run 'TestMountGeneratedEcShards\|TestGenerateEcShards\|TestEcLayoutTotalShards' ./weed/ec/` |
| 7 | resolution order explicit config → collection policy → build default; the worker loads the policy from the filer in its cluster context (detection + execution entry points); job-type defaults are now 0 = inherit | `go test ./weed/worker/tasks/erasure_coding/` |

**2026-09-28 (final) — phase 8 (dashboard) done**

- `/storage/ec-config` page (global default + per-collection overrides, the
  "new volumes only" note, the build-default fallback shown when nothing is set)
  and its API: `GET /api/ec/config`, `POST /api/ec/config/global`,
  `POST /api/ec/config/global/delete`, `POST /api/ec/config/collection`,
  `POST /api/ec/config/collection/delete` (all behind the admin's API auth).
- The page reloads the policy from the filer when opened and after every save, so
  a save cannot leave the admin and the filer disagreeing.
- Tests: request decode/validation, the API's JSON keys, and a page render (which
  is what catches templ runtime errors).
- Verification: `go test ./weed/admin/...` all ok (handlers 3.2s, view/app 5.2s,
  plugin 5.7s, …), alongside the earlier suites.
- Recorded in `fork-delta.md`: `layout.templ`/`ec_config.templ` are produced by
  **templ v0.3.1020** (the go.mod version), while the four files our fork edits
  stay on **v0.3.1001** — using the wrong one rewrites attribute escaping
  tree-wide (483 lines for a one-line edit).

Toolchain notes for the next session:

- `templ` **v0.3.1001** is installed at `~/go/bin/templ` (phase 8 needs it).
- protoc 33.4 lives in `/tmp/protoc33/bin/protoc` (re-download if the machine
  rebooted: `protoc-33.4-osx-aarch_64.zip` from the protobuf releases).
- Rust: `cargo check`/`cargo test --lib` in `seaweed-volume/` pass on macOS —
  optional, not needed to build or deploy anything (the image is Go-only).

## What is left

Nothing in code. All ten questions in `ec-parity-questions.md` are answered, and
both hardening items landed:

1. **`syncECPolicyFromCluster`** now has a test with a fake filer
   (`weed/worker/tasks/erasure_coding/ec_policy_sync_test.go`): the admin's
   document is read and inherited per collection; absent filers and an
   unreachable filer leave the process's policy alone; a document that fails
   validation is rejected without erasing the loaded policy; a deleted document
   clears it.
2. **`test/volume_server/grpc/erasure_coding_test.go`** gained
   `TestEcGenerateRequestedShardConfigOverridesStaleVif` (+106 lines, appended at
   the end of the file): a volume encoded once at the build default (asserting its
   `.vif` now records 10+4) is re-encoded with a requested 3+2 and must produce
   exactly shards 0–4, none of 5–13, with `VolumeEcShardsInfo` reporting 3+2 —
   the incident's exact shape, against a real volume server.

Verified after both: `go test -count=1 ./weed/worker/tasks/erasure_coding/`
(192s, ok) and the single new grpc test (12s, ok). Before deploying, run the
whole grpc package once — `go test -count=1 -v -timeout 30m ./test/volume_server/grpc/`,
~23 min, and never with the default 10m timeout (see `fork-delta.md`).

## The templ version trap (read this before touching admin templates)

Regenerating `view/layout/layout_templ.go` with the **pinned v0.3.1001** rewrote
its attribute handling tree-wide (`ResolveAttributeValue` → `JoinStringErrs` +
`EscapeString`): that file was produced by **v0.3.1020**. Measured in this tree:

- 30 committed `*_templ.go` carry `// templ: version: v0.3.1020` (the go.mod
  version), 6 carry `v0.3.1001` — the six include the four files our fork edits.
- So: regenerate `view/app/{cluster_ec_volumes,cluster_ec_shards,ec_volume_details,collection_details}.templ`
  with **v0.3.1001** (byte-reproducible), and `view/layout/layout.templ` plus the
  new `view/app/ec_config.templ` with **v0.3.1020** (their producer / the go.mod
  version). Both binaries: `~/go/bin/templ` (1001) and
  `~/.local/templ1020/templ` (1020).
- Proof the 1020 run was clean: normalizing `templ_7745c5c3_VarN` and `Line: N,
  Col: N` leaves only the intended nav/`isStoragePage` changes plus the string
  table's index renumbering.

Open decision recorded in `ec-parity-questions.md`: whether the dashboard should
also *reload* the policy into the admin process after a save (so worker jobs
started by that admin see a freshly changed policy without a restart). The page
does this on open and after every save; the worker also reloads per
detection/execution pass.

