# EC parity — questions for review

Answers are only needed where a default would be expensive to change later. I have
picked a default for each (stated) and kept working; this file records what to
confirm or overrule. Tracked in `design-ec-ee-parity.md`.

## Triage (end of the work)

**Decided by you (2026-09-28):**

- **Q2** — policy vs `.vif` mismatch: **refuse** (my judgement: a wrong layout is
  worse than an aborted batch). Implemented and tested.
- **Q3** — workers page: unset ratio means "inherit the policy" (EE's model).
  Accepted.
- **Q7** — full deployment (volume servers, filer, admin) via helm, so there is
  no mixed-version window. See Q7 for the two caveats.
- **Q8** — a **global** policy (the cheaper one to maintain; per-collection
  overrides can be added later without touching it).
- **Q9** — **no code**: no `-forcePolicy`. Converting an existing volume stays a
  manual `ec.decode` → `ec.encode`, written out below.

**All decided.** Remaining work is test hardening only (see
`design-ec-ee-parity.md`, "What is left").

**Just confirm** (cheap to change): **Q1** path/name · **Q4** route/nav ·
**Q5** clearing the global default · **Q6** API auth.

**Known gap, left as-is by my judgement**: **Q10** — see below; it turns out to be
much narrower than first written, and fixing it would add a new upstream-file row
for a display-only label.

## Q2 in detail — the three layouts, and what "refuse" means

Three things can disagree about one volume's layout:

1. the **policy** — what the operator wants for new volumes;
2. the **`.vif`** on the holder — what the last encode *there* recorded;
3. the layout the holder **actually just generated** — what the encode produced.

`ec.encode` per volume does: generate (sending the policy's layout in the new
request field) → the volume server resolves request → `.vif` → build default and
writes the chosen layout to the `.vif` → the shell mounts shard 0, asks the holder
which layout it serves, and mounts the rest of *that* layout.

Q2 is about the last step. The holder's report can only differ from the policy
when the volume server **ignored the requested layout**: a binary older than this
change, a Rust build without the field, or a future bug. In that case the shell
**aborts the batch**; the existing rollback clears the shards and marks the
volumes writable again, and the error names both layouts
(`volume 3606 on X generated 2+2 but the EC policy for collection "blender-blender" is 3+2; refusing to bring it online at a ratio nobody chose`).

Why refuse instead of warn: warning and continuing would *complete* the encode at
the wrong ratio — mounted, rebalanced, and — the step that makes it irreversible
— the original replicas deleted. That is the same failure class that started this
thread (a stale `.vif` silently steering generation), just one step later. The
cost of refusing is one aborted batch and nothing else: no data loss, the volumes
stay regular and writable, and a re-run after fixing the cause works.

Two cases where the check does **not** fire: no policy is configured (nothing was
requested, so nothing is compared — an unconfigured cluster behaves exactly as
before), and a holder that reports **no** layout at all (an old binary) — then the
build's shard list is used, as before.

After your full deployment the "ignored the request" case should be unreachable,
so the check becomes a tripwire that turns "silently wrong layout" into a loud
failure. If you would rather have availability than that guarantee (e.g. a holder
you cannot upgrade yet), warn-and-continue is three lines.

## Q9 in detail — changing a ratio

Two different operations, often confused:

- **Changing the policy** affects only *new* volumes. Existing EC volumes keep the
  ratio recorded in their `.vif` — that is EE's stated rule too. Nothing to do for
  them, and a mixed cluster is fine: every reader in this fork resolves the layout
  per volume.
- **Changing an existing volume's ratio** means decode-then-encode:

  ```sh
  ec.decode -volumeId=3606          # rebuild a normal volume from its shards, then delete the shards
  ec.encode -volumeIds=3606         # now encodes at the policy (3+2), and rewrites the .vif
  ```

  Bulk form: `ec.decode -collection=blender-blender` then
  `ec.encode -collection=blender-blender`. `ec.decode` needs at least
  `dataShards` of the volume's shards to be reachable, and it picks a target
  location with free space.

  No `-forcePolicy` flag is needed for this path once the volume servers are
  deployed: the generate request carries the policy, the new server honours it
  over the leftover `.vif`, and the encode rewrites that `.vif`. (With an *old*
  volume server the leftover `.vif` would win — the Q7 case.)

So if your volumes are already 3+2 and the global policy is 3+2, the answer to Q9
is "nothing to decode": only new volumes need to get it right, and they now will.


1. **Policy file path/name.** We use `/etc/seaweedfs/ec_ratio.json` (JSON only).
   EE's own path could not be recovered from its binary, and EE also writes an
   `ec_ratio.pb` mirror. *Default taken: JSON only, our own name.* Only relevant
   if an EE process must read our file (or vice versa).

2. **Drift handling in `ec.encode`.** When a volume's recorded layout (its `.vif`)
   disagrees with the policy, EE's stated rule is "existing volumes keep their
   configuration". *Default taken: warn loudly, encode/mount at the volume's own
   layout, never silently at the policy's.* A flag to force the policy ratio
   (re-encode) can be added later if you want it.

3. **Workers-page `data_shards` / `parity_shards`.** **Taken and implemented**
   (phase 7): an unset value now means "inherit" — the cluster EC policy for the
   collection, else the build default — and an explicit number still overrides it.
   Concretely: the stored default is 0, the field's minimum is 0, and the help
   text says so. An existing stored 3/2 keeps working unchanged. *Confirm you want
   the workers-page field to read "0 = inherit the cluster EC policy" now (it is
   the EE model); reverting is a one-line default change if you would rather keep
   10/4 as the page default.*

4. **Dashboard page.** EE has one page in the Storage nav: `/storage/ec-config`
   with a global form plus per-collection overrides. I plan to mirror that route,
   the API shapes (`POST /api/ec/config/global`, `…/collection`) and the "applies
   to new volumes only" note. *Confirm the route/nav placement is fine (it is a
   new nav entry in the Storage section).*

5. **Deleting the global default.** EE's page allows clearing per-collection
   overrides; I also allow clearing the global default (`ec.config`). With no
   global and no override, encoding falls back to the build default 10+4.
   *Confirm "no policy = build default" is acceptable rather than forcing a
   policy to exist.*

6. **Admin API auth.** The new `/api/ec/config/...` endpoints will sit behind the
   admin's existing API auth (same as `/api/plugin/...`). *Confirm no separate
   token/permission is expected.*

## Deliberate deviations from EE (for the record)

- We share one filer-I/O helper between the shell and the admin; EE duplicates it
  in each caller.
- No `ec_ratio.pb` mirror, no `min_tombstone_age_ns` generate field (EE-only
  features we do not use).
- Mount after generation reads the holder's reported layout rather than a
  policy-provided shard list, so a volume carrying a different ratio mounts
  correctly instead of failing (or being re-encoded) mid-batch.

## Added at the end of the work (found while reviewing, not asked earlier)

7. **Upgrade order — resolved: full helm deploy of everything, Go only.** With
   volume servers, filer and admin rolled together there is no mixed-version
   window, so the refusal path in Q2 should never be seen. No Rust build is needed:
   the deployed image is built from `docker/Dockerfile.local`, which contains no
   Rust, and `VOLUME_SERVER_IMPL=rust` is only ever set by tests (and upstream's
   own Rust CI job). The Rust edits in
   this change are parity-only (the proto copy is forced by `weed/pb/Makefile`).
   The one caveat that remains: set the policy *after* the rollout finishes — a
   batch that carries a policy but reaches an old volume server would be refused.

8. **Global policy — decided: global.** The cheaper one to maintain: a single
   value, and per-collection overrides can be added later without touching it.
   Set it from the dashboard page or with
   `ec.config -set -dataShards=3 -parityShards=2`. It applies to *new* volumes in
   every collection; nothing already encoded changes.

9. **Re-laying existing volumes.** After a policy change, volumes that already
   record a ratio keep it, and `ec.encode` refuses them (Q2). If you ever want to
   *convert* them (e.g. everything to 3+2 once and for all), that needs either
   deleting each `<base>.vif` on the holder before the encode, or a `-forcePolicy`
   flag that re-encodes at the policy ratio. I have not built that flag; say the
   word and it is a small addition to `ec.encode` plus one test.

10. **Admin job-plan labels (narrower than first written; left as-is).** The plan
    view in `weed/admin/plugin/job_execution_plan.go` (upstream file, untouched by
    the fork) resolves a job's data/parity with
    `ecParams.DataShards <= 0 → build default`. Since the worker's *detection*
    now stamps the policy-resolved ratio into the proposal's parameters, jobs the
    worker creates carry the real ratio (labels correct). Only a job stored with
    `0` parameters — a job created before this change — can show "10+4" labels
    while the worker actually encodes the policy's ratio. Display-only: the
    encode itself resolves the policy independently. *Judgement: leave it; fixing
    costs a new upstream-file row for a label that only legacy jobs can get
    wrong. Revisit if the job view ever looks wrong.*

