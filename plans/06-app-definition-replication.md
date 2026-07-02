# Plan 06 — App-definition replication & config convergence

**Goal:** close the real gap in demand 1 (fast failover). Today
`app_replication` syncs *data* into an app that must already be installed on
the target; the app **definition** is the one manual, error-prone step left.
This plan mirrors the proven VM pattern (`Extract_vm_definitions` →
`Transform_vm_definition` → recreate) for apps, using
`midclt call app.config` / `app.create` / `app.update`, and then uses that
pipeline to *converge* the artificial per-host divergences (ports, and
eventually names).

Do plans 01–02 first. Read `plans/README.md` guardrails. This plan is
**phased**: each phase is independently valuable and a safe stopping point.
Phases 0–1 are read-only/low-risk; do not start phase 2 until phase 0's
findings are written down, because the exact `midclt app.*` payload shapes on
Goldeye must be confirmed on a live host — do not trust this document (or any
model's memory) for those shapes.

## Non-negotiable guardrails (in addition to the global ones)

- **The pipeline must NEVER call `app.delete`.** `app.delete` has options that
  can remove ix_volumes (data loss). Semantics are strictly: create if
  missing, update if present-and-stopped, **skip with a loud warning if the
  target app is RUNNING** — a running target instance may be a
  test-in-progress (demand 2) and must never be clobbered by a sync.
- **Never transform or copy external-identity values** (Plex claim/GUID
  territory, anything registering with a third-party service). These stay
  per-instance, exactly like `app_dir_list` already protects their files.
- New subtask is **manual-only** at first: not part of `--subtask=all` until
  it has survived several supervised runs in both directions.

---

## Phase 0 — Host exploration (read-only; produces notes, no code)

On `truenas-master`, as root, record the answers to these in
`plans/06-phase0-findings.md`:

1. Shape of an app object:
   `midclt call app.query | jq '.[0] | keys'` and
   `midclt call app.query | jq '.[] | {name, state, custom_app, version, train: .metadata?.train?}'`
   — note which fields identify the catalog app, train, and version, and
   whether any of your apps are `custom_app: true` (custom apps replicate via
   their compose config instead of catalog values — different payload).
2. Full config of one app:
   `midclt call app.config immich-master | jq . > /root/immich-master.config.json`
3. The create/update signatures:
   `midclt call core.get_methods | jq '."app.create", ."app.update"'`
   — record the accepted keys (expect roughly
   `{app_name, train, catalog_app, version, values, custom_app, custom_compose_config}`;
   **verify, don't assume**).
4. Whether the source app's `version` is installable on the target
   (catalog trains sync per host): note where the available versions list
   comes from (`app.available` / catalog query) and check both hosts show the
   same version for one app.
5. Repeat step 2 on `truenas-backup` for the same app (needed for phase 1).

**Deliverable:** `plans/06-phase0-findings.md` with the confirmed payload
shapes and any surprises. Every later phase cites this file, not guesses.

## Phase 1 — Config audit & convergence (small, mostly host work)

For each app in `config/apps.json`, diff the two live configs:

```bash
diff <(jq -S . /root/immich-master.config.json) <(jq -S . /root/immich-backup.config.json)
```

Classify every difference into exactly one bucket:

| Bucket | Meaning | Action |
|---|---|---|
| **(a) must diverge** | external identity; per-host certificate ID (the `truenas-master`/`truenas-backup` cert have different IDs on each box); anything embedding the host's own IP/hostname (e.g. Root URL) | keep; becomes a transform rule in phase 2; document |
| **(b) artificial drift** | ports, resource limits, toggles that just drifted apart over time | **converge now** via the UI or `midclt call app.update <name> '{"values": ...}'` |
| **(c) pool-path differences** | `ssdmaster-pool` vs `backup-pool` in host paths | expected; becomes a mechanical transform rule (same shape as `vm_device_mappings.json`) |

Port convergence note: identical host ports on both servers is not just
cleanup — the `data` floating-alias failover (hosts-file repoint) only works
transparently for web services if both hosts serve on the **same port**.

**Deliverables:**
- bucket-(b) divergences eliminated on the live hosts;
- `config/app_divergences.md` — a short table per app of the bucket-(a)
  fields that must stay different, and the bucket-(c) mechanical rewrites.
  This file is the spec for phase 2's mapping config.

Phase 1 alone already delivers most of the "what's different between my two
installs?" mental-load reduction. **Stop here and live with it for a while if
unsure.**

## Phase 2 — `app_definition_replication` subtask (medium)

Mirror the VM architecture — same file layout, same naming, same TEST_MODE
discipline:

1. **`config/app_config_mappings.json`** — transform rules derived from
   `config/app_divergences.md`. Reuse the `vm_device_mappings.json` rule shape
   (`{scope, master: ..., backup: ...}` lists keyed by config path), so the
   transform logic can share its mental model:
   - pool-path rewrites (bucket c),
   - certificate-ID map (bucket a, per host),
   - host-IP/URL rewrites (bucket a).
2. **`lib/rep_app_definitions.bash`** with, following `rep_vms.bash`'s shape:
   - `Extract_app_definitions` — `app.query` metadata + `app.config` per app,
     written to `tmp/apps/json/per_app/<app>.<server>.{source,target}.json`;
   - `Transform_app_definition` — apply mapping rules source→target (if plan
     05 was done, extend the Python transformer; otherwise jq, following the
     existing `Transform_vm_definition` structure);
   - `Apply_app_definition` — decision per app:
     - target app **absent** → `midclt call app.create` with the phase-0
       verified payload (source's `catalog_app`/`train`/`version` + transformed
       values); if the version isn't available on the target, fail with a
       message that says to sync catalogs — do not silently take "latest";
     - target app **present and STOPPED/CRASHED** → `midclt call app.update`
       with transformed values only;
     - target app **RUNNING** → skip + WARNING (see guardrails);
     - **never `app.delete`**.
   - All state-changing calls suppressed by TEST_MODE (mode token `_test`),
     but extract/transform/print always run, so `--test` shows the full diff
     of what *would* be pushed.
3. **CLI wiring** ([lib/cli.bash](../lib/cli.bash)): new
   `--subtask=app_definition_replication` setting a new `PERFORM_APP_DEF_REP`
   flag; honor `--app=` filtering; **do not add it to `--subtask=all` or the
   no-subtask default**. Dispatch from
   [bin/sync_truenas_servers:86-90](../bin/sync_truenas_servers#L86-L90)
   *before* `Perform_app_replication` (definition before data). Source the new
   lib file with the others.
4. **Docs**: add the subtask to `Help`, `CLAUDE.md`/`AGENTS.md`.

**Verification:** `--test` run both directions and read the printed
transformed values against `config/app_divergences.md`; then one real run
against a deliberately stopped, unimportant app on backup; then diff
`app.config` on target vs the transformed JSON.

## Phase 3 — Name convergence (optional; only after phase 2 is trusted)

End state: the same bare app name (`immich`, `plex`) on both hosts; the
`-master`/`-backup` suffix logic removed from code. TrueNAS cannot rename an
installed app, so this is a **per-app delete/recreate migration** — run it via
the phase-2 pipeline, which makes each migration double as a failover drill:

1. Add a per-app transition flag to `apps.json`, e.g.
   `"per_server_name_suffix": true` (default true). Code composes
   `FULL_APP_NAME` with the suffix only when the flag is true
   ([lib/rep_apps.bash:44](../lib/rep_apps.bash#L44)), and the docker grep
   patterns in [lib/immich_db.bash](../lib/immich_db.bash) similarly. This
   lets apps migrate **one at a time** with the tool working throughout.
2. Per app, on **backup** first: verify replicated data is current → delete
   the suffixed app in the UI (double-check the delete dialog leaves host-path
   datasets and, if applicable, ix_volumes alone) → recreate as the bare name
   via the pipeline (name override: transform target name = bare name) →
   run data replication + DB restore → test the app end-to-end (this IS the
   demand-1 drill).
3. Same for master (recreate from backup's now-canonical definition,
   direction `backup_to_master`).
4. Flip the app's `per_server_name_suffix` to false. When all apps are
   migrated, remove the flag and the suffix composition entirely.

Do **not** do this as a manual big-bang reinstall without phase 2 — that's
all of the risk with none of the tooling payoff.

---

## Order & dependencies

Phase 0 → 1 → 2 → 3, strictly. Phases 0–1 need only host access and produce
documentation + live-config convergence. Phase 2 is the first code change.
Phase 3 changes production app installs — schedule it, don't drift into it.
