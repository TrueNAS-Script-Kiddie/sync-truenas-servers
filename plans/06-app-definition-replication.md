# Plan 06 — App-definition replication & config convergence

> **Status 2026-10-22:** phase 0 done 2026-10-21 — payload shapes confirmed
> ([06-phase0-findings.md](06-phase0-findings.md)). Phase 1 done 2026-10-22:
> bucket (b) converged on both hosts (`config/app_divergences.md`).
> Phase 2 run for real 2026-10-22 on a throwaway app (it-tools): `app.create`
> and `app.update` both succeeded with no post-apply drift; `--test` runs on
> Plex/Immich clean. First real update on immich-backup 2026-10-22 (master→backup,
> `--stop-running-apps`): no post-apply drift. `--test` backup→master clean the
> same day (Plex/Immich: nothing to push; reverse path rules and `keep_target`
> correct). A real backup→master run waits for an actual failback.
> grafana-backup created 2026-10-23 (master→backup): no post-apply drift, serves
> the `truenas-backup` cert. Target dataset + host paths were made by hand first.
> grafana data replicated the same day (`data/` SQLite + `plugins/`). The Loki
> data source in `grafana.db` still points at master (`192.168.0.10:3100`).
> Inventory of every app on truenas-master done 2026-10-23 (see "Inventory
> decisions"). Next: redesign `app_config_mappings.json` (and the `apps.json`
> hooks) so one config covers all three layers — app definition, files inside
> the app's data, app databases — then implement the transforms.
> Phase 3 not started.

**Goal:** close the remaining gap for failover (truenas-backup fully taking
over from a broken truenas-master — README "Purpose"). Today
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

## Scope: every app on master

The goal is that **all** apps on master replicate (definition + data), not only
the two in `apps.json` today. Each row stays until it works.

| App | Definition replication needs | Data replication needs | Status |
|---|---|---|---|
| plex | host paths, `keep_target` claim token — `--test` clean, no real run yet | ✅ selective folder list in `apps.json` (identity files stay per host) | definition: first real run pending |
| immich | ✅ host paths | ✅ in `apps.json`; DB dump/restore 1:1 (DB has no host references) | done |
| grafana | ✅ `certificate_id` 2↔3, `root_url` | ✅ `data`, `plugins`; **open:** Loki data source URL inside `grafana.db` (SQLite) | data-source rewrite open |
| forgejo | `certificate_id`, `root_url` (existing rules cover them) | repos + Postgres DB (no host references, only history); **`custom/conf/app.ini`**: `DOMAIN`, `SSH_DOMAIN`, `ROOT_URL` name the IP | open. Clients follow failover via the `forgejo` SSH alias (`HostName data`). Forgejo-backup is only used on failover; push mirrors/webhooks (none today) must be disabled there |
| firefly-iii | `app_url`, **`host_ips` = the host's own IP** (binds there; wrong IP = won't start) — existing IP rule covers it; target host paths must exist before `app.create` | Postgres DB (no host references) + `uploads`; `APP_KEY` comes with the definition | open |
| loki, promtail, syslog-ng | `ix-app` (custom image, `:latest`): `hostname` (`<app>-master`, rewritten by the name convention), host paths; payload shape unverified | config files: **syslog-ng `syslog-ng.conf`** forwards to the IP, **promtail `config.yml`** pushes to Loki's IP; loki clean. Chain on truenas-backup must point at its own copies | open; image-digest check needed (`:latest`) |
| hermes-agent | custom app (compose): volume path; name lacks the `-<server>` suffix the code relies on; `:latest` | content (memories, sessions) copied as-is | open; to be replaced by the official TrueNAS app — meanwhile the custom-app test case |

## Inventory decisions (2026-10-23)

From the inventory of all apps on truenas-master (app definition, the app's own
files/DBs, container volumes):

- **Host references live in three layers**, all must be rewritten for
  truenas-backup to be independent: app definition (cert id, root/app URL,
  firefly-iii's `host_ips`, host paths), config files inside the app's data
  (forgejo `app.ini`; syslog-ng `syslog-ng.conf`; promtail `config.yml`), and
  app databases (grafana's SQLite data source). Checked as root: the Postgres
  DBs of immich and firefly-iii have no host references; forgejo's only hold
  history (Actions run payloads) and an SSH key's name label — copied as-is.
  Plex's library DB has none.
- **Logging chain** (router → syslog-ng → promtail → loki → grafana): on
  truenas-backup every link points at truenas-backup's own copy. The router
  itself is repointed by hand on failover.
- **Immich, Plex:** current mechanisms (DB dump/restore; selective folder
  list) stay; they only move into the new config shape.
- **`:latest` images** (syslog-ng, promtail, loki, hermes-agent): keep
  `:latest`; add a check that compares the running image digest on both hosts
  and skips the app when they differ. Today's version checks compare the
  `ix-app` wrapper version, identical on both, so they miss this.
- **ix-volumes** hold only caches/temp (Redis, ML cache, transcodes, tmp) that
  rebuild themselves — `app.delete` is acceptable as a method.
- **Forgejo:** clients reach it via the `forgejo` SSH alias, now `HostName
  data`, so they follow the failover. No push mirrors/webhooks today; if added,
  disable them on forgejo-backup.
- **hermes-agent:** content (memories, sessions) is copied as-is, not
  rewritten. To be replaced by the official TrueNAS app; meanwhile the test
  case for custom (compose) apps.

## Non-negotiable guardrails (in addition to the global ones)

- **`app.delete` only as a deliberate method, never with ix-volume removal.**
  `app.delete` can also delete the app's ix-volumes (storage TrueNAS keeps in
  its own `ix-apps` dataset) — data loss. Host-path data is not touched by it.
  Delete-and-recreate is allowed where it is the cleaner method, only for apps
  whose data lives in host paths, and `--test` must show it. Not used yet;
  verify `app.delete`'s parameters and which apps use ix-volumes on both hosts
  before the first use.
- **A RUNNING target app is skipped with a loud warning by default** — it may
  be a test on truenas-backup. `--stop-running-apps` overwrites it for that run
  (stop, update, start again); kept separate from `--start-stopped-apps`
  because stopping a running app must be chosen on purpose.
- **Never transform or copy external-identity values** (Plex claim/GUID
  territory, anything registering with a third-party service). These stay
  per-instance, exactly like `app_dir_list` already protects their files.
  Enforced by `keep_target` in `app_config_mappings.json` (Plex `claim_token`).
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

**Built** — [lib/rep_app_definitions.bash](../lib/rep_app_definitions.bash).
Where it differs from the design below: the mapping file uses
`string_rewrites` (whole-token literal replace, optional `scope`),
`value_overrides` (per-key exact-value swap) and `keep_target` (per-path,
target keeps its own value) instead of the VM rule shape;
server-managed keys (`ix_*`, `release_name`) are stripped on extract; an
update is skipped when the catalog versions differ (`app.update` keeps the
target's version); shown diffs mask secret-looking values; `--app` works for
any `apps.json` entry, including a name-only (definitions-only) one.

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
   no-subtask default**. Dispatch from the subtask list in
   [bin/sync_truenas_servers](../bin/sync_truenas_servers)
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
   (`Control_app_with_checks` and the other `${APP_NAME}-${…SERVER_ID}` sites in
   [lib/rep_apps.bash](../lib/rep_apps.bash) and
   [lib/rep_app_definitions.bash](../lib/rep_app_definitions.bash)), and the docker grep
   patterns in [lib/immich_db.bash](../lib/immich_db.bash) similarly. This
   lets apps migrate **one at a time** with the tool working throughout.
2. Per app, on **backup** first: verify replicated data is current → delete
   the suffixed app in the UI (double-check the delete dialog leaves host-path
   datasets and, if applicable, ix_volumes alone) → recreate as the bare name
   via the pipeline (name override: transform target name = bare name) →
   run data replication + DB restore → test the app end-to-end (this IS a
   failover drill).
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
