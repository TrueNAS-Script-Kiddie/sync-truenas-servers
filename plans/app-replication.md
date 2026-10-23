# Plan: replicate every app, fully independent of the other host

**Goal:** after a sync, every app on truenas-master (current and future) runs on truenas-backup
with the same content and **no reference to truenas-master** — see README "Purpose". Adding an
app should be configuration, not code.

## Where it stands

- **Data** (`app_replication`): works for plex, immich (DB dump/restore), grafana.
- **Definitions** (`app_definition_replication`, manual-only): create and update work for
  catalog apps — immich-backup updated, grafana-backup created; plex `--test` clean, no real run
  yet. Custom (`custom_app`) apps are skipped; `ix-app` apps are untested.
- **Gap:** host references inside the app's own files and databases are not rewritten at all.

## What has to be transformed

Inventory of every app on truenas-master (2026-10-23; per-app values in
[config/app_divergences.md](../config/app_divergences.md)). Host references live in three layers:

| Layer | Where | Covered |
|---|---|---|
| App definition (`app.config`) | host paths, certificate id, URLs (grafana, forgejo, firefly-iii), firefly-iii's bind IP, `hostname` (syslog-ng, promtail, loki), hermes-agent's compose volume | yes, by `app_config_mappings.json` |
| Config files in the app's data | forgejo `custom/conf/app.ini`; syslog-ng `syslog-ng.conf`; promtail `config.yml` | no |
| App database | grafana `grafana.db` (SQLite): Loki data-source URL | no |

Copied as-is (checked, no host references or only history): the Postgres DBs of immich,
firefly-iii and forgejo; Plex's library DB; hermes-agent's memories and sessions.

Decisions taken:
- **Logging chain** (router → syslog-ng → promtail → loki → grafana): on truenas-backup every link
  points at truenas-backup's own copy.
- **`:latest` images** (syslog-ng, promtail, loki, hermes-agent) stay `:latest`; replication
  compares the running image digest on both hosts and skips the app when they differ. The current
  version checks compare the `ix-app` wrapper version, identical everywhere, so they miss this.
- **Forgejo:** the instance on truenas-backup is only used on failover. Push mirrors / webhooks
  (none today) must be disabled on it if ever added.
- **Immich, Plex** keep their mechanisms (DB dump/restore; selective folder list).
- **`app.delete`** is allowed as a deliberate method (e.g. where recreate is cleaner than
  update), never with ix-volume removal, always shown by `--test`.

## Steps

1. **Config redesign.** One readable config covering all three layers per app, with consistent
   names — including the hook points (today `pre_action`/`post_action`; a rewrite between copy
   and start needs a third, so name them by *when* they run). Replaces `apps.json` +
   `app_config_mappings.json` or restructures them; decided in this step.
2. **Rewrite inside data** — files and SQLite on the target while the app is stopped, after the
   copy and before the start, as new subcommands of `lib/transform.py`. `--test` prints every
   change.
3. **Image-digest check** (data and definition replication): skip an app whose running images
   differ between the hosts. Needed for `:latest` apps, and for every app whose database files
   are copied directly — the current check compares only the upstream app version, which can
   match while the Postgres images differ.
4. **Create prerequisites:** before `app.create`, check the target's host paths exist; if not,
   skip with the exact GUI steps ([docs/operations.md](../docs/operations.md#preparing-truenas-backup-for-a-new-app)).
   Never create datasets from the script.
5. **`ix-app` and custom apps:** verify their `app.config` / compose payloads on a live host,
   then support them (hermes-agent is the custom-app test case until it is replaced by the
   official TrueNAS app).
6. **Roll out per app** — loki/promtail/syslog-ng, forgejo, firefly-iii, hermes-agent; plex's
   first real definition run. Forgejo's and firefly-iii's Postgres: copy the data directory
   with the app stopped on both hosts, no dump — only when step 3 confirms the identical
   Postgres image on both (same Postgres major and glibc, so no collation mismatch); otherwise
   skip the app. Before each app's first run, check its `--test` diff for unmasked secrets.
7. **Include `app_definition_replication` in `--subtask=all`** once it has survived supervised
   runs in both directions.
8. **Name convergence** — same bare app name (`immich`) on both hosts, suffix logic removed.
   Simplifies code and config (the `-master`/`-backup` rewrite disappears). TrueNAS can't rename
   an app, so per app: a transition flag (e.g. `per_server_name_suffix`) lets apps migrate one at
   a time; on truenas-backup first, delete the suffixed app (host-path data stays) and recreate it
   under the bare name via the pipeline, replicate data, test end to end (doubles as a failover
   drill); then the same on truenas-master in the other direction; finally remove the flag and
   the suffix code.
