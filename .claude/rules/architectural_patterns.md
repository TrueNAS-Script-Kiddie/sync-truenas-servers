---
paths:
  - bin/**
  - lib/**
  - config/**
---

# Architectural patterns

Conventions across modules. Follow them when editing or adding code.

## Direction model
`bin/sync_truenas_servers` derives `LOCAL_SERVER_ID` from `hostname -s` (strips `truenas-`) and
`REMOTE_SERVER_ID` as the other of `master`/`backup`. `cli.bash` sets exactly **one** of
`LOCAL_SOURCE`/`REMOTE_SOURCE` and one of `LOCAL_TARGET`/`REMOTE_TARGET` from
`${TASK}:${LOCAL_SERVER_ID}`. `Resolve_direction` (common.bash) derives the read-only view every
module uses: `SOURCE_LOCATION`/`TARGET_LOCATION` (`local`|`remote`) and
`SOURCE_SERVER_ID`/`TARGET_SERVER_ID`. Use these; never re-derive direction or remove the four
base variables.

## Local-vs-remote execution via `Execute_command`
Every command that may run on either host goes through `Execute_command <MODE> <cmd>`. `MODE` is
substring-matched: `local` or `remote`, optionally `verbose` and/or `test`
(`local_verbose_test`). `remote` prefixes `${REMOTE_CMD[@]}` (`ssh -F $SSH_CONFIG_FILE
truenas-<peer>`); `test` skips execution. Never call `ssh` directly. Commands are strings
evaluated through several quoting layers — this design is load-bearing; changing it amounts to a
rewrite.

## Pool resolution
`Resolve_pool <server_id> [fast|normal]` is the only source of pool names: `master` →
`master-pool` / `ssdmaster-pool` (fast); `backup` → `backup-pool` for both. Apps and VMs use
`fast`. Never hardcode a pool name in code.

## JSON-driven configuration
Behaviour is declared in `config/apps.json`, `config/app_config_mappings.json` and
`config/vm_device_mappings.json`, read with `jq` (`mapfile` over `jq -c`, fields via `jq -r`).
`apps.json` `pre_action`/`post_action` are shell function names called around the data copy — a
new app hook is a function in `lib/` referenced by name.

## Errors: abort the run, skip the item
`Background_error "msg"` prints, runs the cleanup stack, mails the failure, kills the tail and
exits. A plain `exit` inside `lib/` would only end the background worker and orphan the tail —
never use it there (`cli.bash` parsing runs before backgrounding and may). A **per-item**
precondition that fails before anything changed (wrong state, not installed, version mismatch) is
not fatal: print `Replication skipped for …` with the fix, count it, `continue`, report counts in
the subtask summary — runs are started by hand and watched.

## Restore-to-previous-state: the cleanup stack
A section whose side effects must be undone on failure wraps itself in `Register_cleanup '<undo>'`
/ `Unregister_cleanup`. The undo is **single-quoted** (expands at fire time); it undoes only what
*this run* changed (remount `UNMOUNTED_LIST`, never all `IMPACTED_DATASETS`); register just before
the risky step, unregister right after the normal-path undo, fresh per loop iteration. `Run_cleanup`
pops LIFO. `Background_error` runs the stack before killing the tail; the single
`trap 'Run_cleanup; Kill_tail' EXIT` in `bin/` is the safety net — never add another EXIT trap.
`Kill_tail` is not a stack entry: it runs on every exit, last.

## `--test` discipline
`TEST_MODE` skips every state-changing operation but **still** runs introspection and app/container
stop/start. Mechanisms: the `test` token in `Execute_command`'s mode; `[[ -z "${TEST_MODE}" ]] &&`
around direct side effects; `rsync ${TEST_MODE:+--dry-run}`; `zfs_autobackup ${TEST_MODE:+--test}`;
`rollup.py -t`. Extract/transform/diff output always runs, so `--test` shows what would change.
One deliberate exception: VM replication still sets and clears the source zvols'
`autobackup:<task>_vm_latest_snapshot_only` tag (`Tag_vm_disks`, `Cleanup_vm_disk_tags`) —
`zfs_autobackup --test` selects datasets by that tag, and the run removes it again.

## Two-instance app topology
Both hosts run the same apps as separate instances (dual-active whenever truenas-backup is on);
truenas-backup must fully take over on failover. Consequences:
1. **No cross-host references after a sync.** Every value naming the source host (IP, hostname,
   pool path, certificate) is rewritten to the target's own — in the app definition, the app's
   files and its database.
2. **`app_dir_list` is a contract.** It separates shared content (copied) from per-instance
   identity (never copied) — e.g. Plex's `Preferences.xml` (server GUID, claim), `Cache`, `Logs`.
   Check sibling files for identity before broadening it. `keep_target` in
   `app_config_mappings.json` does the same for definition values (Plex `claim_token`).
3. **Immich's dump path is load-bearing.** Postgres lives in `immich-pgdata-ds`, not in
   `app_dir_list`; `Backup_immich_DB` writes the dump there and moves it into
   `immich-data-ds/backups/` because `backups` *is* copied — the dump rides the rsync. Change
   either path and the restore silently finds nothing.
4. **Immich's container order is the only one that works:** stop writers → start pgvecto → dump;
   on the target stop all → wipe pgdata → start pgvecto → wait (TCP) → restore → full app stop →
   start. Don't reorder.

## Naming
Apps `<app>-<server_id>`; `Control_app_with_checks` builds it with indirect expansion on
`LOCAL_SERVER_ID`/`REMOTE_SERVER_ID`. Containers `ix-<app>-<server_id>-<component>-<N>`, found
with `docker ps … | grep -E` (never anchored at `^<app>`).

## Wait loops
Poll with a counter and bounded sleep, `Background_error` on timeout: `Control_app` (job →
`SUCCESS`, 60 s), `Wait_for_app_job` (600 s, a create pulls images), `Control_vm` (180 s),
`Wait_for_docker_state` (60 s), `Wait_for_pg_ready` (60 s). Match this shape. A readiness probe
must use the same channel as the command that follows (`Wait_for_pg_ready … tcp` before a
`psql --host=localhost`: during initdb bootstrap the socket answers while TCP is still down).

## ZFS replication is property-driven
Datasets opt in with `autobackup:<task>_<scope>=true`; discovery filters on the value. Target
datasets are unmounted before `zfs_autobackup` and exactly those remounted after. VM zvols are
tagged per run (`<task>_vm_latest_snapshot_only`) and untagged afterwards.

## Module sourcing
All `lib/*.bash` are sourced flat into one shell: functions and uppercase globals share one
namespace (`SCRIPT_DIR`, `TASK`, `TEST_MODE`, `LOG_FILE`, `APP_LIST`, `VM_LIST`, …). Declare
function-local vars `local`; name helpers scoped to one function `l_…`. `SOURCE_POOL`/`TARGET_POOL`
reach `immich_db.bash` by dynamic scoping from `Perform_app_replication` — keep those names.

## Deliberate choices — don't "fix"
- **`zfs_autobackup`'s `*_OPTARGS` are unquoted on purpose** (each holds several flags that must
  word-split). Quoting them breaks the command; after editing that line, check the logged command
  still shows separate flags.
- **`[[ … =~ " ${X} " ]]` with a quoted right side** is a literal substring match, not a regex
  (names may contain regex characters) — ignore shellcheck SC2076 there.
- **No `set -u` / `set -o pipefail`:** the code relies on unset-is-empty (`LOCAL_SOURCE` vs
  `REMOTE_SOURCE`, `${TEST_MODE:+…}`) and on pipelines whose early stages may fail harmlessly.
- **VM stops are graceful only** — a forced stop lets the source snapshot catch a guest that
  hasn't shut down cleanly ([docs/truenas-api.md](../../docs/truenas-api.md#vms)).
- **Any `zfs_autobackup` failure aborts the run.** Within a scope it already continues past a
  failed dataset. Carrying on to later subtasks would start a VM on a stale disk, can't tell a
  crash (also exit 1) from one failed dataset, and would report a stale backup as a success.
- **The 10 s sleep before the target's `vm.delete`** outlasts TrueNAS's 5 s post-stop poll, which
  otherwise re-defines the deleted domain ([docs/truenas-api.md](../../docs/truenas-api.md#vms)).
- **Stay in bash for orchestration.** Most code drives external commands; a rewrite would be
  validated only by destructive live runs. Pure data transforms (JSON in → JSON out) are the
  exception: they move to Python, testable offline ([plans/app-replication.md](../../plans/app-replication.md)).

## Linting
`bash -n` and `shellcheck -x` on every touched file. The VS Code shellcheck extension needs both a
`# shellcheck source=…` directive above each dynamic `source` and `"shellcheck.customArgs": ["-x"]`
(in `.vscode/settings.json`). Cross-file globals carry targeted `# shellcheck disable=SC2034`
comments with the reason.
