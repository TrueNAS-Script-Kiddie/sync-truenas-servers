---
paths:
  - bin/**
  - lib/**
  - config/**
---

# Architectural patterns

These conventions appear across multiple modules. Follow them when editing or adding code.

## Direction model: LOCAL_* and REMOTE_* role vars
`bin/sync_truenas_servers` derives `LOCAL_SERVER_ID` from `hostname -s` (strips `truenas-` prefix) and picks `REMOTE_SERVER_ID` as the opposite of `master`/`backup`. `lib/cli.bash::Process_command_line_options` then sets exactly **one** of `LOCAL_SOURCE`/`REMOTE_SOURCE` and exactly one of `LOCAL_TARGET`/`REMOTE_TARGET` based on `${TASK}:${LOCAL_SERVER_ID}`. Every module decides execution location by testing which of these is set — e.g. `[[ -n "${LOCAL_SOURCE}" ]] && SOURCE_LOCATION="local" || SOURCE_LOCATION="remote"`. Preserve this pattern rather than introducing a new direction variable.

## Local-vs-remote execution via `Execute_command`
All commands that might run on either host go through `lib/common.bash::Execute_command <MODE> <cmd...>`. `MODE` is a substring-matched token string — `local`, `remote`, optionally combined with `verbose` and/or `test` (e.g. `local_verbose_test`). `remote` mode prefixes the command with `${REMOTE_CMD[@]}` (an `ssh -F $SSH_CONFIG_FILE truenas-${REMOTE_SERVER_ID}` array built in the entry script). `test` mode skips execution entirely. Never shell out to `ssh` directly — use `Execute_command remote` so `--test` and verbose tracing stay consistent.

## Pool resolution
`Resolve_pool <server_id> [fast|normal]` in `lib/common.bash` is the single source of truth for pool names: `master` → `master-pool` (normal) or `ssdmaster-pool` (fast); `backup` → `backup-pool` for both. App and VM replication pass `fast`; filesystem replication receives the `POOL_TYPE` as its second argument. Do not hardcode pool names in new code.

## JSON-driven configuration, parsed with `jq`
Replication behavior is declared in `config/apps.json` and `config/vm_device_mappings.json`. Modules `mapfile`/loop over `jq -c` output and extract fields with `jq -r '.field'`. `apps.json` entries may declare `pre_action` / `post_action` strings — these are invoked as shell function names (e.g. `Backup_immich_DB`, `Restore_immich_DB` defined in `lib/immich_db.bash`). Adding a new app-level lifecycle hook means adding a function in `lib/` and referencing it by name in `apps.json`.

## Error handling: `Background_error` aborts everything
Because the working script re-execs itself under `nohup ... | tail -f`, a plain `exit 1` would only kill the background child. `Background_error "msg"` prints the message, kills the tail PID, and exits. Use it for any fatal condition; do not use bare `exit 1` inside `lib/` functions (CLI parsing in `cli.bash` is the exception — it runs before backgrounding).

## `--test` / `TEST_MODE` discipline
`TEST_MODE=true` must skip state-changing operations but **still** perform introspection and container stop/start (per the `--test` help text in `cli.bash::Help`). Two conventions implement this:
1. Pass `test` as part of the `MODE` string to `Execute_command` for zfs/remote commands that must be suppressed.
2. Wrap direct local side effects in `[[ -z "${TEST_MODE}" ]] && ...` (see `lib/immich_db.bash`).
`rsync` uses `${TEST_MODE:+--dry-run}` in `rep_apps.bash`. `zfs_autobackup` uses `${TEST_MODE:+ --test}` in `rep_filesystems.bash`.

## Per-app naming convention
Apps are deployed as `<app>-<server_id>` (e.g. `immich-master`, `immich-backup`). `Control_app_with_checks` builds the full name via `${APP_NAME}-${!SERVER_ID_VAR}` using indirect expansion on `LOCAL_SERVER_ID` / `REMOTE_SERVER_ID`. Container names follow `<app>-<server_id>-<component>-<N>` and are discovered dynamically with `docker ps ... | grep -E`.

## State-machine loops with timeouts
Long operations poll with a counter and bounded sleep, then call `Background_error` on timeout. Examples: `Control_app` (midclt job state → `SUCCESS`, 60s), `Wait_for_docker_state` (60s), `Wait_for_pg_ready` (60s). Match this shape when adding a new wait loop rather than inventing a different retry strategy.

## ZFS replication is property-driven
`rep_filesystems.bash` discovers what to replicate by scanning ZFS datasets for a `autobackup:<task>_<scope>` property (e.g. `autobackup:master_to_backup_latest_snapshot_only`). Datasets are opted in by setting that property in TrueNAS, not by listing them in config. Before replication it `zfs umount`s each target dataset, runs `zfs_autobackup`, then remounts only what it unmounted.

## Module sourcing is flat and order-sensitive
All `lib/*.bash` files are sourced by `bin/sync_truenas_servers` into the top-level shell. Functions and globals share a single namespace — note the uppercase global names (`SCRIPT_DIR`, `LOCAL_SERVER_ID`, `TASK`, `EXEC_DATE`, `SSH_CONFIG_FILE`, `APP_LIST`, `VM_LIST`, `TEST_MODE`, `LOG_FILE`, `DB_RESTORE_LOG`). Do not rely on local encapsulation across modules; do prefix function-local vars with `local` and use `l_`-prefixed inner functions for helpers scoped to a single outer function (pattern used in `rep_filesystems.bash` and `cli.bash`).
