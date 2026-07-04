# sync_truenas_servers

## Project overview
Bash-based replication orchestrator for a pair of TrueNAS SCALE servers (`truenas-master` ⇄ `truenas-backup`) running in a **dual-active** topology — both hosts have the same apps/VMs installed independently and may run simultaneously, so replication syncs *content* between two live instances rather than priming a cold standby. A single entry script coordinates four replication subtasks: TrueNAS Apps (rsync + app stop/start via `midclt`), VMs (JSON-driven definition transform + zvol replication), ZFS filesystems (via `zfs_autobackup`), and snapshot rollup (via external `zfs-rollup`). Must run as `root`; logs go to `logs/` and run-state JSON to `tmp/vms/`.

## Tech stack
- Bash 5 (scripts are `#!/usr/bin/bash`)
- `jq` for JSON parsing/transform (configs + `midclt` output)
- `midclt` (TrueNAS middleware CLI) for app/VM queries and control
- `zfs` / `zfs_autobackup` for replication; `rsync` for app data; `ssh` for remote exec
- Docker CLI for low-level Immich Postgres container control
- `sendmail` for completion email

## External repos
`zfs_autobackup` and `zfs-rollup` are **third-party tools** (not authored in this project) — public git repos that this project drives as external dependencies. They are expected as siblings of this project (referenced via `${SCRIPT_DIR}/../../`):
- `../zfs_autobackup/` — ZFS snapshot/replication tool driven by dataset `autobackup:<task>` properties (see `.claude/rules/architectural_patterns.md`); contains `autobackup-venv/bin/python` and the `zfs_autobackup` module.
- `../zfs-rollup/` — snapshot pruning/consolidation tool, invoked post-replication via `rollup.py`.

Locally they're cloned to `C:\Apps\zfs_autobackup` and `C:\Apps\zfs-rollup` as their own VS Code projects, SFTP-synced to the same sibling layout on the TrueNAS host. When behavior needs verifying beyond what's summarized here, read the source directly at those paths rather than guessing from usage in `lib/`. The local clones are what's actually deployed and may lag the upstream repo — that's expected; treat the local copy as the version of record for understanding current behavior, not the latest upstream.

New-host deploy: SFTP doesn't set the exec bit on new files — `chmod +x bin/sync_truenas_servers` once after first push to a host. Same applies to `zfs-rollup/rollup.py`; `zfs_autobackup` also needs `autobackup-venv/` built fresh (SFTP excludes it) — see those projects' own docs.

## Key directories
- [bin/sync_truenas_servers](bin/sync_truenas_servers) — entry point. Resolves paths, sources config + all `lib/` modules, re-execs itself with `--running_in_background` via `nohup`, dispatches subtasks, emails log.
- [lib/](lib/) — one module per concern. All functions share globals set by the entry script + `cli.bash`.
  - [common.bash](lib/common.bash) — `Execute_command` (local/remote/test/verbose mode multiplexer), `Background_error`, `Resolve_pool`.
  - [cli.bash](lib/cli.bash) — `Process_command_line_options`, `Help`. Sets `TASK`, `PERFORM_*` flags, `APP_LIST`, `VM_LIST`, and the `LOCAL_SOURCE`/`REMOTE_SOURCE`/`LOCAL_TARGET`/`REMOTE_TARGET` direction vars.
  - [rep_apps.bash](lib/rep_apps.bash) — `Perform_app_replication`, `Control_app`, `Control_app_with_checks`. Driven by [config/apps.json](config/apps.json).
  - [rep_vms.bash](lib/rep_vms.bash) — `Extract_vm_definitions`, `Transform_vm_definition`, VM replication pipeline. Writes per-VM JSON under `tmp/vms/json/per_vm/`. Driven by [config/vm_device_mappings.json](config/vm_device_mappings.json).
  - [rep_filesystems.bash](lib/rep_filesystems.bash) — `Perform_filesystem_replication` (scopes: `all_snapshots`, `latest_snapshot_only`, `vm_latest_snapshot_only`). Discovers impacted datasets via `autobackup:<task_scope>` ZFS property.
  - [snap_rollup.bash](lib/snap_rollup.bash) — `Perform_snapshot_rollup`. Shells out to `../../zfs-rollup/rollup.py`.
  - [docker.bash](lib/docker.bash) — `Control_docker_containers`, `Wait_for_docker_state`.
  - [immich_db.bash](lib/immich_db.bash) — `Backup_immich_DB` / `Restore_immich_DB` wired as `pre_action` / `post_action` in `apps.json`.
- [config/](config/) — `apps.json` (app replication spec), `vm_device_mappings.json` (per-dtype path/NIC/display rewrites master↔backup), `config.local.bash` (untracked, provides `EMAIL_TO` + `SSH_CONFIG_FILE`), `config.example.bash` (template).
- `logs/`, `tmp/` — runtime only (gitignored).

## Essential commands
No build step, no test suite, no linter configured. Validation is by execution on a TrueNAS host.

```bash
# Syntax check a module (no project-wide check target exists)
bash -n lib/rep_apps.bash

# Dry run on a TrueNAS host (as root)
./bin/sync_truenas_servers --test --task=master_to_backup

# Real run, single subtask / single app or VM
./bin/sync_truenas_servers --task=master_to_backup --subtask=app_replication --app=immich
./bin/sync_truenas_servers --task=backup_to_master --subtask=vm_replication --vm=VM1 --vm=VM2
```

Deploy is via the VS Code SFTP extension (`.vscode/sftp.json`, `backup`/`master` profiles) — no build/deploy script in this repo.

See [lib/cli.bash](lib/cli.bash) (`Help`) for the full option matrix.

## Hooks
No `.claude/settings.json` present. No Claude-affecting hooks, auto-formatters, or blocked paths configured.

## Additional docs
@.claude/rules/architectural_patterns.md
