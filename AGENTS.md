# sync_truenas_servers

## Project overview
Bash orchestrator replicating apps, VMs, ZFS filesystems and snapshots between two TrueNAS SCALE
hosts. `truenas-master` is the always-on primary; `truenas-backup` is mostly powered off and must
be able to fully take over (failover, independent of truenas-master) and hand back (failback).
Both hosts run the same apps/VMs as separate instances (dual-active whenever truenas-backup is
on), so replication syncs content and definitions, rewriting every per-host value. Purpose,
subtasks and usage: [README.md](README.md). Runs on a host, as `root`; logs in `logs/`, run-state
JSON in `tmp/vms/` and `tmp/apps/`.

## Tech stack
- Bash 5 (`#!/usr/bin/bash`), `jq` (1.6 on the hosts)
- `midclt` (TrueNAS middleware) for app/VM queries and control — verified behaviour in
  [docs/truenas-api.md](docs/truenas-api.md)
- `zfs` / `zfs_autobackup` for replication, `rsync` for app data and file-backed VM disks, `ssh`
- Docker CLI for Immich's Postgres containers; `sendmail` for the result email

## External repos
`zfs_autobackup` and `zfs-rollup` are **third-party tools**, expected as siblings of this repo
(`${SCRIPT_DIR}/../../`): `../zfs_autobackup/` (`autobackup-venv/bin/python -m
zfs_autobackup.ZfsAutobackup`) and `../zfs-rollup/` (`rollup.py`). Locally cloned to
`C:\Apps\zfs_autobackup` and `C:\Apps\zfs-rollup`, SFTP-synced to the same sibling layout. To
verify their behaviour, read those local clones — they are what's deployed (may lag upstream).
New host: `chmod +x bin/sync_truenas_servers` and `zfs-rollup/rollup.py` after the first push;
`zfs_autobackup` needs its `autobackup-venv/` built there (SFTP excludes it).

## Key files
- [bin/sync_truenas_servers](bin/sync_truenas_servers) — entry point: paths, config, host roles,
  lock, re-exec as a `systemd-run` unit with `--running_in_background`, subtask dispatch, email.
- [lib/](lib/) — sourced flat into one shell (shared globals):
  - [common.bash](lib/common.bash) — `Execute_command`, `Background_error`, cleanup stack,
    `Resolve_pool`, `Resolve_direction`.
  - [cli.bash](lib/cli.bash) — `Process_command_line_options`, `Help` (full option matrix).
  - [rep_app_definitions.bash](lib/rep_app_definitions.bash) — `app_definition_replication`:
    extract → transform (`config/app_config_mappings.json`) → `app.create`/`app.update`.
  - [rep_apps.bash](lib/rep_apps.bash) — `app_replication` (`config/apps.json`), app control
    helpers, version-mismatch message.
  - [immich_db.bash](lib/immich_db.bash) — `Backup_immich_DB` / `Restore_immich_DB`, Immich's
    `pre_action` / `post_action`.
  - [rep_vms.bash](lib/rep_vms.bash) — `vm_replication` (`config/vm_device_mappings.json`).
  - [rep_filesystems.bash](lib/rep_filesystems.bash) — `zfs_autobackup` runs, mount handling,
    failure diagnostics.
  - [snap_rollup.bash](lib/snap_rollup.bash) — `zfs-rollup` on the target.
  - [docker.bash](lib/docker.bash) — container stop/start with state waits.
- [config/](config/) — `apps.json`, `app_config_mappings.json` (spec:
  `app_divergences.md`), `vm_device_mappings.json`, `config.example.bash` → untracked
  `config.local.bash` (`EMAIL_TO`, `SSH_CONFIG_FILE`).
- [tools/mountwatch.sh](tools/mountwatch.sh) — standalone watcher for the
  [dataset-busy investigation](docs/dataset-busy-investigation.md); not part of a run.
- [docs/](docs/) — operations procedures, TrueNAS API behaviour, investigations.
- [plans/](plans/) — open work only; deleted once implemented.

## Essential commands
No build, no test suite. Validate with `bash -n <file>` and `shellcheck -x <file>`, then a
`--test` run on a host (as root):

```bash
./bin/sync_truenas_servers --test --task=master_to_backup
./bin/sync_truenas_servers --test --task=master_to_backup --subtask=app_definition_replication --app=grafana
```

Run commands are given for **truenas-backup** unless asked otherwise. The run always exits 0;
the result is in the email and the log.

Deploy: VS Code SFTP extension, `watcher.autoUpload` on — a save uploads to the active profile's
host (default truenas-backup). Never instruct a manual sync. Other host = switch profile.

## Docs rule
Docs describe the current state only — no history, no references to finished plans. Plans hold
open work and are deleted when done; what stays true moves into the docs.

## Additional docs
@.claude/rules/architectural_patterns.md
