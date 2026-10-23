# sync_truenas_servers

Bash orchestrator that replicates apps, VMs, ZFS filesystems and snapshots between two
TrueNAS SCALE servers, in either direction.

## Purpose

`truenas-master` is the primary: always on, serving everything (the `data` hosts-file alias points
to it). `truenas-backup` is mostly powered off — started, synced by hand, powered off again. Its
jobs:

1. **Backup** — a copy of truenas-master's data, with snapshot history.
2. **Failover** — when truenas-master breaks: power on truenas-backup, repoint `data`, and every
   app and VM works with the last synced state, with nothing to install or configure by hand.
   So truenas-backup must be as **independent** of truenas-master as possible: after a sync,
   nothing on truenas-backup may refer to truenas-master (IP, hostname, paths) — not in app
   definitions, container config, nor an app's own database.
3. **Failback** — `backup_to_master` brings changes back, with the same rules in reverse.
4. **Occasional test bed** — trying an app change on truenas-backup first. Hence a running app on
   truenas-backup is left alone unless `--stop-running-apps` is passed; overwriting it is a
   per-run choice.

Both hosts have the same apps and VMs installed as separate instances (`<app>-master` /
`<app>-backup`), and whenever truenas-backup is on, both run at the same time (**dual-active**).
So this syncs content and definitions between two installs rather than priming a cold standby.
Only genuinely per-host values differ: IP/hostname, certificate, pool paths, external identities
(Plex claim/GUID) and the name suffix. Everything else — ports included — is identical on both
hosts, so repointing `data` is transparent. The goal covers **every** app on truenas-master,
current and future; [plans/app-replication.md](plans/app-replication.md) tracks the way there.

Built for one specific homelab: hostnames, pool names and dataset layout are assumptions baked
into the code. Read [Assumed layout](#assumed-layout) before pointing it at other hardware.

## What it does

A run executes the selected subtasks in this order:

| Subtask | What it does |
|---|---|
| `app_definition_replication` | Replicates app *definitions*. Reads the source app's `app.config`, rewrites per-host values with [config/app_config_mappings.json](config/app_config_mappings.json), then on the target: **creates** a missing app with the source's exact catalog version, **updates** a stopped one (only when both run the same catalog version), and **skips** a running one unless `--stop-running-apps` (stops it, updates, starts it again). Never deletes. Custom (compose) apps are skipped for now. Manual-only: not part of `all`. |
| `app_replication` | Replicates app *data*. Stops the app on both hosts, `rsync`s the directories listed in [config/apps.json](config/apps.json), starts it again. Optional `pre_action`/`post_action` hooks — Immich dumps its Postgres DB before and restores it after. Skips an app that isn't installed on both hosts, whose upstream versions differ, or (Immich) that isn't running on the source or the target — `--start-stopped-apps` starts it for the run. |
| `vm_replication` | Per VM: rewrites per-host paths, NIC and display ports with [config/vm_device_mappings.json](config/vm_device_mappings.json); deletes the target VM; removes target zvols/files the source no longer has; rsyncs file-backed disks and ISOs; replicates the zvols (latest snapshot); recreates the VM. A running VM is skipped unless `--stop-running-vms` (graceful stop, restart afterwards). |
| `zfs_replication` (all snapshots) | Drives `zfs_autobackup` over every dataset whose `autobackup:<task>_all_snapshots` property is `true`, including foreign snapshots (`--other-snapshots`). |
| `snapshot_rollup` | Thins snapshots of `media-ds` on the target with `zfs-rollup` (`hourly:48,daily:14,weekly:8,monthly:24,yearly:10`, prefix `auto`). |
| `zfs_replication` (latest snapshot) | Same, for datasets tagged `autobackup:<task>_latest_snapshot_only`. |

Datasets opt into ZFS replication by setting that property in TrueNAS, not by being listed in
config. Before replicating, the target datasets are unmounted; afterwards exactly those are
remounted (also when the run aborts). `zfs_autobackup` runs with `--rollback --keep-source=0
--keep-target=0 --allow-empty --snapshot-format {}-%Y-%m-%d_%H-%M --zfs-compressed --decrypt
--clear-refreservation --strip-path 2 --exclude-received`, plus `--other-snapshots` for the
all-snapshots scope.

`--subtask=all`, also the default when no subtask is given, runs everything except
`app_definition_replication`. The ZFS subtask has variants with/without rollup and per scope —
see `--help`.

## Requirements

- **TrueNAS SCALE** (developed against Goldeye 25.10). The script runs **on the host itself, as
  `root`**, on either host — it derives which side is local and which is remote from
  `hostname -s`.
- Host tools, all present on a stock TrueNAS: `jq`, `rsync`, `ssh`, `zfs`, `docker`, `midclt`,
  `flock`, `sendmail`, `systemd-run`.
- Two third-party repos cloned as **siblings** of this one: `../zfs_autobackup/` (with its
  `autobackup-venv/` built) and `../zfs-rollup/` (with `rollup.py` executable).
- Key-based SSH between the two hosts, through the config file `SSH_CONFIG_FILE` points at.

## Configuration

Copy [config/config.example.bash](config/config.example.bash) to `config/config.local.bash`
(untracked, required):

| Variable | Meaning |
|---|---|
| `EMAIL_TO` | Where the completion / failure mail goes. |
| `SSH_CONFIG_FILE` | SSH config defining `truenas-master` / `truenas-backup`. On these hosts: `/mnt/<pool>/homedir-ds/home/root/.ssh/config` — root's persistent home, since `/root` is replaced by every TrueNAS update. |
| `LOG_RETENTION_DAYS` | Optional, default 365. Prunes `logs/`. |
| `DUMP_RETENTION_DAYS` | Optional, default 365. Prunes old Immich DB dumps. |

[config/apps.json](config/apps.json) lists the apps and, per app, the directories to copy
(`app_base_path` + `app_dir_list`). That list is a contract: it separates shared content from
per-instance identity — Plex's `Preferences.xml` holds the server GUID and plex.tv claim, so it is
deliberately not copied. An entry with only a `name` is definitions-only.
[config/app_divergences.md](config/app_divergences.md) records, per app, which values differ
between the hosts and why — the spec for the mapping file.

## Usage

Always start with a dry run:

```bash
./bin/sync_truenas_servers --test --task=master_to_backup
```

```bash
# Everything, master -> backup
./bin/sync_truenas_servers --task=master_to_backup

# One subtask, one app
./bin/sync_truenas_servers --task=master_to_backup --subtask=app_replication --app=immich

# Full app sync: definitions first, then data; every target app ends in the state it started in
./bin/sync_truenas_servers --task=master_to_backup --subtask=app_definition_replication --subtask=app_replication --stop-running-apps --start-stopped-apps

# Two VMs, backup -> master, stopping them first if they happen to be running
./bin/sync_truenas_servers --task=backup_to_master --subtask=vm_replication --vm=VM1 --vm=VM2 --stop-running-vms

# Full option matrix
./bin/sync_truenas_servers --help
```

Worth knowing:

- **`--test` still stops and starts apps and containers**, so the run exercises the real
  sequence; it suppresses everything that changes data or definitions.
- **The run detaches** as a transient systemd unit (`systemd-run`) and the foreground tails the
  log, so a lost session doesn't touch it. Not `nohup`: a child of a `sudo su` shell stays under
  sudo's `log_subcmds` tracer, and once sudo loses track of it every exec fails with `ENOSYS`.
- **It always exits 0.** Success or failure is reported by email and in `logs/`; don't chain `&&`
  off a run.
- **A failing item is skipped, a failing run aborts.** An app or VM in the wrong state, not
  installed, or on a different version is skipped with the fix printed, and counted in the
  summary. Anything that makes the run itself unsafe aborts it, after restoring: unmounted
  datasets are remounted, a source VM it stopped is started, apps changed by
  `--start-stopped-apps`/`--stop-running-apps` return to their state. Not restored: apps stopped
  for the data copy (start them by hand), and a target VM it stopped (a warning names it — it was
  mid-rebuild).
- **Concurrent runs are refused:** a lock in `logs/` blocks a second run on the same host, and the
  peer's lock is checked too.
- `logs/` and `tmp/` are runtime only; `tmp/` keeps the per-VM and per-app JSON of the last run —
  where to look when a transform did something unexpected. `tmp/apps` is root-only (it holds app
  secrets).

Running the pair (failover, preparing truenas-backup for a new app, recovering from known
failures): [docs/operations.md](docs/operations.md).

## Deploy

Edited on Windows, deployed over SFTP with the VS Code SFTP extension
([.vscode/sftp.json](.vscode/sftp.json), one profile per host, `watcher.autoUpload` on) to
`/mnt/<fast pool>/encrypted-ds/app-ds/sync_truenas_servers`. No build step, no separate sync step.
SFTP doesn't carry the exec bit — once per host after the first push:
`chmod +x bin/sync_truenas_servers ../zfs-rollup/rollup.py`. `zfs_autobackup` also needs its
`autobackup-venv/` built on the host; SFTP excludes it.

## Assumed layout

Hardcoded or derived, not configurable:

- Hostnames **`truenas-master`** (`192.168.0.10`) and **`truenas-backup`** (`192.168.0.60`); the
  local host's role comes from its own hostname.
- Pools ([lib/common.bash](lib/common.bash) `Resolve_pool`): master has `master-pool` plus the fast
  `ssdmaster-pool`; backup uses `backup-pool` for both. Apps and VMs live on the fast pool.
- Datasets under `<pool>/encrypted-ds/` — `app-ds`, `vm-ds`, `media-ds`. They must be unlocked and
  mounted before a run.
- Apps are named `<app>-<server_id>`; containers `ix-<app>-<server_id>-<component>-<N>`.

## Repo layout

| Path | |
|---|---|
| [bin/sync_truenas_servers](bin/sync_truenas_servers) | Entry point: resolves paths, sources config and modules, takes the lock, detaches, dispatches subtasks, mails the log. |
| [lib/](lib/) | One module per concern — CLI, shared helpers, app / app-definition / VM / filesystem replication, rollup, Docker, Immich DB. |
| [config/](config/) | `apps.json`, the two mapping files, the divergence spec, the config template. |
| [tools/](tools/) | Standalone diagnostics, run by hand ([mountwatch.sh](tools/mountwatch.sh)). |
| [docs/](docs/) | [operations](docs/operations.md), [TrueNAS API behaviour](docs/truenas-api.md), [dataset-busy investigation](docs/dataset-busy-investigation.md). |
| [plans/](plans/) | Open work only. |
| [AGENTS.md](AGENTS.md) + [.claude/rules/](.claude/rules/) | Orientation and conventions for coding agents. |

## Known issues

- **Intermittent `dataset is busy`** during ZFS receive — cause unknown; diagnostics fire on
  failure. [docs/dataset-busy-investigation.md](docs/dataset-busy-investigation.md).
- **VM UUID collision** on the target's delete → recreate under `--stop-running-vms` —
  [plans/backlog.md](plans/backlog.md), recovery in [docs/operations.md](docs/operations.md#vm-orphan-libvirt-domain).
- **Host references inside app data aren't rewritten yet** (grafana's Loki data source, forgejo's
  `app.ini`, syslog-ng/promtail config) — [plans/app-replication.md](plans/app-replication.md).

No test suite; validation is `bash -n`, `shellcheck -x` and `--test` runs against the real servers.
