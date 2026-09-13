# sync_truenas_servers

Bash orchestrator that replicates apps, VMs, ZFS filesystems and snapshots between two
TrueNAS SCALE servers, in either direction. The pair is **dual-active** — both hosts have the
same apps and VMs installed independently and may run at the same time — so this syncs
*content* between two live installs rather than priming a cold standby.

Built for one specific homelab: hostnames, pool names and dataset layout are assumptions baked
into the code. Read [Assumed layout](#assumed-layout) before pointing it at other hardware.

## What it does

One entry script dispatches four replication subtasks:

| Subtask | What it does |
|---|---|
| `app_replication` | Stops the app on **both** hosts, `rsync`s the directories listed in [config/apps.json](config/apps.json), starts it again. An app may declare a `pre_action`/`post_action` hook — Immich uses them to dump its Postgres DB before and restore it after. |
| `vm_replication` | Reads the source VM definition via `midclt`, rewrites per-host paths, NIC and display ports using [config/vm_device_mappings.json](config/vm_device_mappings.json), replicates the zvols, then recreates the VM on the target. |
| `zfs_replication` | Drives `zfs_autobackup` over every dataset carrying the `autobackup:<task>_<scope>` ZFS property. Datasets opt in by setting that property in TrueNAS, not by being listed in config. |
| `snapshot_rollup` | Thins snapshots on the target with `zfs-rollup`. |

A fifth subtask, `app_definition_replication`, replicates app *definitions* (`midclt app.create` /
`app.update`, never `app.delete`). It is **manual-only and not yet verified on a live host** —
see [plans/06-phase0-findings.md](plans/06-phase0-findings.md) before using it.

`--subtask=all`, which is also the default when no subtask is given, runs the four subtasks
above and deliberately excludes `app_definition_replication`.

## Requirements

- **TrueNAS SCALE** (developed against Goldeye 25.10). The script runs **on the host itself, as
  `root`**, on either `truenas-master` or `truenas-backup` — it derives which direction is
  local and which is remote from `hostname -s`.
- Host tools, all present on a stock TrueNAS: `jq`, `rsync`, `ssh`, `zfs`, `docker`, `midclt`,
  `flock`, `sendmail`.
- Two third-party repos cloned as **siblings** of this one:
  - `../zfs_autobackup/` — with its `autobackup-venv/` built
  - `../zfs-rollup/` — with `rollup.py` executable
- Key-based SSH between the two hosts, reachable through the config file that
  `SSH_CONFIG_FILE` points at.

## Configuration

Copy [config/config.example.bash](config/config.example.bash) to `config/config.local.bash`
(untracked, required) and fill in:

| Variable | Meaning |
|---|---|
| `EMAIL_TO` | Where the completion / failure mail goes. |
| `SSH_CONFIG_FILE` | SSH config defining the `truenas-master` / `truenas-backup` hosts used for every remote call. |
| `LOG_RETENTION_DAYS` | Optional, default 365. Prunes `logs/`. |
| `DUMP_RETENTION_DAYS` | Optional, default 365. Prunes old Immich DB dumps. |

Which apps are replicated, and which of their directories, is declared in
[config/apps.json](config/apps.json). That directory list is a contract, not a convenience: it
separates shared content from per-instance identity (Plex's `Preferences.xml` holds the server
GUID, so it is deliberately not synced). Do not broaden it without checking what identity files
sit next to the ones you add.

## Usage

Always start with a dry run:

```bash
./bin/sync_truenas_servers --test --task=master_to_backup
```

Then the real thing:

```bash
# Everything, master -> backup
./bin/sync_truenas_servers --task=master_to_backup

# One subtask, one app
./bin/sync_truenas_servers --task=master_to_backup --subtask=app_replication --app=immich

# Two VMs, backup -> master, stopping them first if they happen to be running
./bin/sync_truenas_servers --task=backup_to_master --subtask=vm_replication --vm=VM1 --vm=VM2 --stop-running-vms

# Full option matrix
./bin/sync_truenas_servers --help
```

Worth knowing before the first run:

- **`--test` still touches app and container state.** It suppresses everything that changes
  data, but apps and Docker containers are genuinely stopped and started so the run exercises
  the real sequence. That is intentional, not a leak.
- **The run detaches.** The foreground process re-execs itself under `nohup` and tails the log,
  so closing the terminal does not kill it. It **always exits 0** — success or failure is
  reported by email and in `logs/`, never by the exit code, so don't chain `&&` off a run.
- **A running VM is skipped by default.** `--stop-running-vms` opts into stopping it (always
  gracefully) and starting it again afterwards.
- **Concurrent runs are refused**: a lock in `logs/` blocks a second run on the same host, and
  the peer host's lock is checked too.
- `logs/` and `tmp/` are runtime only and gitignored; `tmp/` keeps the per-VM and per-app JSON
  of the last run, which is where to look when a transform did something unexpected.

## Deploy

Edited on Windows, deployed over SFTP with the VS Code SFTP extension
([.vscode/sftp.json](.vscode/sftp.json), one profile per host, `watcher.autoUpload` on), to
`/mnt/<pool>/encrypted-ds/app-ds/sync_truenas_servers` on each server. There is no build step
and no separate sync step.

SFTP does not carry the exec bit, so once per host after the first push:

```bash
chmod +x bin/sync_truenas_servers
```

## Assumed layout

These are hardcoded or derived, not configurable:

- Hostnames **`truenas-master`** and **`truenas-backup`**; the local host's role comes from its
  own hostname, the peer is the other one.
- Pools resolve in `lib/common.bash::Resolve_pool`: master has `master-pool` plus a fast
  `ssdmaster-pool`; backup uses `backup-pool` for both. Apps and VMs live on the fast pool.
- Datasets under `<pool>/encrypted-ds/` — `app-ds`, `vm-ds`, `media-ds`. Encrypted datasets must
  be unlocked and mounted before a run; the script aborts if they are not.
- Apps are installed per host as `<app>-<server_id>` (`immich-master`, `immich-backup`), with
  containers named `<app>-<server_id>-<component>-<N>`.

## Repo layout

| Path | |
|---|---|
| [bin/sync_truenas_servers](bin/sync_truenas_servers) | Entry point: resolves paths, sources config and modules, backgrounds itself, dispatches subtasks, mails the log. |
| [lib/](lib/) | One module per concern — CLI, shared helpers, app / VM / filesystem / app-definition replication, rollup, Docker, Immich DB. |
| [config/](config/) | `apps.json`, the two mapping files, and the config template. |
| [plans/](plans/) | Numbered analysis and improvement plans, the open-issue log, and the design reasoning behind the awkward parts. Start at [plans/README.md](plans/README.md). |
| [AGENTS.md](AGENTS.md) + [.claude/rules/](.claude/rules/) | Orientation and conventions for coding agents working in this repo. |

## Status and known issues

In daily production use for app, VM and ZFS replication. Three things a reader should know:

- **Intermittent `dataset is busy` failures** during ZFS receive — external cause, still
  unresolved after a controlled reproduction. Diagnostics fire automatically on failure.
  [plans/08](plans/08-known-operational-issues.md).
- **VM UUID collision** on the target's delete→recreate step under `--stop-running-vms`: the
  transform copies the source VM's UUID verbatim, which can collide with a not-yet-undefined
  libvirt domain. Recovery steps in [plans/09](plans/09-stop-running-vms-optarg.md).
- **`app_definition_replication` is unverified.** Its `midclt` payload shapes were written
  offline and never checked against a live host — always `--test` first.

There is no test suite; validation is by `bash -n`, `shellcheck -x` and `--test` runs against
the real servers.
