# Investigation: intermittent "dataset is busy" on ZFS receive

**Status: cause unknown.** Investigated for a long time without a confirmed cause. Everything
below is an *observation* or a *test result* with its evidence — none of it is proven beyond
doubt, and any of it may rest on a wrong assumption. Re-check before building on it.

## Symptom

`zfs_autobackup` occasionally fails partway through a filesystem-replication scope. Most often
`latest_snapshot_only` on the large `backup-*-ds` datasets (100 GB–3 TB files), not only there —
on 2026-09-13 it failed on `media-ds` in the `all_snapshots` scope. Never seen on VM zvols.

```
! [Target] STDERR > cannot receive incremental stream: dataset is busy
! [Source] Command "ssh ... zfs send ..." returned exit code 255 (valid codes: [0])
! [Target] Command "zfs recv -u -x refreservation -v -s backup-pool/encrypted-ds/backup-htpc-ds" returned exit code 1 (valid codes: [0])
! [Source] master-pool/encrypted-ds/backup-htpc-ds: FAILED: Last command returned error
! 1 dataset(s) failed!
ERROR: ZFS Replication failed
```

A separate, harmless symptom sometimes follows on the next run: a stale resume token
(`cannot resume send: ... no longer exists` / `Aborting resume, we dont want that snapshot
anymore`). Reading `ZfsAutobackup.py` (`_plan_sync`): the tool recomputes the plan every run and
drops a token that no longer fits — self-healing.

## Observations

- **Mid-transfer, not up front.** Most often on `backup-desktop-ds` — the largest dataset and the
  first one processed — after it has already been transferring for hours. If accurate, something
  changes *during* the transfer window rather than being present at the start. The `media-ds`
  failure, however, came after 18 short sequential incrementals (29 minutes), not one long one.
- **Larger files, more failures** — matches "longer window, more chance of interference", which
  is a correlation, not a cause.
- **A controlled run on 2026-09-13 still failed.** Beforehand on truenas-backup: both periodic
  snapshot tasks disabled; no Cloud Sync / TrueCloud / Rsync / Replication tasks; the
  `zfs-rollup` cron disabled; no scrub that month and none running; no SMART test running; the
  four `backup-*` SMB shares disabled; NFS / iSCSI / NVMe-oF stopped with no shares; no resume
  tokens, clones, or holds beyond zfs_autobackup's own; no `zfs send`/`recv` process. It failed on
  `media-ds` anyway. `middlewared.log` recorded nothing from 53 minutes before the failure until
  after it.
- **No remount during the transfer** (watcher, see below): over `backup-pool/encrypted-ds` it saw
  exactly two changes in that run — the script's own `zfs umount` (15:33:04) and the cleanup's
  remount after the failure (16:02:07). `mounted` read `no` throughout.
- **`fuser` and `smbstatus` showed nothing** on the target at the moment of failure: no process
  on the mountpoint, no locked files, no SMB sessions. An EBUSY from `zfs recv` suggests a ZFS
  *long hold*, which neither tool can see.
- **Disabling the SMB share on the source (truenas-master) made failures rarer but did not stop
  them.** Could be coincidence; could mean the share matters some other way.
- **No client is known to connect to truenas-backup directly** — clients use truenas-master /
  `data`; the one known exception was a single manual test years ago.
- **Source-side activity looks less likely:** `zfs send` reads an immutable snapshot, never the
  live filesystem, and `dl-ds` — heavily used via SMB on the source, including during runs — has
  never failed. Still captured in diagnostics as a secondary data point.

## How zfs_autobackup behaves (read from the local clone's source)

- **No retry logic anywhere** (`ZfsAutobackup.py`, `ExecuteNode.py`). `sync_datasets()` loops per
  dataset with its own `try/except`, counts failures and continues — matching logs where the next
  datasets complete after one failed.
- **Exit code = number of failed datasets (capped at 255)**, but 255 also means a top-level
  exception or `KeyboardInterrupt` — this wrapper can't tell "N failed, rest done" (1–254) from
  "crashed" (255).
- **Transfers don't overlap:** `CmdPipe.execute()` loops until every fd is EOF and every process
  has exited before closing them and calling the exit handlers (`CmdPipe.py:133-168`).
- **No remount mid-run:** `automount()` runs only on an *initial* transfer
  (`ZfsDataset.py:803-807`); `--rollback` fires at most once, before the first transfer. Between
  two receives the tool touches the target only with `zfs hold`, `zfs holds`, `zfs release`.
- **`_pre_clean()` destroys obsolete target snapshots right before the first `zfs recv`**
  (`ZfsDataset.py:999-1031`); with `--keep-target=0` that is everything except the common
  snapshot.
- **`--other-snapshots` sends foreign snapshots but never manages them:** `thin_list()` keeps only
  names that parse with the snapshot format; `auto-*` don't, so they're never destroyed. That is
  why an `all_snapshots` run logs one "Destroying" pair then a long series of pure transfers.

## Explanations tested — not reproduced (could still be wrong)

| Explanation | Test | Result |
|---|---|---|
| A scheduled TrueNAS task on the target | 2026-09-13 controlled run, all schedules off, `middlewared.log` silent | still failed |
| Target-side SMB activity (and a "disable the target share during sync" feature) | same run: shares disabled, `smbstatus` zero sessions/locks | still failed — don't build that feature on this evidence alone |
| Something remounting the target mid-transfer | 10 s watcher | no remount seen |
| A mount surviving in a container's namespace (`ix-plex-backup-plex-1` bind-mounts `media-ds`) | with Plex running: `zfs umount …/media-ds`, then scanned every `/proc/[0-9]*/mounts` | nothing found; the watcher never saw `mounted=no` with a namespace count > 0 |
| Overlapping send/recv pairs inside zfs_autobackup | read the execution chain (above) | not possible per the code |
| A stale handle making `umount` silently fail | timing: would fail at the start, not hours in | doesn't fit the observed timing |
| Veeam backup touching `backup-desktop-ds` | a kill-test caught a Veeam SMB lease | the failure also happens when Veeam is definitely not running — lease likely an artefact of testing in Veeam's window |

Rejected as a workaround: **force-unmount (`-f`)** — unconfirmed that it clears whatever blocks
`zfs recv`, and it risks corrupting a write in progress; worse than a failed sync.

## Live leads

1. **An asynchronous `zfs destroy` still holding the dataset when the receive begins.**
   `_pre_clean()` destroys snapshots right before the first `zfs recv`; `zfs destroy` returns
   when its transaction commits while block freeing continues in the background. Fits: invisible
   to userspace tools, worse with bigger snapshots, intermittent, and it is the only heavy ZFS
   operation aimed at the target dataset right before the failing call. **Not verified** in the
   ZFS source. Observable: the pool's `freeing` being non-zero at the moment of failure — now
   captured by the diagnostics.
2. **Samba itself** (service or share definitions, not a client). On 2026-09-13 `media-ds` failed
   while every `backup-*-ds` succeeded, and `media-ds` was the one whose share was enabled. Running
   experiment: every share on truenas-backup enabled and the **SMB service stopped**. If failures
   continue, Samba is out entirely; if they stop, look inside Samba. Result pending — no
   conclusive full sync yet. ⚠️ Failover must start the service by hand
   ([operations.md](operations.md#failover--truenas-backup-takes-over)).

## Diagnostics in place

On any mount/umount or `zfs_autobackup` failure, `l_Print_diagnostics` in
[lib/rep_filesystems.bash](../lib/rep_filesystems.bash) captures, per dataset, on target **and**
source: `fuser -vm`; mount namespaces still holding the dataset (only while the host reports it
unmounted); snapshots with a hold (`userrefs > 0`); running `zfs send`/`recv` processes; the
pool's `freeing`, health and scan state; `smbstatus -L`.

[tools/mountwatch.sh](../tools/mountwatch.sh) is a standalone watcher to run by hand on the
target during a sync: it logs whenever a dataset's `mounted` state, namespace count or the pool's
`freeing` (zero/non-zero) changes. Start it with `setsid` (a `nohup … &` under sudo gets killed),
from the deployed copy, never from `/tmp`.

## Next steps

1. Let the diagnostics catch a real failure; check whether `freeing` was non-zero (lead 1) and
   whether the SMB-off experiment changed anything (lead 2).
2. If `freeing` correlates, confirm in the ZFS source which paths return EBUSY from
   `dmu_recv_begin_check()` before building anything. A fix would then be a bounded wait for
   `freeing` to drain before `zfs_autobackup` runs — in this wrapper.
3. Independent of the cause: exit-code-aware partial-failure handling, so one failed dataset
   doesn't abort the rest of the run ([plans/backlog.md](../plans/backlog.md)).
