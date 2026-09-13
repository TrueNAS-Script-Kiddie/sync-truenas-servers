# Plan 08 — Known operational issues (external causes, not code bugs)

Three recurring issues observed in real runs. #1 and #2 are rooted **outside**
this script's own logic; #3 was a **timing bug in the script's own wait logic**
— diagnosed from a real 2026-07-07 failure and **fixed**. Documented so they
aren't mistaken for regressions.

---

## 1. Immich Postgres collation-version-mismatch warning

**Symptom:** every `Backup_immich_DB` run prints a Postgres `WARNING` about a
collation version mismatch (the OS-provided collation library drifted
version since the databases were created — typically a base-image/package
update).

**Impact: none.** It's a `WARNING`, not an error — `pg_dumpall` still exits
`0`, so `Backup_immich_DB` proceeds normally (confirmed directly by a real
run: the warnings printed and the backup completed successfully right after).
Plan 01 item 2d's `|| Background_error` conversion does not and should not
trigger on this.

**Fixing it is out of scope for this project** — Immich/Postgres
administration, not something `sync_truenas_servers` should automate. If you
ever do: Postgres's own hint is `ALTER DATABASE ... REFRESH COLLATION
VERSION`, but **don't run that blindly** — per Postgres's own docs, a
mismatch can mean indexes on collation-sensitive columns are silently using
a different sort order than when they were built. Safe order: identify
affected indexes/objects, `REINDEX` them, *then* `REFRESH COLLATION VERSION`
— not the other way round. Whether this is worth doing for Immich's specific
schema is a separate decision from this codebase.

**Action: none.** Documented so a future `pg_dumpall` run showing these
lines isn't mistaken for something today's fixes broke.

---

## 2. Intermittent "dataset is busy" ZFS receive failures — UNRESOLVED

**Symptom:** `zfs_autobackup` occasionally fails partway through a
filesystem-replication scope. Most often `latest_snapshot_only` on the large
`backup-*-ds` datasets, but not exclusively — a 2026-09-13 run failed on
`media-ds` in the `all_snapshots` scope:

```
! [Target] STDERR > cannot receive incremental stream: dataset is busy
! [Source] Command "ssh ... zfs send ..." returned exit code 255 (valid codes: [0])
! [Target] Command "zfs recv -u -x refreservation -v -s backup-pool/encrypted-ds/backup-htpc-ds" returned exit code 1 (valid codes: [0])
! [Source] master-pool/encrypted-ds/backup-htpc-ds: FAILED: Last command returned error
! 1 dataset(s) failed!
ERROR: ZFS Replication failed
```

A related, separate symptom seen alongside it: a subsequent run reported a
stale resume token (`cannot resume send: ... no longer exists` / `Aborting
resume, we dont want that snapshot anymore`). Confirmed by reading
`ZfsAutobackup.py` directly (`_plan_sync`, ~line 1217): the tool recomputes
what needs syncing fresh every run and discards a resume token that no
longer matches that plan. **This is self-healing, not a bug** — resolves
itself automatically, not a concern on its own.

### Established facts

- **A controlled run on 2026-09-13 reproduced the failure with every known
  external cause removed.** Beforehand `truenas-backup` had: both periodic
  snapshot tasks disabled, no Cloud Sync / TrueCloud / Rsync / Replication tasks,
  the `zfs-rollup` cron disabled, no scrub that month and none running, no SMART
  self-test running, the four `backup-*` SMB shares disabled, NFS / iSCSI /
  NVMe-oF all STOPPED with zero shares, no resume tokens, no clones, no snapshot
  holds beyond zfs_autobackup's own, and no `zfs send`/`recv` process. **It failed
  anyway**, on `media-ds`, after 18 sequential incrementals had been received
  successfully in 29 minutes. `middlewared.log` recorded nothing at all from 53
  minutes before the failure until after it.
- **The target dataset is never remounted during the transfer.** A 10-second
  watcher over `backup-pool/encrypted-ds` logged exactly two changes in that run:
  the script's own `zfs umount` at 15:33:04 and the cleanup's remount at 16:02:07
  after the failure. `mounted` read `no` continuously in between.
- **`fuser` and `smbstatus` cannot see the cause.** At the moment of failure both
  were clean on the target: no process on the mountpoint, no locked files, no SMB
  sessions at all. EBUSY from `zfs recv` means ZFS holds a *long hold* on the
  dataset, and neither tool can observe the two cheapest sources of one — a mount
  surviving in another mount namespace, or a snapshot hold.
- **This is a mid-transfer failure, not a quick pre-flight check.** For
  these 100GB–3TB-file datasets, a send/receive can already be hours into
  moving data by the time this hits. Confirmed directly from you: the
  failure occurs *most often* on `backup-desktop-ds` — the **largest**
  dataset and the **first** one processed — specifically *after* it has
  already been transferring successfully for hours. This kills a
  "stale/pre-existing handle" explanation on two independent counts: (1) no
  "waiting its turn" story is available for the first-processed dataset,
  and (2) a precondition-style rejection would fire before any data moves,
  not hours into an already-progressing transfer. Something changes
  **during** the transfer window; it isn't present before the run starts.
- **Predominantly on the large-file "backup-\*-ds" datasets** (100GB–3TB files),
  and on 2026-09-13 also on `media-ds`; never yet on VM zvols. The working
  hypothesis (huge files → longer send/receive window → more opportunity for
  something to interfere) matches the pattern but is not proof of cause, and the
  `media-ds` failure came after eighteen *short* transfers rather than one long
  one.
- **`--other-snapshots` sends foreign snapshots but never manages them.**
  `thin_list()` works on `our_snapshots`, filtered by `is_ours()` →
  `strptime(name, snapshot_time_format)`; `auto-*` names don't parse, so they are
  never "ours", never obsolete and never destroyed. That is why an
  `all_snapshots` run logs a single "Destroying" pair and then a long series of
  pure transfers — useful when reading a failure log.
- **`zfs_autobackup` has no retry logic anywhere** — confirmed by reading
  `zfs_autobackup/ZfsAutobackup.py` and `ExecuteNode.py` directly (per this
  project's convention of reading the local third-party clone rather than
  guessing). `sync_datasets()` loops per-dataset with its own
  `try/except`, incrementing a fail count and continuing to the next
  dataset — exactly what the logs show (`dl-ds`/`shared-ds` completing
  right after `backup-htpc-ds` failed). The **exit code is the fail count**
  (capped at 255), but 255 is *also* separately used for a top-level
  exception or `KeyboardInterrupt` — so this wrapper currently can't tell
  "1 dataset failed, everything else the tool could finish, it did" (1–254)
  apart from "the whole tool crashed" (255). Any retry logic would have to
  be built into this wrapper; there's no CLI flag for it.
- **Disabling the SMB share on the source (master) reduces frequency but
  doesn't eliminate it** — suggesting the busy-ness isn't (only) the share,
  and/or the source-side share was never the actual mechanism (see below).
- **No client is known to ever connect to `truenas-backup` directly** — all
  real backup clients have always pointed at `master`/the `data` alias; the
  one exception is a single manual test from the Windows desktop, years ago,
  during initial setup. Together with the 2026-09-13 readings on the target —
  `smbstatus -p` and `-S` both empty, no locked files, and the failure happening
  regardless — this closes out the client-side explanations entirely.
- **`zfs send` only ever reads from an immutable snapshot, never the live
  mounted filesystem** (established via the send/recv mechanics discussion:
  the destination side receives into the *live* dataset and leaves a
  matching snapshot behind, but the source side is always snapshot→stream,
  never touching the live source filesystem). This is why heavy SMB
  activity on the **source** dataset is expected to be harmless — and
  `dl-ds` is a live counter-example: it's used heavily via SMB on the
  source constantly, including during testing, and has never once produced
  a "dataset is busy" failure. **Not called 100% harmless** — source-side
  SMB activity is still captured in diagnostics as a secondary data point —
  but it's a clearly weaker lead than target-side activity, and the
  practical guidance is: look at the target side first.

### Ruled out

- **A scheduled TrueNAS-internal task on the target.** Falsified by the
  2026-09-13 controlled run: every schedule was disabled or verified inactive,
  `middlewared.log` was silent across the whole window, and it failed regardless.
- **Target-side SMB activity — and with it the "disable the target share for the
  full sync duration" feature.** Falsified by the same run: the relevant shares
  were disabled and `smbstatus` showed zero sessions and zero locks. Do not build
  that feature.
- **Anything remounting the target dataset mid-transfer.** Falsified by the
  watcher timeline above.
- **A mount surviving in a container's mount namespace.** TrueNAS apps are Docker
  containers with host paths bind-mounted, and `ix-plex-backup-plex-1` does bind
  `/mnt/backup-pool/encrypted-ds/media-ds` — so this fitted the signature of an
  EBUSY that `fuser` cannot see. Tested deterministically on 2026-09-13 with Plex
  running: `zfs umount backup-pool/encrypted-ds/media-ds`, then a scan of every
  `/proc/[0-9]*/mounts`, returned nothing. The host unmount propagates into the
  container namespaces, so no app can pin a dataset this way on this system —
  which generalises to every dataset, not just `media-ds`. The watcher agrees
  from the other side: across a full successful run it never once recorded
  `mounted=no` together with a non-zero namespace count.
- **A teardown race between consecutive send/recv pairs inside `zfs_autobackup`.**
  Falsified by reading the whole execution chain (`CmdPipe.py`, `ExecuteNode.py`,
  `ZfsNode.py`, `ZfsDataset.py`, `ZfsAutobackup.py`): `CmdPipe.execute()` loops
  until every filedescriptor is EOF **and** every process reports `poll() is not
  None`, only then closes them and calls the exit handlers
  (`CmdPipe.py:133-168`). Two transfers cannot overlap at process level. The same
  read shows `automount()` runs only on an *initial* transfer
  (`ZfsDataset.py:803-807`), so the tool never remounts the target mid-run — an
  independent confirmation of the watcher result — and `--rollback` fires at most
  once, before the first transfer. Between two consecutive receives the tool
  touches the target with metadata calls only: `zfs hold`, `zfs holds`,
  `zfs release`.
- **Stale/pre-existing handle causing `umount` itself to silently fail**
  (which motivated plan 01 item 12 — checking `umount`'s exit code). Falsified
  by the mid-transfer timing above: this theory predicted the *next* failure
  would surface immediately at the start of a run, which isn't what happens.
  Item 12 is still a correct, worthwhile fix in general (checking an ignored
  exit code is just correct practice, and it might still catch a genuinely
  different, rarer scenario), but it does not address this dominant failure
  mode.
- **Force-unmounting (`-f`) as a workaround** — deliberately rejected, not
  just deprioritized. Force would push the unmount through even with an open
  handle, but (a) it's unconfirmed this actually clears whatever ZFS-internal
  state blocks a subsequent `zfs recv`, and (b) more importantly it risks
  corrupting/truncating whatever a client is actively writing at that exact
  moment — a materially worse outcome than the sync simply failing and
  retrying next cycle.
- **Veeam backup.** A deliberate kill-test (killing `zfs_autobackup`
  mid-run to force a diagnosable failure) captured an active Veeam SMB
  lease on `backup-desktop-ds` on the target at the time — initially a
  promising lead, since Veeam running against the same dataset mid-transfer
  would fit the "something touches the target mid-window" pattern. **Retracted**
  on your direct evidence: the real failure is confirmed to also occur when
  Veeam is definitely not running (you know Veeam's schedule and have run
  the sync outside it). The lease seen in the kill-test is most likely just
  an artifact of testing during Veeam's own window — correctly captured by
  the diagnostic, but not causal.

### Live leads (unconfirmed, in priority order)

1. **An asynchronous `zfs destroy` still holding the dataset when the receive
   begins.** `_pre_clean()` destroys every obsolete target snapshot immediately
   before the first `zfs recv` (`ZfsDataset.py:999-1031`), and `--keep-target=0`
   makes that everything except the common snapshot. `zfs destroy` returns as
   soon as its transaction commits while block freeing continues in the
   background, so the dataset can still be held when the next call arrives.
   Fits every observation: invisible to every userspace tool, worse the larger
   the snapshot being freed, intermittent, and it is the only heavyweight ZFS
   operation the tool aims at the target dataset right before the failing call.
   **Not verified:** that an in-flight async destroy produces this specific
   EBUSY is where the code points, not something read in the ZFS source.
   The correlation to capture is `freeing` being non-zero on the pool at the
   moment of failure — now part of the diagnostics.
2. **Exit-code-aware partial-failure handling** — capture `zfs_autobackup`'s
   actual exit code (1–254 = "N datasets failed, everything else completed,
   the tool's own graceful degradation" vs. 255 = "top-level exception or
   `KeyboardInterrupt`, categorically worse") and treat them differently —
   e.g. let the rest of the scheduled subtasks (rollup, later replication
   scopes) still run on a 1–254 partial failure, reserving today's
   full-abort `Background_error` behavior for 255. Valuable on its own
   merits (losing an entire night's remaining pipeline over one transient,
   already-isolated dataset failure is expensive), but doesn't address the
   *cause* — only limits the blast radius.

### Diagnostics — built, then extended after they came up empty

Plan 01 items 12/13 added automatic `fuser -vm` + `smbstatus -L` capture on both
target and source whenever a mount/umount or `zfs_autobackup` failure occurs
(shared `l_Print_diagnostics()` in `lib/rep_filesystems.bash`). They fire
correctly on both sides, and plan 01 item 14's guaranteed-remount cleanup fires
with them. On the 2026-09-13 failure they produced nothing at all on the target
— which is itself the finding above: both tools are blind to a ZFS long hold.

`l_Print_diagnostics()` therefore now also captures, per dataset, on both hosts:
- **mount namespaces still holding the dataset**, counted per program name, and
  only while the host reports it unmounted (on the source, still mounted, every
  process would match, so it reports that instead of dumping hundreds of lines);
- **snapshots carrying a hold** (`userrefs > 0`);
- **running `zfs send`/`recv` processes**;
- **the pool's `freeing` value plus health and scan state** — the async-destroy
  backlog, which is the one remaining lead's only observable.

The next genuine failure should name its own cause rather than come back empty.

### Live experiment — SMB service stopped on the target (2026-09-13)

An observation the diagnostics did not surface, from the same day's runs:
`media-ds` failed while every `backup-*-ds` succeeded, and `media-ds` was the one
whose SMB share was **enabled** — the four `backup-*` shares were disabled at the
time.

The readings argue against a client being involved: `smbstatus -L`, `-p` and `-S`
on the target were all empty at the moment of failure. So if Samba is implicated
at all, it is the share definition or the service itself, not a connection.

**Change made:** every SMB share on `truenas-backup` enabled, and the **SMB
service itself stopped**. That is a cleaner cut than toggling shares one by one —
if the next failure still happens, Samba is out entirely, share definitions
included. If the failures stop, the mechanism is inside Samba and the
share-level question becomes worth asking properly.

Operationally free: `truenas-backup` only serves clients when the `data` alias is
repointed to it. ⚠️ Which also means a failover now needs the SMB service started
by hand — a stopped service will not announce itself, it will just look like the
share is gone.

Result pending: the next full sync, expected within weeks.

### Next steps, in priority order

1. **Let the extended diagnostics catch a real `backup-*-ds` failure** and check
   whether `freeing` is non-zero at that moment (lead 1). That is the common
   case, and the only lead left is unproven.
2. **If `freeing` correlates, confirm the mechanism in the ZFS source** before
   building anything — specifically which paths return EBUSY from
   `dmu_recv_begin_check()`. A fix would then be a bounded wait for `freeing` to
   drain before `zfs_autobackup` is invoked, which belongs in this wrapper.
3. **Exit-code-aware partial-failure handling** (design above) — lower priority
   since it limits impact rather than addressing the cause.

---

## 3. Intermittent Immich DB restore failure — DIAGNOSED + FIXED (2026-07-07)

**Real occurrence captured (2026-07-07 run, master→backup).** Main log:
`Wait_for_pg_ready` printed "Start complete", then the restore aborted. The
restore log (`sync_truenas_servers_DB_Restore.2026-07-07_16-57.log`) is 4 lines:

```
psql: error: connection to server at "localhost" (::1), port 5432 failed: Connection refused
	Is the server running on that host and accepting TCP/IP connections?
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
	Is the server running on that host and accepting TCP/IP connections?
```

### Root cause — the probe and the restore used different transports

The readiness wait and the restore checked **different channels**, so the wait
passed while the restore's channel was still down:

- `Wait_for_pg_ready` probed with a bare **`pg_isready`** → the **Unix socket**.
- The restore pipes into **`psql --host=localhost`** → **TCP** `127.0.0.1/::1:5432`.

`Restore_immich_DB` `rm -rf`s all of pgdata every run, so the freshly-started
pgvecto container always re-runs **initdb bootstrap**. The postgres image runs
that bootstrap on a temporary internal server with **`listen_addresses=''`**
(Unix socket only, TCP deliberately off), then stops it and starts the real
TCP-listening server. During bootstrap, `pg_isready` on the socket answers
"accepting connections" while TCP :5432 is still refused → `Wait_for_pg_ready`
returned early, `sleep 2` wasn't enough, and `psql --host=localhost` hit the
gap. **Intermittent = race:** usually bootstrap finishes within the probe +
`sleep 2`; occasionally it doesn't. This revises the earlier guess (stop/rm-rf
race, or `Control_app`): it *is* the start side, but not a missing wait — the
wait probed the wrong channel.

### Fix (applied, `lib/immich_db.bash`)

- `Wait_for_pg_ready` now probes **`pg_isready --host=localhost`** — the same TCP
  channel the restore uses. It stays "not ready" all through initdb bootstrap and
  only flips once the real TCP server is up, closing the gap by construction.
- Removed the now-meaningless `sleep 2` (it was papering over the wrong probe).
- Stop/start **ORDER** unchanged (README global guardrail #1) — only the probe
  channel changed; no reorder.

### Operational note for the failed run

The 2026-07-07 restore aborted via `Background_error` *before* the final
`Control_app` stop/start, leaving **immich-backup with a freshly-initialised
empty pgdata and its writer containers stopped**. Master's data is intact, so
re-running the immich app_replication (backup → rsync → restore) recovers it.

### Still open (not this bug, but same class — deferred)

`Backup_immich_DB` starts pgvecto then runs `pg_dumpall` with **no**
`Wait_for_pg_ready`. Lower risk: backup does **not** `rm -rf`/re-initdb (existing
cluster, no bootstrap phase) and `pg_dumpall` defaults to the **socket**, so
there's no transport mismatch — but `docker running` still ≠ pg ready. Worth
adding a socket-side wait defensively; not implicated in this failure.
