# Plan 08 — Known operational issues (external causes, not code bugs)

Three recurring issues observed in real runs. #1 and #2 are rooted **outside**
this script's own logic; #3 was a **timing bug in the script's own wait logic**
— diagnosed from a real 2026-07-07 failure and **fixed**. Documented so they
aren't mistaken for regressions.

---

## 1. Immich Postgres collation-version-mismatch warning

**Status:** clean on both hosts (2026-10-22). Recurs after every Postgres
*major* upgrade of the Immich app — follow the procedure below.

**Symptom:** `Backup_immich_DB` prints, once per database it connects to:

```
WARNING:  database "postgres" has a collation version mismatch
DETAIL:  The database was created using collation version 2.41, but the operating system provides version 2.36.
```

It's a `WARNING`: `pg_dumpall` still exits `0` and the backup completes.
Plan 01 item 2d's `|| Background_error` does not and should not trigger on it.

### Cause

Postgres records, per database, the glibc version it was created under
(`pg_database.datcollversion`), because glibc defines text sort order. When
the running glibc differs, it warns: indexes on text columns built under the
old sort order may now be silently wrong.

The Immich app contains two containers that run the **same** data files
(`immich-pgdata-ds`) on **different** Debian releases:

| Container | Image | Debian / glibc | Runs |
|---|---|---|---|
| `ix-immich-<server>-pgvecto-1` | `ghcr.io/immich-app/postgres:18-vectorchord1.1.1` (Immich) | 12 / **2.36** (`PG_VERSION=…pgdg12…`) | all the time — the Postgres server |
| `ix-immich-<server>-pgvecto_upgrade-1` | `ghcr.io/truenas/containers/postgres-upgrade:1.2.24` (TrueNAS) | 13 / **2.41** (`PG_VERSION=…pgdg13…`) | at each app start; only acts on a Postgres major upgrade |

On a major upgrade the TrueNAS helper `initdb`s the new cluster and
`pg_upgrade`s into it under 2.41, then `REFRESH`es every database to 2.41.
From then on `pgvecto` runs everything under 2.36 → mismatch. TrueNAS treats
this as expected ([truenas/apps#3882](https://github.com/truenas/apps/issues/3882):
the helper is shared by all apps, most of which run on trixie).

**Which host:** only the warning on the sync's *source* is ever seen — only
`pg_dumpall` connects there. A *target* never keeps it: `Restore_immich_DB`
wipes pgdata, `pgvecto` re-`initdb`s under its own glibc, and the dump's
`CREATE DATABASE immich` runs under that same glibc. So the host that is
normally the source (master) keeps the helper's 2.41 labels; the other host
is clean after its next restore.

`postgres` and `template1` are Postgres's built-in, normally empty databases
(`template0` refuses connections and carries no version). `immich` holds the
data; it is usually unaffected because each restore recreates it.

### Procedure — when the warning appears

Root shell on the host that prints it (and, to be sure, on the other host).
All three steps read only; step 3 needs the app running.

1. Show recorded vs. running version per database:

```bash
h=$(hostname -s | sed 's/^truenas-//')
c=$(docker ps --format '{{.Names}}' | grep -E "immich-$h-pgvecto-[0-9]+$"); echo "pgvecto container: $c"
docker exec -i "$c" psql -U immich -d immich -c "SELECT datname, datcollversion AS created_with, pg_database_collation_actual_version(oid) AS running_on FROM pg_database;"
```

2. Every row where `created_with` ≠ `running_on` is affected. For each
   affected database other than `immich`, confirm it holds no user objects
   (expected `0`; replace `postgres template1` with the affected names):

```bash
for db in postgres template1; do echo -n "$db: "; docker exec -i "$c" psql -U immich -d "$db" -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%';"; done
```

3. Fix, per affected database:

   - **Empty database (`0` in step 2)** — only the label is wrong: system
     catalogs don't sort via glibc, so there is nothing to rebuild.
     Refreshing the label *is* the complete fix, not a suppression:

```bash
docker exec -i "$c" psql -U immich -d postgres -c "ALTER DATABASE postgres REFRESH COLLATION VERSION;"
docker exec -i "$c" psql -U immich -d template1 -c "ALTER DATABASE template1 REFRESH COLLATION VERSION;"
```

   - **`immich`, or any database with a non-zero count** — refreshing alone
     *would* only hide it: indexes built under the other sort order stay
     wrong. Rebuild first, then refresh. Stop Immich's writers so nothing
     writes during the `REINDEX` (minutes on a large library):

```bash
docker stop "ix-immich-$h-server-1" "ix-immich-$h-machine-learning-1"
docker exec -i "$c" psql -U immich -d immich -c "REINDEX DATABASE immich;"
docker exec -i "$c" psql -U immich -d immich -c "ALTER DATABASE immich REFRESH COLLATION VERSION;"
docker start "ix-immich-$h-machine-learning-1" "ix-immich-$h-server-1"
```

   Verify by re-running step 1: every row except `template0` must show equal
   versions. Each `ALTER` prints the warning once more as it connects, then
   `NOTICE: changing version from X to Y` — that's expected.

Alternative for a host that is a sync *target*: the next `app_replication`
run onto it wipes and re-creates its cluster under its own glibc, clearing
the mismatch without any manual step.

### When it becomes risky

When Immich moves its Postgres image to Debian 13 (glibc 2.41), the mismatch
flips direction and hits **every** database on both hosts — including
`immich`, which then needs the `REINDEX` path above. Check step 1 after any
Immich app update that changes the `pgvecto` image's `PG_VERSION` suffix
(`pgdg12` → `pgdg13`).

Not automated in this project on purpose: `REINDEX` of a live Immich DB is a
judgement call, and a silent auto-refresh is exactly the "hide it" outcome
above.

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
