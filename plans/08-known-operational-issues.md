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

**Symptom:** `zfs_autobackup` occasionally fails partway through the
`latest_snapshot_only` filesystem-replication scope:

```
! [Target] STDERR > cannot receive incremental stream: dataset is busy
! [Source] Command "ssh ... zfs send ..." returned exit code 255 (valid codes: [0])
! [Target] Command "zfs recv -u -x refreservation -v -s backup-pool/encrypted-ds/backup-htpc-ds" returned exit code 1 (valid codes: [0])
! [Source] master-pool/encrypted-ds/backup-htpc-ds: FAILED: Last command returned error
! 1 dataset(s) failed!
ERROR: ZFS Replication failed
```

A related, separate symptom seen the same day: a subsequent run reported a
stale resume token (`cannot resume send: ... no longer exists` / `Aborting
resume, we dont want that snapshot anymore`). Confirmed by reading
`ZfsAutobackup.py` directly (`_plan_sync`, ~line 1217): the tool recomputes
what needs syncing fresh every run and discards a resume token that no
longer matches that plan. **This is self-healing, not a bug** — resolves
itself automatically, not a concern on its own.

### Established facts

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
- **Only seen on large-file "backup-\*-ds" style datasets** (100GB–3TB
  files) — never on VM zvols or `media-ds`. The working hypothesis (huge
  files → longer send/receive window → more opportunity for something else
  to touch the target dataset mid-transfer) is plausible and matches this
  pattern, but is not proof of cause.
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
  during initial setup. This weakens "a live SMB client happens to connect
  to the target mid-transfer" as the default explanation, and correspondingly
  strengthens a scheduled-task theory: a task running entirely on
  `truenas-backup` itself doesn't need any client to be involved at all. It
  doesn't fully rule out a client — a years-old Windows mapped drive set to
  auto-reconnect could silently keep trying with zero ongoing awareness —
  but it's a materially weaker candidate now.
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

1. **A scheduled TrueNAS-internal task** on `truenas-backup` — a periodic
   snapshot task, a scrub, a SMART test, or anything else with its own
   schedule — touching `backup-pool` (or specifically
   `backup-pool/encrypted-ds/backup-desktop-ds`) sometime during the
   multi-hour transfer window. Favored by elimination (see "no known client"
   above) but not yet directly observed. Genuinely checkable: audit
   `truenas-backup`'s Data Protection scheduling for anything that could run
   during the hours these large transfers are typically active.
2. **Target-side SMB share disable for the full sync duration** (not just a
   one-time check at the start) — would close the window if the culprit
   turns out to be SMB-related after all. Mechanism already reasoned through
   in detail: unmounting the dataset (which the script already does)
   already stops a share from serving anything at that path, so disabling
   the share adds nothing beyond a *successfully-checked* unmount **at the
   moment the unmount happens** — but it also does **not** retroactively
   close a connection a client already had open before the sync started,
   which is the actual gap a full-duration disable would close, if the
   cause is SMB at all. Deliberately **not built yet**: it only helps if the
   culprit is SMB-related — if it's actually a scheduled ZFS/TrueNAS-level
   task, disabling a Samba share does nothing, since that operates below
   the Samba layer entirely. Building this before knowing which theory is
   right risks solving a problem that isn't the one you have. If built:
   scope to all shares matching that run's `IMPACTED_DATASETS`, target side
   only, guaranteed re-enable via the shared cleanup stack in
   `lib/common.bash` (`Register_cleanup`/`Run_cleanup` — the same mechanism
   plan 01 item 14 uses for remounting).
3. **Exit-code-aware partial-failure handling** — capture `zfs_autobackup`'s
   actual exit code (1–254 = "N datasets failed, everything else completed,
   the tool's own graceful degradation" vs. 255 = "top-level exception or
   `KeyboardInterrupt`, categorically worse") and treat them differently —
   e.g. let the rest of the scheduled subtasks (rollup, later replication
   scopes) still run on a 1–254 partial failure, reserving today's
   full-abort `Background_error` behavior for 255. Valuable on its own
   merits (losing an entire night's remaining pipeline over one transient,
   already-isolated dataset failure is expensive), but doesn't address the
   *cause* — only limits the blast radius.

### Diagnostics — built and confirmed working

Plan 01 items 12/13 added automatic `fuser -vm` + `smbstatus -L` capture on
both target and source whenever a mount/umount or `zfs_autobackup` failure
occurs (shared `l_Print_diagnostics()` helper, cleanly formatted). **Confirmed
working end-to-end** via a deliberate kill-test (2026-07-03): diagnostics
fired correctly on both sides (this is what surfaced and then let us retract
the Veeam lead above), and plan 01 item 14's guaranteed-remount cleanup fired
correctly too. What's still needed is for this to catch a **real**
occurrence rather than an artificial one — nothing observed so far changes
the leads above; the most decisive single piece of evidence would still be
`smbstatus` output captured at the moment of a genuine failure, which the
diagnostics now do automatically without needing anyone watching live.

### Next steps, in priority order

1. **Do plan 02 (failure email) regardless** — highest-leverage, lowest-risk,
   and more clearly urgent given this can fail hours into an unattended
   overnight run with nobody notified for days.
2. **Audit `truenas-backup`'s scheduled tasks** (Data Protection: snapshot
   tasks, scrub, SMART tests) for anything that could run during a large
   transfer's multi-hour window — the most promising lead by elimination.
3. **Wait for the diagnostics to catch a real occurrence** — check both
   `fuser` output (anything other than `root kernel mount`?) and whether
   `smbstatus` shows anything on the target at the time. This is the
   cheapest remaining way to convert "known pattern" into "known cause."
4. **Only after 2–3 point at SMB specifically:** build the
   target-share-disable feature (design above).
5. **Exit-code-aware partial-failure handling** (design above) — lower
   priority since it limits impact rather than addressing the cause.

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
