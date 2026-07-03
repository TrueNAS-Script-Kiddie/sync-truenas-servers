# Plan 08 — Known operational issues (external causes, not code bugs)

Two recurring issues observed in real runs, both pre-existing (predate this
session's changes) and both rooted **outside** this script — in Immich's
Postgres instance and in ZFS/dataset contention, respectively. Documented
here so they aren't mistaken for regressions the next time they show up, and
so the one genuinely actionable piece (this script's all-or-nothing failure
behavior) has a place to live.

---

## 1. Immich Postgres collation-version-mismatch warning during DB dump

**Symptom** — every `Backup_immich_DB` run prints, from `pg_dumpall` itself:

```
WARNING:  database "postgres" has a collation version mismatch
DETAIL:  The database was created using collation version 2.41, but the operating system provides version 2.36.
HINT:  Rebuild all objects in this database that use the default collation and run ALTER DATABASE postgres REFRESH COLLATION VERSION, or build PostgreSQL with the right library version.
```

(repeats for `template1` too, and any other DB using the default collation.)

**Cause** — the OS-provided collation library (glibc/ICU inside the
`ix-immich-*-pgvecto` container) has drifted version since the databases
were created — typically after a base-image/package update changed the
library's collation version without the databases being told to accept it.
Not something this script does or can control — it's a property of the
Postgres instance Immich manages inside its own container.

**Impact — none on this script.** It's a Postgres `WARNING`, not an error;
`pg_dumpall` still exits `0`, so `Backup_immich_DB` correctly proceeds (this
was directly confirmed by the 2026-07-03 real run: the warnings printed, and
`### Making a backup of the Immich Postgres DB has completed successfully ###`
followed immediately after). Item 2d's `|| Background_error` conversion does
not and should not trigger on this.

**If you want to actually clear it** (out of scope for this project — this
is Immich/Postgres administration, not something `sync_truenas_servers`
should automate): Postgres's own hint is `ALTER DATABASE ... REFRESH
COLLATION VERSION`, but **don't run that blindly** — per Postgres's own
documentation, a collation version mismatch can mean any indexes built on
collation-sensitive columns are silently using a different sort order than
when they were built, which can affect correctness (e.g. subtly wrong sort
order or missed rows on collation-dependent range queries) until those
objects are `REINDEX`ed. The safe order is: identify affected
indexes/objects first, `REINDEX` them, *then* run `REFRESH COLLATION
VERSION` to acknowledge the new baseline — not the other way round. Whether
this is worth doing for Immich's specific schema (would need to check which
columns/indexes actually rely on default collation) is a decision for you,
separately from this codebase.

**Action for this plan: none.** Documented so a future `pg_dumpall` run
showing these lines isn't mistaken for something today's fixes broke.

---

## 2. Intermittent "dataset is busy" ZFS receive failures on large-file backup datasets

**Symptom** — `zfs_autobackup` occasionally fails partway through the
`latest_snapshot_only` filesystem-replication scope with:

```
! [Target] STDERR > cannot receive incremental stream: dataset is busy
! [Source] Command "ssh ... zfs send -L -e -c -i @<snap> master-pool/encrypted-ds/backup-htpc-ds@<snap>" returned exit code 255 (valid codes: [0])
! [Target] Command "zfs recv -u -x refreservation -v -s backup-pool/encrypted-ds/backup-htpc-ds" returned exit code 1 (valid codes: [0])
! [Source] master-pool/encrypted-ds/backup-htpc-ds: FAILED: Last command returned error
! 1 dataset(s) failed!
ERROR: ZFS Replication failed
```

A related symptom seen the same day: a subsequent run reported `cannot
resume send: '...' used in the initial send no longer exists` /
`Aborting resume, we dont want that snapshot anymore` — `zfs_autobackup`
detecting a stale resume token left over from the earlier failed attempt and
self-healing by falling back to a fresh full send. That part resolved
itself automatically and is not a concern on its own.

**Important clarification (from you, 2026-07-03): this is a mid-transfer
failure, not a quick pre-flight check.** For these 100GB–3TB-file datasets, a
send/receive can already be hours into moving data by the time this hits —
this isn't "fails fast, retry costs nothing," it's "potentially hours of
work already spent before the failure surfaces." That materially changes
the cost/benefit of every mitigation option below, especially retry logic
(option 2) — see the revised note there. One thing already working in your
favor, confirmed directly from your own failure log: `zfs_autobackup`
does **not** destroy the failed dataset's old snapshot (no `Destroying` line
for `backup-htpc-ds`, unlike its siblings that succeeded) — it deliberately
preserves resume capability for that specific dataset. And it doesn't stop
at the first failure either: `dl-ds` and `shared-ds`, later in the same
batch, completed and had their old snapshots cleaned up *after*
`backup-htpc-ds` failed. So the underlying tool already does the right
thing per-dataset — the all-or-nothing behavior described below is entirely
this wrapper's own choice, not something `zfs_autobackup` forces on it.

**Known pattern (per your own observation), not yet root-caused:**
- Only seen on the large-file "backup-\*-ds" style datasets (100GB–3TB
  files) — never on VM zvols or `media-ds`.
- Disabling the SMB share on the **source** (master) reduces frequency but
  doesn't eliminate it — meaning the busy-ness likely isn't (only) the
  share, and something else still holds the dataset (the error is a
  **receive**-side failure, i.e. on the **target**/backup-pool side — worth
  specifically checking what's touching `backup-pool/encrypted-ds/<dataset>`
  on `truenas-backup` at the time of failure: lingering SMB client sessions
  from *that* side, an antivirus/indexer scan, a stale `zfs recv -s` resume
  in progress, or a snapshot hold from a previous interrupted run).
- Your hypothesis (huge files → longer send/receive window → larger chance
  of something touching the dataset mid-transfer) is plausible and matches
  the "only on the huge-file datasets" pattern — larger files mean a longer
  window during which the target dataset is mounted-and-receiving, giving
  more opportunity for something else to open a handle on it.

**Confirmed by reading the actual `zfs_autobackup` source (not inferred from
logs) — `C:\Apps\zfs_autobackup\zfs_autobackup\ZfsAutobackup.py`, per this
project's own convention of reading the local third-party clone directly
rather than guessing from usage:**

- `sync_datasets()` (~line 359) loops over every source dataset in a plain
  `for` loop with a `try/except Exception as e:` **around each individual
  dataset**. On failure it does `fail_count += 1`, logs
  `"FAILED: " + str(e)`, and — unless `--debug` is passed — **just continues
  to the next dataset**. This is exactly what the log showed (`dl-ds`/
  `shared-ds` completing after `backup-htpc-ds` failed): it's the tool's
  designed behavior, not a happy accident, and there is **no built-in retry
  anywhere** — not in this loop, not in the lower `ExecuteNode.py` command
  layer (checked directly; no retry/attempt/backoff logic exists at all).
  Any retry would have to be built into *this* wrapper, re-invoking the
  whole `zfs_autobackup` command — there's no CLI flag for it.
- **The exit code *is* the fail count** (`sys.exit(min(failed_datasets, 255))`,
  bottom of the file) — 1 dataset failed → exit 1, 2 failed → exit 2, etc.,
  **capped at 255**. Crucially, **255 is also separately used** for two
  completely different failure classes: an uncaught top-level `Exception`
  and `KeyboardInterrupt` (both hit a `except ...: return 255` a few lines
  up). So exit code 255 conflates "everything possible failed" with "the
  whole command crashed/was interrupted," while 1–254 unambiguously means
  "N individual datasets failed, everything else that could complete, did."
  Our wrapper currently can't tell these apart — `if
  ${ZFS_AUTOBACKUP_COMMAND}...; then success; else Background_error; fi`
  treats every nonzero exit identically, aborting the **entire remaining
  sync_truenas_servers run** (not just that subtask) on top of a signal
  that the tool itself designed to be much more nuanced than pass/fail.
  Depending on where in the subtask order (`app_replication` →
  `vm_replication` → `zfs_replication_all` → `rollup` →
  `zfs_replication_latest`) this lands, everything scheduled *after* it
  silently never runs. Combined with plan 02's still-open gap (no failure
  email), a transient one-dataset hiccup can currently mean the whole
  night's sync quietly stops partway with nobody notified.
- Confirmed too: the stale-resume "Aborting resume, we dont want that
  snapshot anymore" message (`ZfsAutobackup.py` ~line 1217, inside
  `_plan_sync`) fires when the sync planner walks forward through candidate
  snapshots and decides the current source snapshot "isn't needed" by the
  target *and* a resume token was pointing at it — i.e. the tool
  **recomputes what needs syncing fresh every run** and discards a resume
  token that no longer matches that plan. Genuinely self-healing, not a
  bug, exactly as suspected — now confirmed from the source rather than
  inferred from one log line.

**Options for a future pass (not applied now — needs a decision, not just a
fix), re-prioritized given the mid-transfer clarification above:**

1. **Do plan 02 first regardless** (failure email) — the highest-leverage,
   lowest-risk fix, and now more clearly urgent: this failure can happen
   hours into an overnight run, entirely unattended. Right now that means
   silently finding out days later that a backup window was missed.
2. **Reconsider whether one dataset's failure should abort the *rest of the
   sync_truenas_servers pipeline*** — and this now has a concrete, surgical
   hook to hang off, thanks to reading the source: capture
   `zfs_autobackup`'s actual **exit code**, not just success/failure.
   `1`–`254` means "N individual datasets failed, everything else the tool
   *could* finish, it did" (its own designed, graceful degradation); `255`
   means something categorically worse (a top-level exception or
   `KeyboardInterrupt` — the tool didn't even get to run its normal
   per-dataset loop to completion). Those two cases arguably deserve
   different treatment from this wrapper — e.g. treat 1–254 as "completed
   with N failures, log/report it clearly, but let the rest of the
   scheduled subtasks (rollup, later replication scopes) still run," and
   reserve the current full-abort `Background_error` behavior for 255 or
   for a genuinely fatal signal. Given how expensive it looks to lose an
   entire night's remaining pipeline over one transient contention window
   that the tool itself already isolated to one dataset, this is probably
   **more impactful than retry logic** (item 3) and worth deciding on
   first. Still a real behavior change (needs a decision on what "partial
   success" means for the summary/email, and plan 02's email would need to
   distinguish this case too), not a quick fix — but no longer a vague
   "reconsider," now a specific `$?` check to add.
3. **Retry-with-backoff for this failure signature** (`cannot receive
   incremental stream: dataset is busy`) — worth a precise caveat given
   the mid-transfer clarification: the log shows `zfs recv` **rejecting
   the attempt outright** (`dataset is busy`), not an interrupted transfer
   partway through — so there's no partial progress on *that specific
   attempt* to lose or resume from; zfs's own resumable-receive (`-s`) only
   helps if a transfer that had already started gets interrupted mid-stream,
   which isn't quite what this log shows. So a short retry (e.g. 2–3
   attempts, 10–30s backoff) mainly buys "ride out a brief lock/contention
   window" rather than "resume a half-finished multi-hundred-GB transfer."
   Still worthwhile — cheap, bounded (gives up and falls through to today's
   behavior if the condition doesn't clear quickly) — but item 2 above
   addresses the bigger cost (losing the rest of the night's pipeline)
   more directly than this does.
4. **Investigate the actual holder** directly on `truenas-backup` next time
   it happens (`fuser`/`lsof` on the mountpoint, `zfs holds`, check for an
   in-progress SMB session or scrub) — this is diagnosis, not a code change,
   but would turn "known pattern" into "known cause," and is the only option
   here that could eliminate the problem rather than just soften its impact.

**Action for this plan: none applied yet — this is a documented, watched
issue.** Recommend tackling item 1 (plan 02) soon regardless of this
specific problem; item 2 (don't let one dataset's failure cancel the rest of
the pipeline) looks like the next-highest-value follow-up specifically
because of the "hours of work already done, more work still scheduled"
shape of this failure.
