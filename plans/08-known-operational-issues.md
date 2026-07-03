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

---

### Resolved during discussion (2026-07-03) — skipping live diagnosis, trying two direct fixes instead

**5. `zfs umount`'s exit status was never checked — ✅ FIXED, see
[plan 01 item 12](01-bug-fixes.md).** This was found by reasoning through the
mechanism, not by re-diagnosing live: standard Unix `umount` fails with
`EBUSY` whenever something still has an open file handle on the filesystem —
and `l_Toggle_mounts` runs a **plain** `zfs umount` (no `-f`) whose result was
discarded. So the exact "dataset is busy" condition this whole issue is about
was almost certainly **already causing the umount itself to silently fail**,
well before `zfs_autobackup` ever got to attempt the receive — the script just
had no way of knowing, and the real failure surfaced much later and far less
usefully, deep inside the receive step. Checking the exit code (now done)
turns this into an immediate, correctly-attributed failure at the umount
step itself, before any of that dataset's potentially-hours-long transfer
even starts.

**Deliberately not force-unmounting (`-f`).** Force would push the unmount
through even with an open handle, but (a) it's not confirmed that this
actually clears whatever ZFS-internal state blocks a subsequent `zfs recv`
(it might just detach the mountpoint while the client's file descriptor
keeps referencing the old instance in limbo), and (b) more importantly, it
risks corrupting or truncating whatever a client is actively writing at that
exact moment — a materially worse outcome than the sync simply failing and
retrying next cycle. Rejected on safety grounds, not just uncertainty.

**6. Target-side share disable/re-enable — included per your request.
Initial assessment below was later superseded by decisive new evidence; see
the section after this one before drawing conclusions from this part.**
Talked through the mechanism in detail:
- Unmounting a dataset (which the script already attempts, and now correctly
  detects if it fails) already means nothing is being served at that path —
  a share whose `enabled` flag is still `true` in TrueNAS's config can't hand
  out a file that isn't there. So disabling the share doesn't add protection
  against **new** connections beyond what a *successfully-checked* unmount
  already provides, **at the moment the unmount happens**.
- Disabling the share also does **not** retroactively close a connection a
  client already had open before the sync started — TrueNAS's `enabled` flag
  most likely only stops new connections, not existing ones.
- *(Initial, since-revised take:* for the specific "dataset is busy" problem,
  share-disabling doesn't clearly close a gap that a properly-checked
  unmount leaves open. *This assumed the busy condition originates before or
  at the start of the sync — which turned out to be wrong; see below.)*

**Confirmed schema** (from `midclt call sharing.smb.query` on `truenas-backup`,
2026-07-03): each share has `id` (int), `path` (exact match to the dataset
mountpoint, e.g. `/mnt/backup-pool/encrypted-ds/backup-htpc-ds` — no
nested-subfolder complexity, simplifying path matching), and `enabled`
(bool). All current shares on `truenas-backup` — `media`, `dl`,
`backup-desktop`, `backup-elke-hp`, `backup-elke-sony`, `backup-htpc`,
`shared` — are `enabled: true` today, confirming both servers currently
share all of these datasets simultaneously.

**If built** (scope: all shares, target side only, matched against that
run's `IMPACTED_DATASETS` since one `zfs_autobackup` invocation covers
multiple datasets at once, **kept disabled for the full duration of the
sync, not just checked once at the start**): the design must guarantee
re-enabling even on failure — a `trap ... EXIT` registered before disabling
(not inside a `$(...)` subshell, so it isn't subject to the same
subshell-scoping problem found elsewhere in this codebase) is the correct
mechanism, since `Background_error` can fire from deep inside the sync and
must not leave a share silently disabled forever.

**Action for this plan:** #5 is done (plan 01 item 12). #6's priority is
revised upward below, based on evidence gathered after this section was
first written — read on.

---

### Your reservation about fix #5 — confirmed correct, my theory falsified (2026-07-03)

I'd proposed a falsifiable test (see git history of this file / the
conversation this plan is drawn from) for whether fix #5 (the umount
exit-code check) actually addresses this issue: a "stale, pre-existing
handle" theory would predict the *next* failure should surface immediately
at the start of a run. **You supplied the decisive counter-evidence
directly: this failure occurs *most often* on `backup-desktop-ds` — the
largest dataset, and the *first* one in the processing order — and
specifically **after it has already been transferring successfully for
hours**, not at the start.** That kills the stale-handle theory outright, on
two independent counts:

1. **No "waiting its turn" explanation is available for the first-processed
   dataset.** My theory's entire explanation for "why does it fail later,
   not immediately" depended on the failing dataset coming later in
   `zfs_autobackup`'s processing order (so other, unrelated datasets get
   attempted — and succeed — first). That doesn't apply when the failure
   happens on the *first* dataset attempted.
2. **A precondition-style "busy" rejection would fire before any data
   moves, not hours into an already-progressing transfer.** Data flowing
   for hours means `zfs recv` genuinely started and was working correctly
   for a long time. Something changed **during** that window — this is not
   consistent with a handle that predates the run.

**Conclusion: fix #5 does not address this, the dominant failure mode.** It
remains a correct, worthwhile fix in general (checking an ignored exit code
is just correct practice, and it might still catch a genuinely different,
rarer scenario — a handle that really was stale before a run started), but
it should not be expected to resolve the failures you actually see most.

**This reopens something I'd wrongly downgraded: target-side share-disable
for the *entire duration* of the sync (not just checked once at the start)
is back on the table as a real candidate**, precisely because the failure
now looks like "something grabs the dataset at some point *during* a
multi-hour window" rather than "something was already holding it before the
run began." If the something is a live SMB client connecting to the
target's active share at a random point mid-transfer, keeping that share
off for the whole duration (design already specified above: match against
that run's `IMPACTED_DATASETS`, trap-guaranteed re-enable) would directly
close that window. I was wrong to call this "likely redundant" — that
reasoning was built on the now-falsified stale-handle framing.

**A second concrete candidate, given "something new, mid-transfer, on the
target host":** a **scheduled TrueNAS-internal task** — a periodic snapshot
task, a scrub, a SMART test, or anything else with its own schedule —
touching `backup-pool` (or specifically `backup-pool/encrypted-ds/backup-desktop-ds`)
on `truenas-backup` sometime during those multi-hour windows. Worth checking
TrueNAS's own Data Protection scheduling for anything that could run during
the hours these large transfers are typically active. This is genuinely
checkable (unlike guessing at share timing) and matches your original
instinct from earlier in this conversation almost exactly ("some TrueNAS
internal service or something").

**Revised recommendation:** given the failure is now understood to be
"something touches the target dataset at an unpredictable point during a
multi-hour transfer," the two most promising next steps are (a) building the
target-share-disable-for-the-whole-duration feature (already fully specified
above) and (b) auditing `truenas-backup`'s scheduled tasks for anything that
could collide with `backup-pool` during a long transfer. Both are concrete
and actionable, unlike the umount-timing theory this section started with.

**Action for this plan:** fix #5 (plan 01 item 12) stays applied — it's
correct on its own merits, just not the fix for this specific problem.
Recommend, in priority order: (1) plan 02's failure email regardless, (2) the
target-share-disable feature (re-elevated, design already specified above),
(3) auditing `truenas-backup`'s scheduled tasks for anything that could
collide with `backup-pool` mid-transfer, (4) the exit-code-aware
partial-failure handling (still valuable, but no longer believed to be
addressing *this* issue directly — it only limits the blast radius of a
failure, not its cause).

---

### Further update (2026-07-03): no known client ever points at `backup` — reprioritizes diagnosis over the share theory

**New fact: your backup clients have always pointed at `master` (or the
`data` alias), never at `backup` — the one exception being a single manual
test against these shares from your Windows desktop, years ago, during
initial setup.** This matters:

- It makes "a live client happens to connect mid-transfer" a *weaker*
  default explanation than I'd been treating it as — there's no known,
  intentional traffic that should ever reach these shares at all.
- It correspondingly *strengthens* the scheduled-TrueNAS-task theory: a task
  running entirely on `truenas-backup` itself doesn't need any client to be
  misconfigured or connecting — it would just be the backup server acting on
  its own schedule, independent of anything external. That fits "something
  unknown to me" much better than a client-side explanation does.
- It doesn't fully rule out a client, though: a years-old Windows mapped
  drive (especially one set to reconnect at sign-in, or still sitting in File
  Explorer's quick-access) can silently keep trying to reconnect indefinitely,
  with zero ongoing awareness required from you. Low-probability, but real.
- **It also exposes a real limitation in the share-disable idea**: disabling
  the Samba share only helps if the culprit is SMB-related. If it's actually
  a scheduled ZFS/TrueNAS-level task (snapshot, scrub, SMART test), disabling
  the share does **nothing** — those operate below the Samba layer entirely.
  Building share-disable before knowing which theory is right risks solving
  a problem that isn't the one you have.

**Revised priority: get direct evidence before building either mitigation.**
The cheapest, most decisive next step is `smbstatus` on `truenas-backup`
during/immediately after the next failure — zero active sessions strongly
suggests the scheduled-task theory; any active session directly confirms the
client theory. This should come before committing to building share-disable.

**Update (2026-07-03, later same day): the second, more relevant diagnostic
capture is now built too — [plan 01 item 13](01-bug-fixes.md).** The
pre-flight umount branch (item 12) only ever covers the rarer failure mode.
The actual dominant-failure capture point — `l_Execute_replication_and_remount`'s
`if ${ZFS_AUTOBACKUP_COMMAND}...; then ... else Background_error; fi` — now
runs `fuser -vm` (per impacted dataset) and `smbstatus -L` on **both target
and source** before aborting, specifically because `zfs_autobackup`'s own
`[Source]`/`[Target]`-tagged failure lines don't reliably tell you which side
actually caused the problem without fragile text parsing (the `zfs send` side
often shows a downstream, consequential failure when the real cause is on
`recv`/target). This is the diagnostic that should actually produce evidence
next time the dominant, mid-transfer failure occurs — not yet observed since
landing, so still needs a real occurrence to confirm it captures something
useful. Still doesn't replace the `smbstatus`-during-a-live-failure check
recommended above as the most decisive single piece of evidence — but now
that capture happens automatically, in the log, without needing to be
watching live.

---

### A second, more serious consequence of this issue was found the same day: a failed run leaves target datasets unmounted forever

While testing the above, a real `zfs list -o name,mounted -r backup-pool/encrypted-ds`
on `truenas-backup` showed `backup-desktop-ds`, `backup-elke-hp-ds`,
`backup-elke-sony-ds`, `backup-htpc-ds`, `dl-ds`, and `shared-ds` **all still
unmounted**, hours after the "dataset is busy" failure earlier that day —
`media-ds` (uninvolved in that failure) was correctly mounted. This means
**a failed transfer doesn't just abort the sync — it can leave a share
serving an empty directory to clients indefinitely**, until either someone
notices and manually remounts, or the code is fixed. See
[plan 01 item 14](01-bug-fixes.md) for the root cause (`Background_error`'s
`exit` skips the remount step entirely) and the fix (a `trap ... EXIT`
scoped precisely to what each run itself unmounted — deliberately **not** a
blind "remount everything," which would wrongly override a dataset
unmounted on purpose for unrelated reasons).

This is arguably the most operationally significant thing found in this
whole investigation: it means every prior occurrence of the "dataset is
busy" failure has likely left affected datasets — and their client-facing
shares — silently broken until someone happened to check. Worth a one-time
audit of `truenas-backup` (and `truenas-master`, for `backup_to_master`
runs) for any other datasets currently sitting unmounted from a past,
unnoticed failure, independent of the specific six found this time.

---

### Deliberate kill-test (2026-07-03): diagnostics confirmed working, but the Veeam lead is retracted

To exercise items 13/14 without waiting for a real occurrence, you
deliberately killed the `zfs_autobackup` process mid-run to force a genuine
failure. Results:

- **Item 13's diagnostics fired correctly** — `fuser -vm`/`smbstatus -L`
  output appeared for both target and source, cleanly formatted (see the
  formatting note below).
- **Item 14's trap correctly remounted** every dataset this run itself had
  unmounted — confirmed via `zfs list -o name,mounted` immediately after.
- The `smbstatus -L` output during this test showed an **active Veeam
  backup SMB lease** on `backup-desktop-ds` at the time of the (artificial)
  failure — initially a promising lead, since Veeam running against the
  same dataset mid-transfer would fit the "something touches the target
  mid-window" pattern this whole plan has been chasing.

**Retracted — you supplied decisive counter-evidence:** *"Veeam backup is
indeed currently running. But I'm also 100% sure that the dataset busy
occurs also when Veeam isn't running (I know when Veeam runs and I try to
run the sync when it doesn't)."* Since you can independently confirm the
real failure happens with Veeam **confirmed not running**, Veeam cannot be
the (sole) cause. The lease seen in this test is retracted as a root-cause
theory — most likely just an artifact of testing during Veeam's own
schedule window, correctly captured by the diagnostic, but not causal.
**The two live leads from the previous section stand unchanged:**
scheduled TrueNAS-internal task (favored by elimination — see two sections
up) and, more weakly, a stray client reconnect. The diagnostic tooling
itself is proven to work; what's still needed is for it to catch the
*real* failure, not an artificial one, so whatever it reports next time is
worth trusting.

**Source-side SMB activity is a much weaker lead than target-side, but not
ruled out entirely.** The same kill-test's `smbstatus -L` also showed SMB
usage on the **source** (`master`) side. You supplied a concrete data
point against it mattering: `dl-ds` is heavily used via SMB on the source
**constantly** — including right now — and has **never** produced a
"dataset is busy" failure. That lines up with the already-established
mechanics (see the send/recv discussion earlier in this conversation):
`zfs send` only reads from an immutable **snapshot**, never the live
mounted filesystem, so ordinary SMB reads/writes against the source
shouldn't be able to contend with it. **But you're explicitly not calling
this 100% harmless** — just clearly less implicated than the target side.
That's exactly why both sides stayed in the diagnostic capture (item 13)
rather than only checking the target: it costs nothing to keep watching
the source too, and the next *real* (non-artificial) failure will settle
it — if source-side SMB activity correlates with real failures the way
target-side does, that reopens the question; if it's absent or unrelated
every time, that confirms it's noise. Until then, treat the target side as
the primary place to look, and the source side as still-open, secondary
data to collect.

**Visibility quirk found during this same test — since FIXED (2026-07-03,
later same day):** the item 14 remount didn't appear on the live terminal,
even though it demonstrably happened (confirmed via `zfs list` right
after). Cause: `Background_error` (`lib/common.bash`) kills the `tail -f`
that streams the log to your terminal **before** calling `exit` — and it's
`exit` that fires the `EXIT` trap doing the remount. So the trap's `echo`
output reached the log **file** (a separate, still-open `>>` redirect) but
never the terminal. **Fix:** the local trap was generalized into a shared
**LIFO cleanup stack** (`Register_cleanup`/`Unregister_cleanup`/`Run_cleanup`
in `lib/common.bash`, upgraded from a single slot to a stack on 2026-07-04
so nested restore scenarios — plan 09's VM restart around the filesystem
remounts — compose correctly). `Background_error` now runs the registered
cleanups itself, *before* killing the tail (restore output visible live),
and a single global `trap 'Run_cleanup; Kill_tail' EXIT` in
`bin/sync_truenas_servers` remains as the safety net for any exit that
bypasses `Background_error` — which also fixed a second latent bug: a bare
`exit` used to leave the foreground `tail -f` hanging forever. Entries are
popped before eval, so whichever path fires first wins, double-runs are
impossible, and a failing cleanup can't block the remaining ones. See plan
01 item 14's follow-up note for the full mechanism.

**Formatting improvement (2026-07-03):** the original diagnostic dump (item
12/13) was flat, unindented `echo`/command output with no visual separation
from the rest of the log — hard to read, especially with multiple datasets
per invocation. Replaced with a shared `l_Print_diagnostics()` helper in
`lib/rep_filesystems.bash` that prints a clear header per server
(`--- Diagnostics: truenas-<id> ---`), blank-line separation, and
consistent indentation for each dataset's `fuser` output and the trailing
`smbstatus -L` block. Both call sites (item 12's umount/mount failure
branch and item 13's `zfs_autobackup` failure branch) now call this one
helper instead of duplicating the echo/`Execute_command` sequence.
