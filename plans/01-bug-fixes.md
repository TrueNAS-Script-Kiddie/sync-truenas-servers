# Plan 01 — Bug fixes

Surgical fixes only; no restructuring. Items are independent — implement and
verify **one at a time**. Read `plans/README.md` "Global guardrails" first.

After each item: `bash -n <changed file>`. When all done, run
`./bin/sync_truenas_servers --test --task=master_to_backup` on a TrueNAS host
and read the log end-to-end.

**Status (2026-07-03):** items 1, 3, 4, 6, 7, 9, 10, 11 — the low-risk,
easily-testable ones — are done and verified (`bash -n` + `shellcheck -x`
clean on every touched file, no new findings introduced). Items **2, 5, 8**
are deliberately still open: 2 (Immich DB restore guard) and 8 (VM
target-state check) touch the two most sensitive paths in the codebase per
the project's own guardrails; 5 changes an actual data-selection query
(which datasets get replicated), not just an additive safety check. All
three deserve dedicated attention and host-side verification rather than
being bundled with the routine fixes.

---

## 1. Bare `exit 1` inside the backgrounded run — [lib/rep_apps.bash:142](../lib/rep_apps.bash#L142) — ✅ DONE (2026-07-03)

When `--app=<name>` is not found in `apps.json`, `Perform_app_replication` does
`echo ... ; exit 1`. This runs in the background child, so the foreground
`tail -f` hangs forever and no failure is visible/emailed. This violates the
project's own error-handling rule.

**Fix:** replace the `echo` + `exit 1` pair with:

```bash
Background_error "ERROR: Requested app '${APP_NAME}' not found in config/apps.json"
```

(Plan 04 item 4 later moves this validation to CLI parse time; this fix is
still correct in the meantime.)

## 2. `Restore_immich_DB` wipes the DB before checking the target app is running — [lib/immich_db.bash:69-88](../lib/immich_db.bash#L69-L88) — ⏸ DEFERRED (touches the Immich DB restore path; do separately with host-side verification)

`midclt app.stop` **destroys** an app's containers. If the target Immich app
was STOPPED before the sync (it stays stopped — `Control_app_with_checks start`
only restarts apps *this run* stopped), then in `Restore_immich_DB`:
`CONTAINERS_TO_STOP`/`CONTAINERS_TO_START` grep nothing, the `rm -rf` of
`immich-pgdata-ds/*` **still executes**, and `Wait_for_pg_ready ""` times out
→ run aborts *after* deleting the target DB. (The rsync'd dump survives, so
it's recoverable, but only manually.)

**Fix:** at the top of `Restore_immich_DB` (right after the two `echo` lines,
before the container discovery), add the same guard `Backup_immich_DB` has at
[lib/immich_db.bash:14](../lib/immich_db.bash#L14), but for the target side:

```bash
    # The target app must be running: app.stop destroys containers, so a stopped
    # app has no pgvecto container to restore into. Abort BEFORE wiping pgdata.
    if [[ "$(Execute_command "${TARGET_LOCATION}" "midclt call app.query | jq -r '.[] | select(.name==\"immich-${TARGET_SERVER_ID}\") | .state'")" != "RUNNING" ]]; then
        Background_error "ERROR: To restore the Immich DB, immich-${TARGET_SERVER_ID} must be in a running state."
    else
        echo "Immich app is running on the target, proceeding..."
    fi
```

## 3. `Control_app` cannot see a FAILED job — [lib/rep_apps.bash:15-20](../lib/rep_apps.bash#L15-L20) — ✅ DONE (2026-07-03)

The poll loop only exits on `SUCCESS`. A job that lands in `FAILED`/`ABORTED`
spins for the full 60 s and then reports a misleading *timeout*.

**Fix:** capture the state in a variable each iteration and abort immediately
on a terminal failure state. Replace the `while` loop body with:

```bash
        local JOB_STATE
        while true; do
            JOB_STATE="$(Execute_command "${LOCATION}" "midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",${JOBID}]]\" | jq -r '.[0].state'")"
            [[ "${JOB_STATE}" == "SUCCESS" ]] && break
            [[ "${JOB_STATE}" =~ ^(FAILED|ABORTED|ERROR)$ ]] \
                && Background_error "ERROR: ${ACTION^} of ${LOCATION} ${FULL_APP_NAME} ended in state ${JOB_STATE}."
            ((TIMEOUT_COUNTER++))
            [[ "${TIMEOUT_COUNTER}" -gt "${MAX_TIMEOUT}" ]] && Background_error "ERROR: Waiting for ${LOCATION} ${FULL_APP_NAME} to ${ACTION} has timed out."
            echo -n "."
            sleep 1
        done
```

(Declare `local JOB_STATE` with the other locals at the top of the function
instead if you prefer — keep declarations grouped as the codebase does.)

## 4. Mounted-check passes when the dataset doesn't exist — [lib/rep_filesystems.bash:103-104](../lib/rep_filesystems.bash#L103-L104) — ✅ DONE (2026-07-03)

`[[ "$(... zfs list -H -o mounted X)" == "no" ]]` only errors when zfs prints
exactly `no`. If the dataset name is wrong or `zfs list` fails, output is
empty → the check *passes*. Also: the pool inside the error message calls
`Resolve_pool` **without** the `POOL_TYPE` argument, so the message can name
the wrong pool.

**Fix:** invert to require `yes`, and pass `${POOL_TYPE}` in the message too:

```bash
    [[ "$(Execute_command local "zfs list -H -o mounted $(Resolve_pool "${LOCAL_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds")" == "yes" ]]   || Background_error "ERROR: $(Resolve_pool "${LOCAL_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds on truenas-${LOCAL_SERVER_ID} is not mounted (and/or unlocked)."
    [[ "$(Execute_command remote "zfs list -H -o mounted $(Resolve_pool "${REMOTE_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds")" == "yes" ]] || Background_error "ERROR: $(Resolve_pool "${REMOTE_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds on truenas-${REMOTE_SERVER_ID} is not mounted (and/or unlocked)."
```

## 5. Impacted-dataset discovery ignores the property VALUE — [lib/rep_filesystems.bash:116-123](../lib/rep_filesystems.bash#L116-L123) — ⏸ DEFERRED (changes an actual data-selection query, not just an additive guard — verify dataset lists match before/after on the host)

The `zfs list | xargs zfs get all | grep " autobackup:${TASK_SCOPE}"` pipeline
matches on property *name* only. A dataset with
`autobackup:<scope>=false` (zfs_autobackup's documented opt-out convention)
would still be unmounted/remounted here, while zfs_autobackup itself excludes
it. Compare `Cleanup_vm_disk_tags` ([lib/rep_vms.bash:299-301](../lib/rep_vms.bash#L299-L301)),
which correctly filters `$2=="true"`.

**Fix:** replace the whole pipeline with a single value-filtered `zfs get`
(no dataset operand = all datasets; keep the inline location expression as-is):

```bash
    mapfile -t IMPACTED_DATASETS < <(
        Execute_command $([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote) \
            "zfs get -H -t filesystem,volume -o name,value \"autobackup:${TASK_SCOPE}\" \
            | awk '\$2==\"true\" {print \$1}' \
            | sed -E 's|.*/encrypted-ds/||'"
    )
```

Behavior notes (verify in the `--test` run): inherited `true` on children is
still included (value column shows the inherited value) — same as before and
same as zfs_autobackup's own selection; `-` (unset) and `false` are now
excluded.

## 6. `Background_error` inside `$( )` cannot abort the run → empty-pool guards — ✅ DONE (2026-07-03)

`Resolve_pool` calls `Background_error` on bad input, but every call site
invokes it inside command substitution: `SOURCE_POOL="$(Resolve_pool ...)"`.
The `exit 1` kills only the **subshell**. Worse, it kills `TAIL_PID`, so the
console goes silent while the background script *continues* with an **empty
pool variable** — producing paths like `/mnt//encrypted-ds/...` that later
feed `rsync --delete`, `rm -rf`, and `zfs destroy`.

**Fix:** validate non-empty results immediately after each resolution block
(do NOT try to make `Background_error` subshell-safe — that's a bigger change):

- [lib/rep_apps.bash:107-117](../lib/rep_apps.bash#L107-L117) — after the
  `if/else` block add:
  ```bash
    [[ -n "${SOURCE_POOL}" && -n "${TARGET_POOL}" ]] || Background_error "ERROR: Failed to resolve source/target pool (source='${SOURCE_POOL}', target='${TARGET_POOL}')."
  ```
- [lib/rep_vms.bash:692-702](../lib/rep_vms.bash#L692-L702) — same line after
  that `if/else` block.
- [lib/rep_filesystems.bash:106-114](../lib/rep_filesystems.bash#L106-L114) —
  after the `if/elif` block add:
  ```bash
    [[ "${TARGET_PARENT_DATASET}" != "/encrypted-ds" ]] || Background_error "ERROR: Failed to resolve target pool for TARGET_PARENT_DATASET."
  ```
- [lib/rep_vms.bash:327](../lib/rep_vms.bash#L327) (`Tag_vm_disks`) and
  [lib/rep_vms.bash:296](../lib/rep_vms.bash#L296) (`Cleanup_vm_disk_tags`) —
  after `POOL_NAME=...` add:
  ```bash
    [[ -n "${POOL_NAME}" ]] || Background_error "ERROR: Failed to resolve pool for server '${SERVER_ID}'."
  ```
- [lib/snap_rollup.bash:13](../lib/snap_rollup.bash#L13) — resolve the pool
  into a variable first, then build `ROLLUP_CMD` from it:
  ```bash
    local TARGET_POOL
    TARGET_POOL="$(Resolve_pool "${LOCAL_TARGET}${REMOTE_TARGET}")"
    [[ -n "${TARGET_POOL}" ]] || Background_error "ERROR: Failed to resolve rollup target pool."
    ROLLUP_CMD="${SCRIPT_DIR/${LOCAL_SOURCE}${REMOTE_SOURCE}/${LOCAL_TARGET}${REMOTE_TARGET}}/../../zfs-rollup/rollup.py -v --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 ${TARGET_POOL}/encrypted-ds/media-ds"
  ```

## 7. Docker container matched by substring — [lib/docker.bash:16](../lib/docker.bash#L16) and [lib/docker.bash:40](../lib/docker.bash#L40) — ✅ DONE (2026-07-03)

`grep "${CONTAINER_NAME}"` is a substring match: `immich-master-server-1` also
matches `immich-master-server-10`. Multiple matches make `CURRENT_STATE`
multi-line, which never equals the desired state → spurious 60 s timeout.

**Fix (both places):** replace `| grep \"...\" | awk '{print \$2}'` with an
exact first-field match:

```bash
docker ps -a --format '{{.Names}} {{.State}}' | awk -v c=\"${CONTAINER_NAME}\" '\$1==c {print \$2}'
```

(In `Control_docker_containers` the variable is `${CONTAINER}`.)

## 8. Target VM state never checked before `vm.delete` — [lib/rep_vms.bash:736](../lib/rep_vms.bash#L736) — ⏸ DEFERRED (VM delete/recreate path — do together with items 2's caution, host-verify a case where the target VM is legitimately running)

The source VM must be STOPPED ([rep_vms.bash:728](../lib/rep_vms.bash#L728)),
but in a dual-active topology the same VM may be **RUNNING on the target**;
`Delete_vm_on_destination` then fails mid-run and aborts everything.

**Fix:** in `Perform_vm_replication`, right after the `Vm_is_stopped "SOURCE"`
check, add a target-side check that only applies when the VM exists on the
target:

```bash
        # b. Validate target state (VM may legitimately not exist on target yet)
        local TARGET_STATE
        TARGET_STATE="$(jq -r --arg VM "${VM}" '.[] | select(.name == $VM) | .status.state' <<< "${TARGET_ALL_VM_JSON}")"
        if [[ -n "${TARGET_STATE}" && "${TARGET_STATE}" != "STOPPED" ]]; then
            echo "Replication skipped for VM ${VM} (state on target='${TARGET_STATE}')"
            ((FAILED++))
            continue
        fi
```

(Declare `TARGET_STATE` with the other locals at the top of
`Perform_vm_replication` — `local` inside a loop is legal but the codebase
groups declarations.)

## 9. `mkdir -p` on the target runs even with `--test` — [lib/rep_vms.bash:522](../lib/rep_vms.bash#L522) — ✅ DONE (2026-07-03)

`Rsync_vm_file_disks` creates the target directory without the `test` token, so
`--test` leaves empty directories behind on the target.

**Fix:**

```bash
            Execute_command "${TARGET_LOCATION}${TEST_MODE:+"_test"}" "mkdir -p \"${TARGET_PATH%/*}\"" \
                || Background_error "ERROR: Failed to create target dir ${TARGET_PATH%/*} for VM '${VM}'"
```

## 10. `PERFORM_ROLLUP` set twice — [lib/cli.bash:71-76](../lib/cli.bash#L71-L76) — ✅ DONE (2026-07-03)

`--subtask=all` assigns `PERFORM_ROLLUP="true"` on both line 71 and line 76.
Delete one of them. (Cosmetic.)

## 11. Echoed restore command drifted from the executed one — [lib/immich_db.bash:93-95](../lib/immich_db.bash#L93-L95) — ✅ DONE (2026-07-03)

The executed command (line 95) includes `--host=localhost`; the echoed preview
(line 93) doesn't. Add `--host=localhost` to the echo so the log matches
reality.

## 12. `zfs mount`/`umount`'s exit status is never checked — [lib/rep_filesystems.bash:61](../lib/rep_filesystems.bash#L61) — ✅ DONE (2026-07-03)

Found while investigating plan 08's "dataset is busy" issue. `l_Toggle_mounts`
runs a **plain** `zfs umount` (no `-f`) on the target dataset before every
`zfs_autobackup` invocation — and its exit status was discarded:

```bash
Execute_command "${EXEC_MODE}" "zfs ${ACTION} '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"
CHANGED="true"
```

Standard Unix `umount` fails with `EBUSY` whenever something still has an open
file handle on the filesystem — exactly the condition plan 08 documents (an
active SMB client on the target). Since the failure was never checked, this
almost certainly means: umount silently fails, the script proceeds anyway,
and the *same* underlying busy condition surfaces again much later, far less
usefully, inside `zfs_autobackup`'s own receive. Turning this into a loud,
immediate, correctly-attributed failure (right at the pre-flight unmount
step, before any of that dataset's potentially-hours-long transfer even
starts) is a clear win regardless of what else plan 08 decides to do.

**Fix:**

```bash
Execute_command "${EXEC_MODE}" "zfs ${ACTION} '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'" \
    || Background_error "ERROR: Failed to ${ACTION} '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}' on truenas-${TARGET_SERVER_ID} (dataset busy?)."
CHANGED="true"
```

Since `l_Toggle_mounts` handles both `mount` and `umount` through the same
code path, this also makes a failed **re-mount** after the sync abort loudly
instead of silently — a natural extension of the same fix, not scope creep.

**Deliberately not added:** a `-f` (force) flag. Forcing the unmount through
while a client still has the dataset open risks corrupting/truncating
whatever that client was actively writing — worse than the sync failing.
See [plan 08](08-known-operational-issues.md) for the fuller reasoning.

**Follow-up, same day: basic diagnostics added to this failure branch.** On
a mount/umount failure, before aborting, now captures `fuser -vm` on the
mountpoint (what process has it open) and `smbstatus -L` (locked files via
Samba) directly into the log, via a shared `l_Print_diagnostics` helper (see
item 13's write-up for the helper itself and the 2026-07-03 formatting pass):

```bash
if ! Execute_command "${EXEC_MODE}" "zfs ${ACTION} '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"; then
    echo "  ${ACTION^} failed (dataset busy?) — gathering diagnostics:"
    l_Print_diagnostics "${EXEC_MODE}" "${TARGET_SERVER_ID}" "${TARGET_PARENT_DATASET}" "${IMPACTED_DATASET}"
    Background_error "ERROR: Failed to ${ACTION} '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}' on truenas-${TARGET_SERVER_ID} (dataset busy?)."
fi
```

This is the automatic-evidence-gathering idea from plan 08, scoped down to
just this one failure branch. No test-mode interaction to worry about: this
branch is unreachable under `--test` (`Execute_command` with a `_test`-suffixed
mode never actually runs the underlying command, so it can't "fail" and this
whole `if` body is never entered). **Caveat inherited from plan 08's
falsified-theory discussion:** this specific branch only fires if the
*pre-flight* unmount itself fails — given the evidence that the dominant
failure happens *mid-transfer*, hours into an already-successful receive, this
diagnostic will likely stay silent for the failure mode you actually see most
often. It's still worth having for the rarer case it does cover.

**Confirmed live (2026-07-03), and one visibility quirk found while testing:**
a deliberate test (killing the `zfs_autobackup` process mid-run) exercised
item 13's version of this same diagnostic successfully — output ran cleanly
on both target and source. But the *subsequent* remount (from item 14's trap)
didn't appear on the live terminal, even though `zfs list -o name,mounted`
afterward confirmed it genuinely happened. Root cause: `Background_error`
(`lib/common.bash`) kills `TAIL_PID` **before** calling `exit` — and it's
`exit` that triggers our `EXIT` trap. So by the time the trap's remount
`echo`s run, the `tail -f` that streams the log to your terminal is already
dead. The remount's output still reaches the actual log **file** (a separate
`>>` redirect, unaffected by the tail being killed) — just not the live view.
Not a functional bug, and not fixed here (would mean reordering
`Background_error`'s kill/exit sequence, a bigger change than this warrants
right now) — but worth knowing: check the log file directly, not just what
scrolled past live, when diagnosing a future failure.

## 13. Diagnostics on `zfs_autobackup`'s own failure — [lib/rep_filesystems.bash:88-113](../lib/rep_filesystems.bash#L88-L113) — ✅ DONE (2026-07-03)

The template from item 12, applied to the failure point that actually matters
most: `l_Execute_replication_and_remount`'s own `if ${ZFS_AUTOBACKUP_COMMAND}...;
then ... else Background_error "ERROR: ZFS Replication failed"; fi` had no
diagnostic capture at all. Added `fuser -vm` (per impacted dataset) and
`smbstatus -L`, run on **both target and source** — not just target — since
`zfs_autobackup`'s own log already tags failures `[Source]`/`[Target]`
separately (the `zfs send` side often shows a downstream, consequential
failure when the real cause is on the `recv`/target side, as seen in plan
08's log excerpts), and rather than parse that fragile text to guess which
side to check, checking both is simple and the commands are read-only.

Needed a new `SOURCE_PARENT_DATASET` (mirroring the existing
`TARGET_PARENT_DATASET`, computed in the same task/host branch, with the
same empty-pool guard as plan 01 item 6) since no source-side equivalent
existed yet.

**Deliberately does NOT reuse `${EXEC_MODE}`** (which carries a `_test`
suffix when `TEST_MODE` is set) for the diagnostic `Execute_command` calls —
computed fresh, unsuffixed `local`/`remote` modes instead. Reason: unlike
item 12's branch (unreachable under `--test`, since `Execute_command`
itself skips execution in test mode), `zfs_autobackup` is invoked **directly**
here, with its own internal `--test` flag, so it can genuinely fail even
during a `--test` run — and `fuser`/`smbstatus` are harmless, read-only
commands with no reason to be suppressed just because `TEST_MODE` is set.

**This is the diagnostic that should actually catch evidence for the
dominant, mid-transfer failure mode** — unlike item 12's version, which only
fires on the rarer pre-flight-unmount failure. See
[plan 08](08-known-operational-issues.md) for the fuller context on why this
one matters more.

## 14. A failed `zfs_autobackup` run leaves target datasets unmounted forever — [lib/rep_filesystems.bash:80-129](../lib/rep_filesystems.bash#L80-L129) — ✅ DONE (2026-07-03)

**Discovered live, not by inspection first: a real run confirmed
`backup-desktop-ds`, `backup-elke-hp-ds`, `backup-elke-sony-ds`,
`backup-htpc-ds`, `dl-ds`, and `shared-ds` were all sitting `MOUNTED=no` on
`truenas-backup`, hours after the "dataset is busy" failure from earlier
that day** (`zfs list -o name,mounted -r backup-pool/encrypted-ds` — five
`no`s where there should have been `yes`, `media-ds` — uninvolved in that
failure — correctly `yes`). Root cause:

```bash
l_Toggle_mounts "umount" "${IMPACTED_DATASETS[@]}"   # unmounts, fills UNMOUNTED_LIST
# --- Run zfs_autobackup ---
if ${ZFS_AUTOBACKUP_COMMAND}...; then
    ...
else
    Background_error "ERROR: ZFS Replication failed"   # exits the WHOLE script here
fi
...
l_Toggle_mounts "mount" "${UNMOUNTED_LIST[@]}"          # never reached if the above fired
```

`Background_error` calls `exit 1`, which terminates the entire script
immediately — the remount call at the end is simply never reached. Once a
dataset is left unmounted this way, it **stays that way indefinitely**, even
across later *successful* runs: the next run's own `l_Toggle_mounts "umount"`
sees the dataset is already unmounted, so it correctly does nothing and adds
nothing to *that* run's `UNMOUNTED_LIST` — meaning the eventual remount call
has nothing to act on for that dataset either. A share pointing at an
unmounted dataset serves an empty directory to clients in the meantime.

**Rejected approach (correctly, by the user):** remounting all of
`IMPACTED_DATASETS` unconditionally at the end. This would blindly override
a dataset that might be unmounted **on purpose**, for reasons unrelated to
this script (manual maintenance, etc.) — not this script's business to
undo. The actual bug is narrower: only *this run's own* `UNMOUNTED_LIST`
should ever get remounted, and the fix needs to guarantee that specific
remount happens even when `Background_error` aborts before reaching it —
not redefine what should be remounted.

**Fix — `trap ... EXIT`, scoped to exactly what this invocation unmounted:**

```bash
l_Toggle_mounts "umount" "${IMPACTED_DATASETS[@]}"

trap 'l_Toggle_mounts "mount" "${UNMOUNTED_LIST[@]}"' EXIT

# --- Run zfs_autobackup ---
...
l_Toggle_mounts "mount" "${UNMOUNTED_LIST[@]}"
trap - EXIT
```

Since the trap string is single-quoted, `${UNMOUNTED_LIST[@]}` is expanded
at *fire* time, not registration time — so it correctly reflects whatever
`UNMOUNTED_LIST` had accumulated at the moment of failure (including a
partial umount loop that itself hit `Background_error` partway through).
The trap covers every abort path in this section, not just the
`zfs_autobackup` failure — the `cd -` failure a few lines later is covered
too. Cleared (`trap - EXIT`) once the normal-path remount succeeds, so it
can't linger into a *later*, unrelated invocation of this same function
later in the same script run (it's called once per scope, and again for VM
zvol replication) — confirmed no other `trap` exists anywhere else in this
codebase to accidentally clobber.

**This fix does NOT retroactively repair the six datasets already stuck
unmounted from today's incident** — that needed a manual, out-of-band fix:

```bash
for ds in backup-desktop-ds backup-elke-hp-ds backup-elke-sony-ds backup-htpc-ds dl-ds shared-ds; do
    zfs mount "backup-pool/encrypted-ds/${ds}"
done
```

It only prevents the *same* failure mode from recurring on future runs.

---

## Verification checklist

1. Items 1, 3, 4, 6, 7, 9, 10, 11, 12, 13, 14 — `bash -n` and `shellcheck -x`
   done and clean on every touched file (2026-07-03): `cli.bash`,
   `docker.bash`, `immich_db.bash`, `rep_apps.bash`, `rep_filesystems.bash`,
   `rep_vms.bash`, `snap_rollup.bash`. No new findings introduced. Items 1–11
   additionally confirmed on both a `--test` and a real run against
   production data.
2. Items 13/14 — **confirmed live (2026-07-03)** via a deliberate test (killing
   the `zfs_autobackup` process mid-run to force a real failure): item 13's
   diagnostics fired correctly and showed `fuser -vm`/`smbstatus -L` output on
   both target and source (surfaced an active Veeam SMB lease at the time —
   see plan 08 for why that turned out to be a red herring, not the root
   cause); item 14's trap correctly remounted every dataset this run itself
   had unmounted, confirmed via `zfs list -o name,mounted` afterward. One
   visibility quirk found, not a bug — see item 12's "Confirmed live" note
   above for why the remount doesn't show up on the live terminal (only in
   the log file). Item 12 itself (pre-flight umount failure) still hasn't
   been observed to fire — that path remains untested but is low-risk/
   low-likelihood by construction.
3. Item 14 — the deliberate kill-test above also validated the "don't touch
   unrelated datasets" guarantee in practice: only datasets this run's own
   `l_Toggle_mounts "umount"` had touched came back via the trap. Still worth
   a follow-up low-stakes test if opportunity arises: manually `zfs umount`
   some *other*, unrelated dataset on the target before a run and confirm
   this script leaves it alone.
4. Item 1: `./bin/sync_truenas_servers --test --task=master_to_backup --subtask=app_replication --app=doesnotexist`
   — must abort cleanly (tail killed, error printed), not hang.
5. Item 11: eyeball the Immich restore preview line in the log — should now
   show `--host=localhost`, matching the executed command below it.
6. Items 2, 5, 8 remain open — plan separately, with the extra host-side
   verification each calls for in its own section above.
