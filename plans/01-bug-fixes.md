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

---

## Verification checklist

1. Items 1, 3, 4, 6, 7, 9, 10, 11 — `bash -n` and `shellcheck -x` done and
   clean on every touched file (2026-07-03): `cli.bash`, `docker.bash`,
   `immich_db.bash`, `rep_apps.bash`, `rep_filesystems.bash`, `rep_vms.bash`,
   `snap_rollup.bash`. No new findings introduced.
2. **Still needed — real TrueNAS-host `--test` run**: this was static
   parse/lint verification only. On a host:
   `./bin/sync_truenas_servers --test --task=master_to_backup` — must
   complete, banner and email as before, no empty-pool errors, no spurious
   "not mounted" errors (item 4), no container-matching timeouts (item 7).
3. Item 1: `./bin/sync_truenas_servers --test --task=master_to_backup --subtask=app_replication --app=doesnotexist`
   — must abort cleanly (tail killed, error printed), not hang.
4. Item 11: eyeball the Immich restore preview line in the log — should now
   show `--host=localhost`, matching the executed command below it.
5. Items 2, 5, 8 remain open — plan separately, with the extra host-side
   verification each calls for in its own section above.
