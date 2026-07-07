# Plan 01 — Bug fixes

Surgical fixes only; no restructuring. Read `plans/README.md` guardrails first.

## Done (2026-07-03/04)

All verified: `bash -n` + `shellcheck -x` clean, confirmed via `--test` and real
production runs. Items 12–14 additionally confirmed via a deliberate kill-test.

| # | Where | Fix |
|---|---|---|
| 1 | `rep_apps.bash` | Unknown `--app` now aborts via `Background_error` instead of a bare `exit 1` that hung the tail |
| 3 | `rep_apps.bash::Control_app` | Poll loop now detects `FAILED`/`ABORTED`/`ERROR` job states instead of spinning to a misleading 60s timeout |
| 4 | `rep_filesystems.bash` | Mounted-check now requires `yes` explicitly (an empty/wrong `zfs list` result used to silently pass) |
| 6 | `rep_apps/rep_vms/rep_filesystems/snap_rollup` | Guard added after every `Resolve_pool` call — an unresolvable pool used to silently propagate as an empty string into rsync/zfs paths |
| 7 | `docker.bash` | Container match is now exact (was substring — `-server-1` also matched `-server-10`, causing spurious timeouts) |
| 9 | `rep_vms.bash::Rsync_vm_file_disks` | `mkdir -p` on target now suppressed under `--test` |
| 10 | `cli.bash` | Removed duplicate `PERFORM_ROLLUP` assignment |
| 11 | `immich_db.bash` | Echoed restore preview now matches the executed command (`--host=localhost`) |
| 12 | `rep_filesystems.bash::l_Toggle_mounts` | `zfs mount`/`umount` exit status is now checked (was silently discarded) and triggers `fuser`/`smbstatus` diagnostics on failure |
| 13 | `rep_filesystems.bash::l_Execute_replication_and_remount` | `zfs_autobackup` failures now capture the same `fuser`/`smbstatus` diagnostics, on **both** target and source |
| 14 | `rep_filesystems.bash` + `common.bash` | A failed run no longer leaves datasets unmounted forever — restore is now guaranteed via the shared cleanup stack (`Register_cleanup`/`Run_cleanup`, see `architectural_patterns.md`) |

Item 14 was found live: a real failure had left 6 production datasets
unmounted for hours (share serving an empty directory to clients). The fix is
scoped to exactly what the run itself unmounted — never a blind "remount
everything," since a dataset can be unmounted on purpose for unrelated
reasons. See `08-known-operational-issues.md` for the investigation this was
found during, and `architectural_patterns.md` for the cleanup-stack mechanism
now used throughout the codebase.

---

Item 8 (target-VM-running check) is now also done — folded into
[plan 09](09-stop-running-vms-optarg.md), see below.

## Remaining (deferred — needs dedicated attention + host verification)

### 2. `Restore_immich_DB` wipes the DB before checking the target app is running

[lib/immich_db.bash:69-88](../lib/immich_db.bash#L69-L88)

`midclt app.stop` **destroys** an app's containers. If the target Immich app
was already STOPPED before the sync, `Restore_immich_DB`'s container
discovery finds nothing, but the `rm -rf` of `immich-pgdata-ds/*` **still
executes** before that's noticed — the run aborts *after* deleting the
target DB (recoverable from the rsync'd dump, but only manually).

**Fix:** add a guard at the top of `Restore_immich_DB`, mirroring
`Backup_immich_DB`'s existing one at [lib/immich_db.bash:14](../lib/immich_db.bash#L14):

```bash
if [[ "$(Execute_command "${TARGET_LOCATION}" "midclt call app.query | jq -r '.[] | select(.name==\"immich-${TARGET_SERVER_ID}\") | .state'")" != "RUNNING" ]]; then
    Background_error "ERROR: To restore the Immich DB, immich-${TARGET_SERVER_ID} must be in a running state."
fi
```

### 5. Impacted-dataset discovery ignores the property VALUE

[lib/rep_filesystems.bash:116-123](../lib/rep_filesystems.bash#L116-L123)

The `zfs list | xargs zfs get all | grep " autobackup:${TASK_SCOPE}"` pipeline
matches on property *name* only — a dataset explicitly opted out
(`autobackup:<scope>=false`, zfs_autobackup's documented convention) still
gets unmounted/remounted here, even though zfs_autobackup itself excludes it.
Compare `Cleanup_vm_disk_tags` ([lib/rep_vms.bash:299-301](../lib/rep_vms.bash#L299-L301)),
which correctly filters `$2=="true"`.

**Fix:** replace with a value-filtered `zfs get`:

```bash
mapfile -t IMPACTED_DATASETS < <(
    Execute_command "$([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote)" \
        "zfs get -H -t filesystem,volume -o name,value \"autobackup:${TASK_SCOPE}\" \
        | awk '\$2==\"true\" {print \$1}' \
        | sed -E 's|.*/encrypted-ds/||'"
)
```

Verify in a `--test` run that the impacted-dataset list is unchanged for
today's actual (all-`true`-or-unset) datasets, and that `-`/`false` datasets
are now correctly excluded.

### 8. Target VM state never checked before `vm.delete` — ✅ DONE via plan 09

The source VM must be STOPPED, but in this dual-active topology the same VM
may legitimately be **RUNNING on the target** — `Delete_vm_on_destination`
used to fail mid-run and abort everything. **Folded into
[plan 09](09-stop-running-vms-optarg.md)** (2026-07-04), which added the
target-side state check in the same place it added the source-side one: a
running target is skipped by default, or (with `--stop-running-vms`) stopped
before delete and handled per that plan. Still pending the same host
verification plan 09 lists.

---

## Verification checklist

1. Deferred items only — the done items above are already verified.
2. Item 2: force a restore against a stopped target Immich app; must abort
   before the `rm -rf`, not after.
3. Item 5: `--test` run before/after — impacted-dataset list must be
   unchanged for real datasets; confirm a `false`-tagged dataset (if any) is
   now excluded.
4. Item 8: `--test` run with a VM legitimately running on the target; must
   skip cleanly instead of aborting the whole run.
