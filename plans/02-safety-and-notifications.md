# Plan 02 — Failure notification & operational safety

The script currently emails **only on success** ([bin/sync_truenas_servers:99-103](../bin/sync_truenas_servers#L99-L103)).
A failed run kills the tail and exits silently — for a backup orchestrator the
failure email is the one that matters. This plan adds failure notification,
prevents concurrent runs, and stops unbounded log/dump growth.

Read `plans/README.md` "Global guardrails" first. Items independent; implement
one at a time.

---

## 1. Send a FAILURE email from `Background_error` — [lib/common.bash:5-14](../lib/common.bash#L5-L14)

**Change:** in `Background_error`, after the `echo -e "$1"` and *before*
killing the tail, send the log by mail. Guard on the variables existing so the
function stays safe if called before backgrounding:

```bash
function Background_error() {
    echo -e "$1"
    if [[ -n "${EMAIL_TO}" && -n "${LOG_FILE}" && -f "${LOG_FILE}" ]]; then
        {
          echo "Subject: FAILED - Sync from TrueNAS-${LOCAL_SOURCE^}${REMOTE_SOURCE^} to TrueNAS-${LOCAL_TARGET^}${REMOTE_TARGET^} server"
          echo
          echo -e "$1"
          echo
          cat "${LOG_FILE}"
        } | sendmail "${EMAIL_TO}"
    fi
    if [[ -n "${TAIL_PID}" ]]; then
        sleep 1
        kill "${TAIL_PID}"
    else
        echo "ERROR: Couldn't find tail PID. Are you sure this is properly running in the background?"
    fi
    exit 1
}
```

Note: `sleep 1` before the kill stays where it is so the final error line
still reaches the console via the tail.

## 2. Prevent concurrent runs (flock) — [bin/sync_truenas_servers:106-109](../bin/sync_truenas_servers#L106-L109)

Two simultaneous runs would fight over apps (stop/start), rsync the same trees,
and — when started within the same minute — share one log file and kill each
other's tails. Add an exclusive lock in the **parent** branch, *before* the
`nohup` line. The child inherits fd 200 (same open file description), so the
lock is held until the background child exits, even after the parent is gone:

```bash
else
    exec 200>"${LOG_DIR}/.sync_truenas_servers.lock"
    if ! flock -n 200; then
        echo "ERROR: Another ${SCRIPT_FILENAME} run is already active (lock: ${LOG_DIR}/.sync_truenas_servers.lock)."
        exit 1
    fi
    echo "Starting ${SCRIPT_FILENAME} in the background (logfile = ${LOG_FILE})."
    nohup "$0" --running_in_background "${LOG_FILE}" "${BACKUP_OPTIONS[@]}"  >>"${LOG_FILE}" 2>&1 & { sleep 1; tail -f "${LOG_FILE}"; }
fi
```

(Also quotes `$0` while touching that line — harmless hardening.)

**Verify:** start a real (or `--test`) run, then immediately start a second
one from another shell: the second must refuse. After the first finishes, a
new run must start normally.

## 3. Log retention — [bin/sync_truenas_servers:9](../bin/sync_truenas_servers#L9)

`logs/` currently holds 400+ files and grows forever. After the
`mkdir -p "${LOG_DIR}"` line, add:

```bash
# Prune old run logs (retention in days, overridable in config.local.bash)
find "${LOG_DIR}" -maxdepth 1 -name "${SCRIPT_FILENAME}*.log" -mtime +"${LOG_RETENTION_DAYS:-60}" -delete
```

Note this runs before `config.local.bash` is sourced only if placed too early —
place it **after** the config `source` block (after line 20) so an optional
`LOG_RETENTION_DAYS` in `config.local.bash` takes effect. Also add
`declare LOG_RETENTION_DAYS="60"` to `config/config.example.bash` as
documentation.

## 4. Immich dump retention — [lib/immich_db.bash:33-34](../lib/immich_db.bash#L33-L34)

Every run leaves `<date>_immich_backup.dump.sql.gz` in
`immich-data-ds/backups/` forever, and the rsync mirrors the growth to the
target. After the successful `mv` in `Backup_immich_DB` (inside the existing
`[[ -z "${TEST_MODE}" ]]` block, after the move's error check), add:

```bash
        # Prune old dumps on the source; rsync --delete mirrors this to the target.
        Execute_command "${SOURCE_LOCATION}" "find \"/mnt/${SOURCE_POOL}/encrypted-ds/app-ds/immich-ds/immich-data-ds/backups/\" -maxdepth 1 -name '*_immich_backup.dump.sql.gz' -mtime +\"${DUMP_RETENTION_DAYS:-30}\" -delete"
```

Add `declare DUMP_RETENTION_DAYS="30"` to `config/config.example.bash`.
Do **not** delete on the target directly — `backups` is in `app_dir_list`, so
the source-side prune propagates via the existing `rsync --delete`.

## 5. Document the known exit-code limitation (no code change)

The foreground parent always exits 0 ([bin/sync_truenas_servers:110](../bin/sync_truenas_servers#L110))
even when the background child fails — acceptable for interactive use, wrong
for cron. Add one line to `CLAUDE.md`/`AGENTS.md` under "Essential commands":

> Note: the foreground wrapper always exits 0; success/failure is signaled by
> email and the log, not the exit code. Don't chain `&&` off a run.

---

## Verification checklist

1. `bash -n bin/sync_truenas_servers lib/common.bash lib/immich_db.bash`.
2. Force a failure in `--test` mode (e.g. `--app=doesnotexist` after plan 01
   item 1) → a FAILED email must arrive containing the log.
3. Concurrency: second simultaneous invocation refused (item 2).
4. `logs/` shrinks to the retention window on next run; no `.lock` file
   deleted by the pruner (pattern only matches `*.log`).
