#!/usr/bin/bash
# lib/common.bash
# Common helpers: error handling and command execution

# Restore-to-previous-state cleanup stack (LIFO). A module calls
# Register_cleanup '<command>' (single-quoted: expanded at fire time, not at
# registration) before entering a section whose side effects must be undone on
# failure — e.g. remounting what rep_filesystems unmounted, or restarting a VM
# this run stopped — and Unregister_cleanup after undoing them itself on the
# normal path. Nesting is supported: Run_cleanup pops newest-first, so an inner
# section's cleanup (remount filesystems) runs before an outer one's (restart
# the VM that owns them). Background_error runs the stack BEFORE killing the
# tail, so cleanup output is visible in the live terminal (not only in the log
# file); the 'trap Run_cleanup; Kill_tail EXIT' in bin/sync_truenas_servers is
# the safety net for any exit that bypasses Background_error.
declare -a CLEANUP_COMMAND_STACK=()

function Register_cleanup() {
    CLEANUP_COMMAND_STACK+=( "$1" )
}

function Unregister_cleanup() {
    if (( ${#CLEANUP_COMMAND_STACK[@]} > 0 )); then
        unset 'CLEANUP_COMMAND_STACK[-1]'
    fi
}

function Run_cleanup() {
    local CLEANUP_COMMAND
    while (( ${#CLEANUP_COMMAND_STACK[@]} > 0 )); do
        CLEANUP_COMMAND="${CLEANUP_COMMAND_STACK[-1]}"
        # Pop BEFORE eval: if a cleanup itself fails into Background_error, the
        # recursive Run_cleanup call continues with the REMAINING entries
        # instead of looping forever on the failing one — so e.g. a failed
        # remount can no longer prevent a registered VM restart from running.
        unset 'CLEANUP_COMMAND_STACK[-1]'
        eval "${CLEANUP_COMMAND}"
    done
}

# Kill the foreground 'tail -f' that streams the log to the terminal. This is
# infrastructure teardown, not restore-to-previous-state — it must run on EVERY
# exit (success included) and always LAST, after all cleanup output has been
# written where the tail can still show it. Idempotent: clears TAIL_PID so a
# second call (Background_error followed by the EXIT trap) is a no-op.
function Kill_tail() {
    if [[ -n "${TAIL_PID}" ]]; then
        sleep 1
        kill "${TAIL_PID}"
        TAIL_PID=""
    fi
}

function Background_error() {
    echo -e "$1"
    Run_cleanup
    if [[ -n "${EMAIL_TO}" && -n "${LOG_FILE}" && -f "${LOG_FILE}" ]]; then
        {
          echo "Subject: FAILED - Sync from TrueNAS-${LOCAL_SOURCE^}${REMOTE_SOURCE^} to TrueNAS-${LOCAL_TARGET^}${REMOTE_TARGET^} server"
          echo
          echo -e "$1"
          echo
          cat "${LOG_FILE}"
        } | sendmail "${EMAIL_TO}"
    fi
    [[ -z "${TAIL_PID}" ]] && echo "ERROR: Couldn't find tail PID. Are you sure this is properly running in the background?"
    Kill_tail
    exit 1
}

function Execute_command() {
  local MODE="$1"
  shift
  local COMMAND="$*"
  local RETURN_VALUE=0

  if [[ "${MODE}" == *local* ]]; then
    [[ "${MODE}" == *verbose* ]] && echo "Executing command: eval ${COMMAND}"
    [[ "${MODE}" != *test* ]]    && { eval "${COMMAND}"; RETURN_VALUE=$?; }

  elif [[ "${MODE}" == *remote* ]]; then
    [[ "${MODE}" == *verbose* ]] && echo "Executing command: ${REMOTE_CMD[*]} \"${COMMAND}\""
    [[ "${MODE}" != *test* ]]    && { "${REMOTE_CMD[@]}" "${COMMAND}"; RETURN_VALUE=$?; }

  else
    Background_error "Incorrect execution mode (${MODE}). Must contain 'local' or 'remote'."
  fi

  return "${RETURN_VALUE}"
}

function Resolve_pool() {
    local SERVER_TYPE="$1"
    local POOL_TYPE="${2:-normal}"   # "fast" or "normal" (default)

    [[ -z "${SERVER_TYPE}" ]] && return 1

    case "${SERVER_TYPE}" in
        master)
            case "${POOL_TYPE}" in
                fast)   echo "ssdmaster-pool" ;;
                normal) echo "master-pool" ;;
                *)      Background_error "ERROR: Invalid \$POOL_TYPE '${POOL_TYPE}' for \$SERVER_TYPE '${SERVER_TYPE}'." ;;
            esac
            ;;
        backup)
            # On backup, both fast and normal map to the same pool
            case "${POOL_TYPE}" in
                fast|normal) echo "backup-pool" ;;
                *) Background_error "ERROR: Invalid \$POOL_TYPE '${POOL_TYPE}' for \$SERVER_TYPE '${SERVER_TYPE}'." ;;
            esac
            ;;
        *)
            Background_error "ERROR: Unknown \$SERVER_TYPE '${SERVER_TYPE}' when resolving pool (\$POOL_TYPE='${POOL_TYPE}')."
            ;;
    esac
}

# Derives the source/target view of the LOCAL_*/REMOTE_* direction model (set by
# cli.bash from ${TASK}:${LOCAL_SERVER_ID}) into globals every subtask shares:
#   SOURCE_LOCATION / TARGET_LOCATION   ("local" or "remote")
#   SOURCE_SERVER_ID / TARGET_SERVER_ID ("master" or "backup")
# Call once after Process_command_line_options. Pools stay per-module via
# Resolve_pool (POOL_TYPE differs per subtask). Does not replace the direction
# model — it is a derived, read-only view on top of it.
function Resolve_direction() {
    # shellcheck disable=SC2034  # SOURCE_/TARGET_ globals consumed across lib/*.bash (direction model, see architectural_patterns.md)
    [[ -n "${LOCAL_SOURCE}" ]] && SOURCE_LOCATION="local" || SOURCE_LOCATION="remote"
    [[ -n "${LOCAL_TARGET}" ]] && TARGET_LOCATION="local" || TARGET_LOCATION="remote"
    if [[ "${SOURCE_LOCATION}" == "local" ]]; then
        SOURCE_SERVER_ID="${LOCAL_SERVER_ID}"
        TARGET_SERVER_ID="${REMOTE_SERVER_ID}"
    else
        SOURCE_SERVER_ID="${REMOTE_SERVER_ID}"
        TARGET_SERVER_ID="${LOCAL_SERVER_ID}"
    fi
}