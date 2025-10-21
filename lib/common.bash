#!/usr/bin/bash
# lib/common.bash
# Common helpers: error handling and command execution

function Background_error() {
    echo -e "$1"
    if [[ -n "${TAIL_PID}" ]]; then
        sleep 1
        kill "${TAIL_PID}"
    else
        echo "ERROR: Couldn't find tail PID. Are you sure this is properly running in the background?"
    fi
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
    return 1
  fi

  return "${RETURN_VALUE}"
}

function Resolve_pool() {
    local SERVER_TYPE="$1"
    local POOL_TYPE="${2:-normal}"   # "fast" or "normal" (default)

    # If SERVER_TYPE is empty, return empty string silently
    [[ -z "${SERVER_TYPE}" || "${SERVER_TYPE}" == "fast" ]] && return 0

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