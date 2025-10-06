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