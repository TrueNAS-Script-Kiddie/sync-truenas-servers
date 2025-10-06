#!/usr/bin/bash
# lib/apps.bash
# Application control helpers for TrueNAS apps

function Control_app() {
    local FULL_APP_NAME="$1"
    local ACTION="$2"
    local LOCATION="$3"

    local JOBID
    local TIMEOUT_COUNTER="0"
    local MAX_TIMEOUT=60

    if JOBID="$(Execute_command "${LOCATION}" "midclt call app.${ACTION} \"${FULL_APP_NAME}\"")" && [[ -n "${JOBID}" ]]; then
        while [[ "$(Execute_command "${LOCATION}" "midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",${JOBID}]]\" | jq -r '.[0].state'")" != "SUCCESS" ]]; do
            ((TIMEOUT_COUNTER++))
            [[ "${TIMEOUT_COUNTER}" -gt "${MAX_TIMEOUT}" ]] && Background_error "ERROR: Waiting for ${LOCATION} ${FULL_APP_NAME} to ${ACTION} has timed out."
            echo -n "."
            sleep 1
        done
        echo " ${ACTION^} was successful."
    else
        Background_error "ERROR: Failed to ${ACTION} the ${LOCATION} ${FULL_APP_NAME}"
    fi
}

function Control_app_with_checks() {
    local APP_NAME="$1"
    local ACTION="$2"
    local LOCATION="$3"

    local PERFORM_ACTION=0
    local APP_STATE

    local SERVER_ID_VAR="${LOCATION^^}_SERVER_ID"
    local STOPPED_LIST_VAR="${LOCATION^^}_STOPPED_LIST[@]"

    local FULL_APP_NAME="${APP_NAME}-${!SERVER_ID_VAR}"
    local -a STOPPED_LIST=( "${!STOPPED_LIST_VAR}" )

    APP_STATE="$(Execute_command "${LOCATION}" "midclt call app.query | jq -r '.[] | select(.name==\"${FULL_APP_NAME}\") | .state'")"

    case "${ACTION}" in
      start)
        if [[ ! "${APP_STATE}" =~ ^(STOPPED|CRASHED)$ ]]; then
            echo "WARNING: ${FULL_APP_NAME} cannot be started because its state is not 'STOPPED' or 'CRASHED'. It is ${APP_STATE}."
        elif [[ " ${STOPPED_LIST[@]} " =~ " ${APP_NAME} " ]]; then
            PERFORM_ACTION=1
            echo -n "Starting ${LOCATION} ${FULL_APP_NAME} again, as it was also active before."
        fi
        ;;
      stop)
        if [[ ! "${APP_STATE}" =~ ^(RUNNING|DEPLOYING|CRASHED)$ ]]; then
            echo "WARNING: ${FULL_APP_NAME} cannot be stopped because its state is not 'RUNNING', 'DEPLOYING' or 'CRASHED'. It is ${APP_STATE}."
        else
            PERFORM_ACTION=1
            echo -n "Stopping ${LOCATION} ${FULL_APP_NAME}"
        fi
        ;;
      *)
        Background_error "Invalid action: ${ACTION}"
        ;;
    esac

    if (( PERFORM_ACTION )); then
        Control_app "${FULL_APP_NAME}" "${ACTION}" "${LOCATION}"
        [[ "${ACTION}" == "stop" ]] && eval "${LOCATION^^}_STOPPED_LIST+=( \"${APP_NAME}\" )"
    fi
}
