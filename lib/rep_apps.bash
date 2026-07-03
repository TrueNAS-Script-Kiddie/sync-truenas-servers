#!/usr/bin/bash
# lib/rep_apps.bash
# TrueNAS Application Replication Orchestration

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
    shift 2
    local LOCATIONS=( "$@" )

    local LOCATION
    local SERVER_ID_VAR
    local STOPPED_LIST_VAR
    local FULL_APP_NAME
    local APP_STATE
    local -a STOPPED_LIST
    local PERFORM_ACTION=0

    for LOCATION in "${LOCATIONS[@]}"; do
        SERVER_ID_VAR="${LOCATION^^}_SERVER_ID"
        STOPPED_LIST_VAR="${LOCATION^^}_STOPPED_LIST[@]"
        FULL_APP_NAME="${APP_NAME}-${!SERVER_ID_VAR}"
        STOPPED_LIST=( "${!STOPPED_LIST_VAR}" )

        APP_STATE="$(Execute_command "${LOCATION}" \
            "midclt call app.query | jq -r '.[] | select(.name==\"${FULL_APP_NAME}\") | .state'")"

        PERFORM_ACTION=0
        case "${ACTION}" in
          start)
            # shellcheck disable=SC2076  # elif below is a deliberate literal-substring match, not regex (APP_NAME may contain regex metacharacters)
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
    done
}

function Perform_app_replication() {
    local SOURCE_LOCATION
    local TARGET_LOCATION
    local SOURCE_POOL
    local TARGET_POOL
    local SOURCE_SERVER_ID
    local TARGET_SERVER_ID

    local FOUND

    local -a SELECTED_APP_JSON_LIST
    local -a APP_JSON_LIST
    local APP_NAME
    local JSON_APP_NAME
    local APP_JSON

    local -a REMOTE_STOPPED_LIST 
    local -a LOCAL_STOPPED_LIST
    local FULL_SOURCE_PATH
    local FULL_TARGET_PATH
    local -a APP_DIRS_TO_RSYNC_LIST
    local APP_DIR_TO_RSYNC

    # Prepare vars
    [[ -n "${LOCAL_SOURCE}" ]] && SOURCE_LOCATION="local" || SOURCE_LOCATION="remote"
    [[ -n "${LOCAL_TARGET}" ]] && TARGET_LOCATION="local" || TARGET_LOCATION="remote"

    if [[ "${SOURCE_LOCATION}" == "local" ]]; then
        SOURCE_POOL="$(Resolve_pool "${LOCAL_SERVER_ID}" "fast")"
        SOURCE_SERVER_ID="${LOCAL_SERVER_ID}"
        TARGET_POOL="$(Resolve_pool "${REMOTE_SERVER_ID}" "fast")"
        TARGET_SERVER_ID="${REMOTE_SERVER_ID}"
    else
        SOURCE_POOL="$(Resolve_pool "${REMOTE_SERVER_ID}" "fast")"
        SOURCE_SERVER_ID="${REMOTE_SERVER_ID}"
        TARGET_POOL="$(Resolve_pool "${LOCAL_SERVER_ID}" "fast")"
        TARGET_SERVER_ID="${LOCAL_SERVER_ID}"
    fi

    echo "##########################################"
    echo "### Performing Application Replication ###"
    echo "##########################################"
    echo

    mapfile -t APP_JSON_LIST < <(jq -c '.apps[]' "${SCRIPT_DIR}/../config/apps.json")
    if [[ ${#APP_LIST[@]} -eq 0 ]]; then
        # No --app specified → select all
        SELECTED_APP_JSON_LIST=("${APP_JSON_LIST[@]}")
    else
        # Filter only requested apps
        for APP_NAME in "${APP_LIST[@]}"; do
            FOUND="false"
            for APP_JSON in "${APP_JSON_LIST[@]}"; do
                JSON_APP_NAME="$(jq -r '.name' <<< "${APP_JSON}")"
                if [[ "${JSON_APP_NAME}" == "${APP_NAME}" ]]; then
                    SELECTED_APP_JSON_LIST+=("${APP_JSON}")
                    FOUND="true"
                    break
                fi
            done
            if [[ "${FOUND}" == "false" ]]; then
                echo "ERROR: Requested app '${APP_NAME}' not found in config/apps.json"
                exit 1
            fi
        done
    fi

    for APP_JSON in "${SELECTED_APP_JSON_LIST[@]}"; do
        REMOTE_STOPPED_LIST=()
        LOCAL_STOPPED_LIST=()

        APP_NAME="$(jq -r '.name' <<< "${APP_JSON}")"
        mapfile -t APP_DIRS_TO_RSYNC_LIST < <(jq -r '.app_dir_list[]' <<< "${APP_JSON}")

        # Prepare the rsyncs 
        FULL_SOURCE_PATH="$([[ "${SOURCE_LOCATION}" == "remote" ]] && echo "truenas-${SOURCE_SERVER_ID}:")/mnt/${SOURCE_POOL}/encrypted-ds/app-ds/$(jq -r '.app_base_path' <<< "${APP_JSON}")"
        FULL_TARGET_PATH="$([[ "${TARGET_LOCATION}" == "remote" ]] && echo "truenas-${TARGET_SERVER_ID}:")/mnt/${TARGET_POOL}/encrypted-ds/app-ds/$(jq -r '.app_base_path' <<< "${APP_JSON}")"

        # Check if local and remote application datasets are available
        Execute_command "${SOURCE_LOCATION}" "test -d \"${FULL_SOURCE_PATH#*:}\"" \
            || Background_error "ERROR: '${FULL_SOURCE_PATH}' does not exist. Is the dataset mounted and unlocked?"
        Execute_command "${TARGET_LOCATION}" "test -d \"${FULL_TARGET_PATH#*:}\"" \
            || Background_error "ERROR: '${FULL_TARGET_PATH}' does not exist. Is the dataset mounted and unlocked?"

        # # Perform pre-action function from the json (for example: Backup Immich Postgres DB)
        PRE_ACTION="$(jq -r '.pre_action // empty' <<< "${APP_JSON}")"
        [[ -n "${PRE_ACTION}" ]] && "$PRE_ACTION"

        # Stop the application locally and remotely
        Control_app_with_checks "${APP_NAME}" stop "local" "remote"
        echo

        for APP_DIR_TO_RSYNC in "${APP_DIRS_TO_RSYNC_LIST[@]}"; do
            # Check if the source and target directories exist
            Execute_command "${SOURCE_LOCATION}" "test -d \"${FULL_SOURCE_PATH#*:}/${APP_DIR_TO_RSYNC}\"" \
                || Background_error "ERROR: '${FULL_SOURCE_PATH}/${APP_DIR_TO_RSYNC}' does not exist. Is the application properly installed?"
            Execute_command "${TARGET_LOCATION}" "test -d \"${FULL_TARGET_PATH#*:}/${APP_DIR_TO_RSYNC}\"" \
                || Background_error "ERROR: '${FULL_TARGET_PATH}/${APP_DIR_TO_RSYNC}' does not exist. Is the application properly installed?"

            # Perform the rsyncs
            echo "rsync ${TEST_MODE:+--dry-run} -e \"ssh -F ${SSH_CONFIG_FILE}\" --delete -aHX \"${FULL_SOURCE_PATH}/${APP_DIR_TO_RSYNC}/\" \"${FULL_TARGET_PATH}/${APP_DIR_TO_RSYNC}/\""
            if rsync ${TEST_MODE:+--dry-run} -e "ssh -F ${SSH_CONFIG_FILE}" --delete -aHX "${FULL_SOURCE_PATH}/${APP_DIR_TO_RSYNC}/" "${FULL_TARGET_PATH}/${APP_DIR_TO_RSYNC}/"; then
                echo "Rsync of '${APP_DIR_TO_RSYNC}' completed successfully"
            else
                Background_error "ERROR: Rsync of '${APP_DIR_TO_RSYNC}' failed"
            fi
            echo
        done

        # Start the application locally and remotely if they were stopped
        Control_app_with_checks "${APP_NAME}" start "local" "remote"
        echo

        # Perform post-action function from the json (for example: Restore Immich Postgres DB)
        POST_ACTION="$(jq -r '.post_action // empty' <<< "${APP_JSON}")"
        [[ -n "${POST_ACTION}" ]] && "$POST_ACTION"
    done

    echo "### Performing Application Replication completed ###"
    echo
}