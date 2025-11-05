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

function Perform_app_replication() {
    local -a LOCATIONS_LIST=( "local" "remote" )
    local -a PLEX_FOLDERS_TO_RSYNC_LIST=( "Media" "Metadata" "Plug-ins" "Plug-in Support" )
    local -a IMMICH_FOLDERS_TO_RSYNC_LIST=( "backups" "encoded-video" "library" "profile" "thumbs" "upload" )
    local PLEX_PATH="/mnt/POOL_TO_INSERT/encrypted-ds/app-ds/plex-ds/Library/Application Support/Plex Media Server"
    local IMMICH_PATH="/mnt/POOL_TO_INSERT/encrypted-ds/app-ds/immich-ds/immich-data-ds"

    local LOCAL_SOURCE_POOL="$(Resolve_pool "${LOCAL_SOURCE}" "fast")"
    local REMOTE_SOURCE_POOL="$(Resolve_pool "${REMOTE_SOURCE}" "fast")"
    local LOCAL_TARGET_POOL="$(Resolve_pool "${LOCAL_TARGET}" "fast")"
    local REMOTE_TARGET_POOL="$(Resolve_pool "${REMOTE_TARGET}" "fast")"

    local APP_NAME
    local -a REMOTE_STOPPED_LIST LOCAL_STOPPED_LIST
    local APP_PATH_VAR APP_PATH
    local LOCATION
    local SOURCE_PATH TARGET_PATH
    local -a FOLDERS_TO_RSYNC_LIST 
    local FOLDER_TO_RSYNC
    local FULL_PATH

    echo "##########################################"
    echo "### Performing Application Replication ###"
    echo "##########################################"
    echo

    for APP_NAME in "${APP_LIST[@]}"; do
        REMOTE_STOPPED_LIST=()
        LOCAL_STOPPED_LIST=()

        APP_PATH_VAR="${APP_NAME^^}_PATH"
        APP_PATH="${!APP_PATH_VAR}"

        # Check if local and remote application datasets are available
        ! Execute_command local "test -d \"${APP_PATH/POOL_TO_INSERT/${LOCAL_SOURCE_POOL}${LOCAL_TARGET_POOL}}\"" \
            && Background_error "ERROR: '${APP_PATH/POOL_TO_INSERT/${LOCAL_SOURCE_POOL}${LOCAL_TARGET_POOL}}' does not exist. Is the dataset mounted and unlocked?"
        ! Execute_command remote "test -d \"${APP_PATH/POOL_TO_INSERT/${REMOTE_SOURCE_POOL}${REMOTE_TARGET_POOL}}\"" \
            && Background_error "ERROR: 'truenas-${REMOTE_SERVER_ID}:${APP_PATH/POOL_TO_INSERT/${REMOTE_SOURCE_POOL}${REMOTE_TARGET_POOL}}' does not exist. Is the dataset mounted and unlocked?"

        # Backup Immich Postgres DB
        [[ "${APP_NAME}" == "immich" ]] && Backup_immich_DB "$([[ -n "${LOCAL_SOURCE_POOL}" ]] && echo "local" || echo "remote")" ${LOCAL_SOURCE}${REMOTE_SOURCE}

        # Stop the application locally and remotely
        for LOCATION in "${LOCATIONS_LIST[@]}"; do
            Control_app_with_checks ${APP_NAME} stop ${LOCATION}
        done
        echo

        # Prepare the rsyncs
        if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
           [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
            SOURCE_PATH="truenas-${REMOTE_SERVER_ID}:${APP_PATH/POOL_TO_INSERT/${REMOTE_SOURCE_POOL}}"
            TARGET_PATH="${APP_PATH/POOL_TO_INSERT/${LOCAL_TARGET_POOL}}"
        elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
             [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
            SOURCE_PATH="${APP_PATH/POOL_TO_INSERT/${LOCAL_SOURCE_POOL}}"
            TARGET_PATH="truenas-${REMOTE_SERVER_ID}:${APP_PATH/POOL_TO_INSERT/${REMOTE_TARGET_POOL}}"
        fi

        eval "local -a FOLDERS_TO_RSYNC_LIST=( \"\${${APP_NAME^^}_FOLDERS_TO_RSYNC_LIST[@]}\" )"
        for FOLDER_TO_RSYNC in "${FOLDERS_TO_RSYNC_LIST[@]}"; do
            # Check if the source and target directories exist
            for FULL_PATH in "${SOURCE_PATH}" "${TARGET_PATH}"; do
                if [[ "${FULL_PATH}" == *:* ]]; then
                    Execute_command remote "test -d \"${FULL_PATH#*:}/${FOLDER_TO_RSYNC}\"" \
                        || Background_error "ERROR: '${FULL_PATH}/${FOLDER_TO_RSYNC}' does not exist. Is the dataset mounted and unlocked?"
                else
                    Execute_command local "test -d \"${FULL_PATH#*:}/${FOLDER_TO_RSYNC}\"" \
                        || Background_error "ERROR: '${FULL_PATH}/${FOLDER_TO_RSYNC}' does not exist. Is the dataset mounted and unlocked?"
                fi
            done

            # Perform the rsyncs
            echo "rsync ${TEST_MODE:+--dry-run} -e \"ssh -F ${SSH_CONFIG_FILE}\" --delete -aHX \"${SOURCE_PATH}/${FOLDER_TO_RSYNC}/\" \"${TARGET_PATH}/${FOLDER_TO_RSYNC}/\""
            if rsync ${TEST_MODE:+--dry-run} -e "ssh -F ${SSH_CONFIG_FILE}" --delete -aHX "${SOURCE_PATH}/${FOLDER_TO_RSYNC}/" "${TARGET_PATH}/${FOLDER_TO_RSYNC}/"; then
                echo "Rsync of '${FOLDER_TO_RSYNC}' completed successfully"
            else
                Background_error "ERROR: Rsync of '${FOLDER_TO_RSYNC}' failed"
            fi
            echo
        done

        # Start the application locally and remotely if they were stopped
        for LOCATION in "${LOCATIONS_LIST[@]}"; do
            Control_app_with_checks ${APP_NAME} start ${LOCATION}
        done
        echo

        # Restore Immich Postgres DB
        [[ "${APP_NAME}" == "immich" ]] && Restore_immich_DB "$([[ -n "${LOCAL_TARGET_POOL}" ]] && echo "local" || echo "remote")" ${LOCAL_TARGET}${REMOTE_TARGET}
    done

    echo "### Performing Application Replication completed ###"
    echo
}