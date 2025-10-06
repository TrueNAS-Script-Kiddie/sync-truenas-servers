#!/usr/bin/bash
# lib/rsync.bash
# Rsync orchestration and Immich DB backup/restore

function Perform_rsync() {
    local -a LOCATIONS_LIST=( "local" "remote" )
    local -a PLEX_FOLDERS_TO_RSYNC_LIST=( "Media" "Metadata" "Plug-ins" "Plug-in Support" )
    local -a IMMICH_FOLDERS_TO_RSYNC_LIST=( "backups" "encoded-video" "library" "profile" "thumbs" "upload" )
    local PLEX_PATH="/mnt/LOCATION_TO_INSERT-pool/encrypted-ds/app-ds/plex-ds/Library/Application Support/Plex Media Server"
    local IMMICH_PATH="/mnt/LOCATION_TO_INSERT-pool/encrypted-ds/app-ds/immich-ds/immich-data-ds"
    local LOCAL_SOURCE_FOR_APPPOOL="${LOCAL_SOURCE/master/ssdmaster}"
    local REMOTE_SOURCE_FOR_APPPOOL="${REMOTE_SOURCE/master/ssdmaster}"
    local LOCAL_TARGET_FOR_APPPOOL="${LOCAL_TARGET/master/ssdmaster}"
    local REMOTE_TARGET_FOR_APPPOOL="${REMOTE_TARGET/master/ssdmaster}"

    local APP_NAME
    local -a REMOTE_STOPPED_LIST LOCAL_STOPPED_LIST
    local APP_PATH_VAR APP_PATH
    local LOCATION
    local SOURCE_PATH TARGET_PATH
    local -a FOLDERS_TO_RSYNC_LIST 
    local FOLDER_TO_RSYNC
    local FULL_PATH

    echo "########################"
    echo "### Performing rsync ###"
    echo "########################"
    echo

    for APP_NAME in "${APPS_LIST[@]}"; do
        REMOTE_STOPPED_LIST=()
        LOCAL_STOPPED_LIST=()

        APP_PATH_VAR="${APP_NAME^^}_PATH"
        APP_PATH="${!APP_PATH_VAR}"

        # Check if local and remote application datasets are available
        ! Execute_command local "test -d \"${APP_PATH/LOCATION_TO_INSERT/${LOCAL_SOURCE_FOR_APPPOOL}${LOCAL_TARGET_FOR_APPPOOL}}\""    \
        && Background_error "ERROR: '${APP_PATH/LOCATION_TO_INSERT/${LOCAL_SOURCE_FOR_APPPOOL}${LOCAL_TARGET_FOR_APPPOOL}}' does not exist. Is the dataset mounted and unlocked?"
        ! Execute_command remote "test -d \"${APP_PATH/LOCATION_TO_INSERT/${REMOTE_SOURCE_FOR_APPPOOL}${REMOTE_TARGET_FOR_APPPOOL}}\"" \
        && Background_error "ERROR: 'truenas-${REMOTE_SERVER_ID}:${APP_PATH/LOCATION_TO_INSERT/${REMOTE_SOURCE_FOR_APPPOOL}${REMOTE_TARGET_FOR_APPPOOL}}' does not exist. Is the dataset mounted and unlocked?"

        # Backup Immich Postgres DB
        [[ "${APP_NAME}" == "immich" ]] && Backup_immich_DB "$([[ -n "${LOCAL_SOURCE_FOR_APPPOOL}" ]] && echo "local" || echo "remote")" ${LOCAL_SOURCE}${REMOTE_SOURCE}

        # Stop the application locally and remotely
        for LOCATION in "${LOCATIONS_LIST[@]}"; do
            Control_app_with_checks ${APP_NAME} stop ${LOCATION}
        done
        echo
        
        # Prepare the rsyncs
        if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
           [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
            SOURCE_PATH="truenas-${REMOTE_SERVER_ID}:${APP_PATH/LOCATION_TO_INSERT/${REMOTE_SOURCE_FOR_APPPOOL}}"
            TARGET_PATH="${APP_PATH/LOCATION_TO_INSERT/${LOCAL_TARGET_FOR_APPPOOL}}"
        elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
            [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
            SOURCE_PATH="${APP_PATH/LOCATION_TO_INSERT/${LOCAL_SOURCE_FOR_APPPOOL}}"
            TARGET_PATH="truenas-${REMOTE_SERVER_ID}:${APP_PATH/LOCATION_TO_INSERT/${REMOTE_TARGET_FOR_APPPOOL}}"
        fi

        eval "local -a FOLDERS_TO_RSYNC_LIST=( \"\${${APP_NAME^^}_FOLDERS_TO_RSYNC_LIST[@]}\" )"
        for FOLDER_TO_RSYNC in "${FOLDERS_TO_RSYNC_LIST[@]}"; do
        # Check if the source and target directories exist
        for FULL_PATH in "${SOURCE_PATH}" "${TARGET_PATH}"; do
            if [[ "${FULL_PATH}" == *:* ]]; then
            Execute_command remote "test -d \"${FULL_PATH#*:}/${FOLDER_TO_RSYNC}\"" || Background_error "ERROR: '${FULL_PATH}/${FOLDER_TO_RSYNC}' does not exist. Is the dataset mounted and unlocked?"
            else
            Execute_command local "test -d \"${FULL_PATH#*:}/${FOLDER_TO_RSYNC}\"" || Background_error "ERROR: '${FULL_PATH}/${FOLDER_TO_RSYNC}' does not exist. Is the dataset mounted and unlocked?"
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
        [[ "${APP_NAME}" == "immich" ]] && Restore_immich_DB "$([[ -n "${LOCAL_TARGET_FOR_APPPOOL}" ]] && echo "local" || echo "remote")" ${LOCAL_TARGET}${REMOTE_TARGET}
    done

    echo "### Performing rsync completed ###"
    echo
}