#!/usr/bin/bash
# lib/rep_apps.bash
# TrueNAS Application Replication Orchestration

function Control_app() {
    local FULL_APP_NAME="$1"
    local ACTION="$2"
    local LOCATION="$3"

    local JOBID
    local JOB_STATE
    local TIMEOUT_COUNTER="0"
    local MAX_TIMEOUT=60

    if JOBID="$(Execute_command "${LOCATION}" "midclt call app.${ACTION} \"${FULL_APP_NAME}\"")" && [[ -n "${JOBID}" ]]; then
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
        echo " ${ACTION^} was successful."
    else
        Background_error "ERROR: Failed to ${ACTION} the ${LOCATION} ${FULL_APP_NAME}"
    fi
}

# Stop an app only if it is active — safe as a cleanup undo whatever state an abort left it in.
# Usage: Stop_app_if_active <FULL_APP_NAME> <LOCATION>
function Stop_app_if_active() {
    local FULL_APP_NAME="$1"
    local LOCATION="$2"
    local APP_STATE

    APP_STATE="$(Execute_command "${LOCATION}" \
        "midclt call app.query | jq -r '.[] | select(.name==\"${FULL_APP_NAME}\") | .state'")"
    if [[ "${APP_STATE}" =~ ^(RUNNING|DEPLOYING|CRASHED)$ ]]; then
        echo -n "Stopping ${LOCATION} ${FULL_APP_NAME} again, as it was stopped before this run."
        Control_app "${FULL_APP_NAME}" stop "${LOCATION}"
    fi
}

# Start an app only if it is stopped — the mirror of Stop_app_if_active, for --stop-running-apps.
# Usage: Start_app_if_stopped <FULL_APP_NAME> <LOCATION>
function Start_app_if_stopped() {
    local FULL_APP_NAME="$1"
    local LOCATION="$2"
    local APP_STATE

    APP_STATE="$(Execute_command "${LOCATION}" \
        "midclt call app.query | jq -r '.[] | select(.name==\"${FULL_APP_NAME}\") | .state'")"
    if [[ "${APP_STATE}" =~ ^(STOPPED|CRASHED)$ ]]; then
        echo -n "Starting ${LOCATION} ${FULL_APP_NAME} again, as it was running before this run."
        Control_app "${FULL_APP_NAME}" start "${LOCATION}"
    fi
}

# Explain a source/target app version mismatch and how to fix it in the GUI.
# Usage: Print_app_version_mismatch <APP_NAME> <WHAT> <SOURCE_VERSION> <TARGET_VERSION>
function Print_app_version_mismatch() {
    local APP_NAME="$1"
    local WHAT="$2"
    local SOURCE_VERSION="$3"
    local TARGET_VERSION="$4"
    local OLDER_SERVER_ID="${TARGET_SERVER_ID}"

    [[ "$(printf '%s\n' "${SOURCE_VERSION}" "${TARGET_VERSION}" | sort -V | head -n1)" == "${SOURCE_VERSION}" ]] \
        && OLDER_SERVER_ID="${SOURCE_SERVER_ID}"

    echo "Replication skipped for app ${APP_NAME}: ${WHAT} differs — ${SOURCE_SERVER_ID}: ${SOURCE_VERSION}, ${TARGET_SERVER_ID}: ${TARGET_VERSION}."
    echo "  Fix on TrueNAS-${OLDER_SERVER_ID^}: Apps → Discover Apps → Refresh Catalog, then Apps → Installed → ${APP_NAME}-${OLDER_SERVER_ID} → Update"
    echo "  (reload the page if it still says 'Up to date'; if no update is offered at all, that host's TrueNAS release may be too old for the newer version)."
    echo
}

function Control_app_with_checks() {
    local APP_NAME="$1"
    local ACTION="$2"
    shift 2
    local LOCATIONS=( "$@" )

    local LOCATION
    local SERVER_ID_VAR
    local FULL_APP_NAME
    local APP_STATE
    local PERFORM_ACTION=0

    for LOCATION in "${LOCATIONS[@]}"; do
        # nameref onto the caller's LOCAL_/REMOTE_STOPPED_LIST (no eval, no copy).
        # unset first: 'local -n' can't rebind an existing nameref in some bash.
        unset -n STOPPED_LIST_REF
        local -n STOPPED_LIST_REF="${LOCATION^^}_STOPPED_LIST"
        SERVER_ID_VAR="${LOCATION^^}_SERVER_ID"
        FULL_APP_NAME="${APP_NAME}-${!SERVER_ID_VAR}"

        APP_STATE="$(Execute_command "${LOCATION}" \
            "midclt call app.query | jq -r '.[] | select(.name==\"${FULL_APP_NAME}\") | .state'")"

        PERFORM_ACTION=0
        case "${ACTION}" in
          start)
            # shellcheck disable=SC2076  # elif below is a deliberate literal-substring match, not regex (APP_NAME may contain regex metacharacters)
            if [[ ! "${APP_STATE}" =~ ^(STOPPED|CRASHED)$ ]]; then
                echo "WARNING: ${FULL_APP_NAME} cannot be started because its state is not 'STOPPED' or 'CRASHED'. It is ${APP_STATE}."
            elif [[ " ${STOPPED_LIST_REF[*]} " =~ " ${APP_NAME} " ]]; then
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
            [[ "${ACTION}" == "stop" ]] && STOPPED_LIST_REF+=( "${APP_NAME}" )
        fi
    done
    unset -n STOPPED_LIST_REF
}

function Perform_app_replication() {
    local SOURCE_POOL
    local TARGET_POOL

    local -a SELECTED_APP_JSON_LIST
    local -a APP_JSON_LIST
    local SOURCE_APP_STATE
    local TARGET_APP_STATE
    local SOURCE_APP_VERSION
    local TARGET_APP_VERSION
    local SOURCE_STARTED_BY_US
    local TARGET_STARTED_BY_US
    local PRECONDITION_MET
    local SIDE
    local STATE_VAR
    local SERVER_ID_VAR
    local SUCCEEDED=0
    local SKIPPED=0
    local APP_NAME
    local JSON_APP_NAME
    local APP_JSON

    local -a REMOTE_STOPPED_LIST 
    local -a LOCAL_STOPPED_LIST
    local FULL_SOURCE_PATH
    local FULL_TARGET_PATH
    local -a APP_DIRS_TO_RSYNC_LIST
    local APP_DIR_TO_RSYNC

    # Resolve pools from the direction globals (apps live on the fast pool).
    SOURCE_POOL="$(Resolve_pool "${SOURCE_SERVER_ID}" "fast")"
    TARGET_POOL="$(Resolve_pool "${TARGET_SERVER_ID}" "fast")"
    [[ -n "${SOURCE_POOL}" && -n "${TARGET_POOL}" ]] || Background_error "ERROR: Failed to resolve source/target pool (source='${SOURCE_POOL}', target='${TARGET_POOL}')."

    echo "##########################################"
    echo "### Performing Application Replication ###"
    echo "##########################################"
    echo

    mapfile -t APP_JSON_LIST < <(jq -c '.apps[]' "${SCRIPT_DIR}/../config/apps.json")
    if [[ ${#APP_LIST[@]} -eq 0 ]]; then
        # No --app specified → select all
        SELECTED_APP_JSON_LIST=("${APP_JSON_LIST[@]}")
    else
        # Filter only requested apps (unknown names already rejected at parse time)
        for APP_NAME in "${APP_LIST[@]}"; do
            for APP_JSON in "${APP_JSON_LIST[@]}"; do
                JSON_APP_NAME="$(jq -r '.name' <<< "${APP_JSON}")"
                if [[ "${JSON_APP_NAME}" == "${APP_NAME}" ]]; then
                    SELECTED_APP_JSON_LIST+=("${APP_JSON}")
                    break
                fi
            done
        done
    fi

    for APP_JSON in "${SELECTED_APP_JSON_LIST[@]}"; do
        REMOTE_STOPPED_LIST=()
        LOCAL_STOPPED_LIST=()
        SOURCE_STARTED_BY_US="false"
        TARGET_STARTED_BY_US="false"

        APP_NAME="$(jq -r '.name' <<< "${APP_JSON}")"
        mapfile -t APP_DIRS_TO_RSYNC_LIST < <(jq -r '.app_dir_list // [] | .[]' <<< "${APP_JSON}")

        # A definitions-only entry (just a name) has no data to replicate
        if [[ ${#APP_DIRS_TO_RSYNC_LIST[@]} -eq 0 ]]; then
            echo "Replication skipped for app ${APP_NAME}: no data folders configured in apps.json — nothing to replicate."
            echo
            ((SKIPPED++))
            continue
        fi

        # Prepare the rsyncs
        FULL_SOURCE_PATH="$([[ "${SOURCE_LOCATION}" == "remote" ]] && echo "truenas-${SOURCE_SERVER_ID}:")/mnt/${SOURCE_POOL}/encrypted-ds/app-ds/$(jq -r '.app_base_path' <<< "${APP_JSON}")"
        FULL_TARGET_PATH="$([[ "${TARGET_LOCATION}" == "remote" ]] && echo "truenas-${TARGET_SERVER_ID}:")/mnt/${TARGET_POOL}/encrypted-ds/app-ds/$(jq -r '.app_base_path' <<< "${APP_JSON}")"

        # Per-app preconditions skip the app (nothing changed yet), they don't abort the run.
        # Data from one app version can break another (e.g. a newer Immich DB dump restored into an
        # older Immich), so the upstream app versions must match; a packaging-only bump may differ.
        SOURCE_APP_VERSION="$(Execute_command "${SOURCE_LOCATION}" \
            "midclt call app.query | jq -r '.[] | select(.name==\"${APP_NAME}-${SOURCE_SERVER_ID}\") | .metadata.app_version'")"
        TARGET_APP_VERSION="$(Execute_command "${TARGET_LOCATION}" \
            "midclt call app.query | jq -r '.[] | select(.name==\"${APP_NAME}-${TARGET_SERVER_ID}\") | .metadata.app_version'")"
        if [[ -z "${SOURCE_APP_VERSION}" || -z "${TARGET_APP_VERSION}" ]]; then
            [[ -z "${SOURCE_APP_VERSION}" ]] \
                && echo "Replication skipped for app ${APP_NAME}: ${APP_NAME}-${SOURCE_SERVER_ID} is not installed on TrueNAS-${SOURCE_SERVER_ID^}."
            [[ -z "${TARGET_APP_VERSION}" ]] \
                && echo "Replication skipped for app ${APP_NAME}: ${APP_NAME}-${TARGET_SERVER_ID} is not installed on TrueNAS-${TARGET_SERVER_ID^} (app_definition_replication can create it)."
            echo
            ((SKIPPED++))
            continue
        fi
        if [[ "${SOURCE_APP_VERSION}" != "${TARGET_APP_VERSION}" ]]; then
            Print_app_version_mismatch "${APP_NAME}" "app version" "${SOURCE_APP_VERSION}" "${TARGET_APP_VERSION}"
            ((SKIPPED++))
            continue
        fi

        # Check if local and remote application datasets are available
        Execute_command "${SOURCE_LOCATION}" "test -d \"${FULL_SOURCE_PATH#*:}\"" \
            || Background_error "ERROR: '${FULL_SOURCE_PATH}' does not exist. Is the dataset mounted and unlocked?"
        Execute_command "${TARGET_LOCATION}" "test -d \"${FULL_TARGET_PATH#*:}\"" \
            || Background_error "ERROR: '${FULL_TARGET_PATH}' does not exist. Is the dataset mounted and unlocked?"

        # Hooks that run inside the app (Immich: the DB dump on the source, the restore on the
        # target) need it RUNNING on that side. Both sides are checked before either is started,
        # so a skip leaves nothing changed.
        SOURCE_APP_STATE="RUNNING"
        TARGET_APP_STATE="RUNNING"
        [[ "$(jq -r '.requires_source_running // false' <<< "${APP_JSON}")" == "true" ]] \
            && SOURCE_APP_STATE="$(Execute_command "${SOURCE_LOCATION}" \
                "midclt call app.query | jq -r '.[] | select(.name==\"${APP_NAME}-${SOURCE_SERVER_ID}\") | .state'")"
        [[ "$(jq -r '.requires_target_running // false' <<< "${APP_JSON}")" == "true" ]] \
            && TARGET_APP_STATE="$(Execute_command "${TARGET_LOCATION}" \
                "midclt call app.query | jq -r '.[] | select(.name==\"${APP_NAME}-${TARGET_SERVER_ID}\") | .state'")"
        PRECONDITION_MET="true"
        for SIDE in SOURCE TARGET; do
            STATE_VAR="${SIDE}_APP_STATE"
            SERVER_ID_VAR="${SIDE}_SERVER_ID"
            [[ "${!STATE_VAR}" == "RUNNING" ]] && continue
            [[ "${!STATE_VAR}" =~ ^(STOPPED|CRASHED)$ && -n "${START_STOPPED_APPS}" ]] && continue
            echo "Replication skipped for app ${APP_NAME}: ${APP_NAME}-${!SERVER_ID_VAR} must be RUNNING on the ${SIDE,,} (a hook runs inside it). Current state: ${!STATE_VAR}."
            [[ "${!STATE_VAR}" =~ ^(STOPPED|CRASHED)$ ]] \
                && echo "  Pass --start-stopped-apps to start it for the run and stop it again afterwards."
            PRECONDITION_MET="false"
        done
        if [[ "${PRECONDITION_MET}" == "false" ]]; then
            echo
            ((SKIPPED++))
            continue
        fi
        # Register each undo BEFORE starting, so even a timed-out start still gets stopped again.
        if [[ "${SOURCE_APP_STATE}" =~ ^(STOPPED|CRASHED)$ ]]; then
            # shellcheck disable=SC2016  # single-quoted on purpose: expand at cleanup (fire) time
            Register_cleanup 'Stop_app_if_active "${APP_NAME}-${SOURCE_SERVER_ID}" "${SOURCE_LOCATION}"'
            SOURCE_STARTED_BY_US="true"
            echo -n "Starting ${SOURCE_LOCATION} ${APP_NAME}-${SOURCE_SERVER_ID} for this run (state ${SOURCE_APP_STATE}; --start-stopped-apps)."
            Control_app "${APP_NAME}-${SOURCE_SERVER_ID}" start "${SOURCE_LOCATION}"
            echo
        fi
        if [[ "${TARGET_APP_STATE}" =~ ^(STOPPED|CRASHED)$ ]]; then
            # shellcheck disable=SC2016  # single-quoted on purpose: expand at cleanup (fire) time
            Register_cleanup 'Stop_app_if_active "${APP_NAME}-${TARGET_SERVER_ID}" "${TARGET_LOCATION}"'
            TARGET_STARTED_BY_US="true"
            echo -n "Starting ${TARGET_LOCATION} ${APP_NAME}-${TARGET_SERVER_ID} for this run (state ${TARGET_APP_STATE}; --start-stopped-apps)."
            Control_app "${APP_NAME}-${TARGET_SERVER_ID}" start "${TARGET_LOCATION}"
            echo
        fi

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

        # Return apps --start-stopped-apps started to their previous (stopped) state.
        # Target first: Unregister_cleanup pops the newest entry.
        if [[ "${TARGET_STARTED_BY_US}" == "true" ]]; then
            Stop_app_if_active "${APP_NAME}-${TARGET_SERVER_ID}" "${TARGET_LOCATION}"
            Unregister_cleanup
            echo
        fi
        if [[ "${SOURCE_STARTED_BY_US}" == "true" ]]; then
            Stop_app_if_active "${APP_NAME}-${SOURCE_SERVER_ID}" "${SOURCE_LOCATION}"
            Unregister_cleanup
            echo
        fi
        ((SUCCEEDED++))
    done

    echo "### Application Replication Summary: ${SUCCEEDED} replicated, ${SKIPPED} skipped ###"
    echo
}