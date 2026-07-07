#!/usr/bin/bash
# lib/snap_rollup.bash
# ZFS snapshot rollup orchestration

function Perform_snapshot_rollup() {
    local EXEC_MODE
    local TARGET_POOL
    local TARGET_APP_POOL
    local ROLLUP_CMD

    echo "######################################"
    echo "### Performing ZFS snapshot rollup ###"
    echo "######################################"
    echo

    TARGET_POOL="$(Resolve_pool "${LOCAL_TARGET}${REMOTE_TARGET}")"
    [[ -n "${TARGET_POOL}" ]] || Background_error "ERROR: Failed to resolve rollup target pool."
    TARGET_APP_POOL="$(Resolve_pool "${LOCAL_TARGET}${REMOTE_TARGET}" fast)"
    [[ -n "${TARGET_APP_POOL}" ]] || Background_error "ERROR: Failed to resolve rollup target app pool."
    ROLLUP_CMD="/mnt/${TARGET_APP_POOL}/encrypted-ds/app-ds/zfs-rollup/rollup.py -v${TEST_MODE:+ -t} --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 ${TARGET_POOL}/encrypted-ds/media-ds"

    if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        EXEC_MODE="local_verbose"
    elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        EXEC_MODE="remote_verbose"
    fi

    if Execute_command "${EXEC_MODE}" "${ROLLUP_CMD}"; then
        echo "${EXEC_MODE%%_*} snapshot rollup completed successfully"
    else
        Background_error "ERROR: ${EXEC_MODE%%_*} snapshot rollup failed"
    fi

    echo
    echo "### Performing ZFS snapshot rollup completed ###"
    echo
}