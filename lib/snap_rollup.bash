#!/usr/bin/bash
# lib/rollup.bash
# ZFS snapshot rollup orchestration

function Perform_snapshot_rollup() {
    local EXEC_MODE

    echo "######################################"
    echo "### Performing ZFS snapshot rollup ###"
    echo "######################################"
    echo

    ROLLUP_CMD="${SCRIPT_DIR/${LOCAL_SOURCE}${REMOTE_SOURCE}/${LOCAL_TARGET}${REMOTE_TARGET}}/../../zfs-rollup/rollup.py -v --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 $(Resolve_pool "${LOCAL_TARGET}${REMOTE_TARGET}")/encrypted-ds/media-ds"
    
    if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        EXEC_MODE="local_verbose"
    elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        EXEC_MODE="remote_verbose"
    fi
    
    [[ -n "${TEST_MODE}" ]] && EXEC_MODE+="_test"
    if Execute_command "${EXEC_MODE}" "${ROLLUP_CMD}"; then
        echo "${EXEC_MODE%%_*} snapshot rollup completed successfully"
    else
        Background_error "ERROR: ${EXEC_MODE%%_*} snapshot rollup failed"
    fi

    echo
    echo "### Performing ZFS snapshot rollup completed ###"
    echo
}