#!/usr/bin/bash
# lib/zfs_rep.bash
# ZFS replication orchestration

function Perform_zfs_rep() {
    function l_Check_scope() {
        if [[ "${SCOPE}" == "all_snapshots" ]]; then
            ZFS_AUTOBACKUP_TASK_OPTARGS="--other-snapshots ${TASK_SCOPE} ${TARGET_PARENT_DATASET}"

            echo "###################################################"
            echo "### Performing ZFS Replication of all snapshots ###"
            echo "###################################################"
            echo "Following datasets are impacted: ${IMPACTED_DATASETS//$'\n'/ / }"
            echo

        elif [[ "${SCOPE}" == "latest_snapshot_only" ]]; then
            ZFS_AUTOBACKUP_TASK_OPTARGS=" ${TASK_SCOPE} ${TARGET_PARENT_DATASET}"

            echo "#########################################################"
            echo "### Performing ZFS Replication of the latest snapshot ###"
            echo "#########################################################"
            echo "Following datasets are impacted: ${IMPACTED_DATASETS//$'\n'/ / }"
            echo
        fi
    }

    function l_Execute_replication_and_remount() {
        local IMPACTED_DATASET
        local ZFS_AUTOBACKUP_COMMAND="autobackup-venv/bin/python -m zfs_autobackup.ZfsAutobackup"
        local ZFS_AUTOBACKUP_FOLDER="${SCRIPT_DIR}/../../zfs_autobackup"
        local EXEC_MODE

        EXEC_MODE="$([[ -n "${LOCAL_TARGET}" ]] && echo local_verbose || echo remote_verbose)"
        [[ -n "${TEST_MODE}" ]] && EXEC_MODE+="_test"

        for IMPACTED_DATASET in ${IMPACTED_DATASETS}; do
            Execute_command "${EXEC_MODE}" "zfs umount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}"
        done
        echo

        cd "${ZFS_AUTOBACKUP_FOLDER}"
        echo "${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}" 
        if ${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}; then
            echo "ZFS Replication completed successfully"
        else
            Background_error "ERROR: ZFS Replication failed"
        fi
        echo
        cd - >/dev/null

        for IMPACTED_DATASET in ${IMPACTED_DATASETS}; do
            Execute_command "${EXEC_MODE}" "zfs mount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}"
        done
    }

    local SCOPE="$1"
    local TASK_SCOPE="${TASK}_${SCOPE}"

    local SNAPSHOT_OPTARGS="--rollback --keep-source=0 --keep-target=0 --allow-empty --snapshot-format {}-%Y-%m-%d_%H-%M"
    local ZFS_OPTARGS="--zfs-compressed --decrypt --clear-refreservation"
    local ZFS_AUTOBACKUP_OPTARGS="--strip-path 2 --exclude-received"
    local SSH_OPTARGS TARGET_PARENT_DATASET IMPACTED_DATASETS ZFS_AUTOBACKUP_TASK_OPTARGS

    [[ "$(Execute_command local "zfs list -H -o mounted ${LOCAL_SERVER_ID}-pool/encrypted-ds")" == "no" ]]   && Background_error "ERROR: ${LOCAL_SERVER_ID}-pool/encrypted-ds on truenas-${LOCAL_SERVER_ID} is not mounted (and/or unlocked)."
    [[ "$(Execute_command remote "zfs list -H -o mounted ${REMOTE_SERVER_ID}-pool/encrypted-ds")" == "no" ]] && Background_error "ERROR: ${REMOTE_SERVER_ID}-pool/encrypted-ds on truenas-${REMOTE_SERVER_ID} is not mounted (and/or unlocked)."

    if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-source truenas-${REMOTE_SOURCE}"
        TARGET_PARENT_DATASET="${LOCAL_TARGET}-pool/encrypted-ds"
        IMPACTED_DATASETS="$(Execute_command $([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote) "zfs list -H | awk '{print \$1}' | xargs zfs get -o name,property all | grep \" autobackup:${TASK_SCOPE}\" | awk '{print \$1}' | xargs basename -a 2>/dev/null")"

        l_Check_scope
        l_Execute_replication_and_remount
    elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-target truenas-${REMOTE_TARGET}"
        TARGET_PARENT_DATASET="${REMOTE_TARGET}-pool/encrypted-ds"
        IMPACTED_DATASETS="$(Execute_command $([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote) "zfs list -H | awk '{print \$1}' | xargs zfs get -o name,property all | grep \" autobackup:${TASK_SCOPE}\" | awk '{print \$1}' | xargs basename -a 2>/dev/null")"

        l_Check_scope
        l_Execute_replication_and_remount
    fi

    echo
    echo "### Performing ZFS Replication completed ###"
    echo
}