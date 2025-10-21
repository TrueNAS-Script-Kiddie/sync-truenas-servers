#!/usr/bin/bash
# lib/zfs_rep.bash
# ZFS replication orchestration

function Perform_zfs_rep() {
    function l_Print_scope() {
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

        elif [[ "${SCOPE}" == "vm_latest_snapshot_only" ]]; then
            ZFS_AUTOBACKUP_TASK_OPTARGS=" ${TASK_SCOPE} ${TARGET_PARENT_DATASET}"

            echo "Performing ZFS Replication of the latest snapshot for the VM '${VM}'"
            echo "  Following datasets/zvols are impacted: ${IMPACTED_DATASETS//$'\n'/ / }"
            echo
        fi
    }

    function l_Execute_replication_and_remount() {
        local IMPACTED_DATASET
        local ZFS_AUTOBACKUP_COMMAND="autobackup-venv/bin/python -m zfs_autobackup.ZfsAutobackup"
        local ZFS_AUTOBACKUP_FOLDER="${SCRIPT_DIR}/../../zfs_autobackup"
        local EXEC_MODE
        local UMOUNT_DONE="false"

        EXEC_MODE="$([[ -n "${LOCAL_TARGET}" ]] && echo local || echo remote)"
        [[ -n "${TEST_MODE}" ]] && EXEC_MODE+="_test"

        for IMPACTED_DATASET in ${IMPACTED_DATASETS}; do
            if Execute_command "${EXEC_MODE}" "zfs get -H -o value type,mounted '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}' | grep -q '^filesystem[[:space:]]*yes$'"; then
                echo "  Executing $([[ -n "${LOCAL_TARGET}" ]] && echo locally || echo remotely): zfs umount '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"
                Execute_command "${EXEC_MODE}" "zfs umount '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"
                UMOUNT_DONE="true"
            fi
        done
        [[ "${UMOUNT_DONE}" == "true" ]] && echo

        cd "${ZFS_AUTOBACKUP_FOLDER}" || Background_error "ERROR: Cannot cd into ${ZFS_AUTOBACKUP_FOLDER}"
        echo "  ${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}" 
        if ${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}; then
            echo "  ZFS Replication completed successfully"
        else
            Background_error "ERROR: ZFS Replication failed"
        fi
        echo
        cd - >/dev/null

        for IMPACTED_DATASET in ${IMPACTED_DATASETS}; do
            if Execute_command "${EXEC_MODE}" "zfs get -H -o value type,mounted '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}' | grep -q '^filesystem[[:space:]]*no$'"; then
                echo "  Executing $([[ -n "${LOCAL_TARGET}" ]] && echo locally || echo remotely): zfs mount '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"
                Execute_command "${EXEC_MODE}" "zfs mount '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"
            fi
        done
    }

    local SCOPE="$1"
    local POOL_TYPE="$2"

    local TASK_SCOPE="${TASK}_${SCOPE}"

    local SNAPSHOT_OPTARGS="--rollback --keep-source=0 --keep-target=0 --allow-empty --snapshot-format {}-%Y-%m-%d_%H-%M"
    local ZFS_OPTARGS="--zfs-compressed --decrypt --clear-refreservation"
    local ZFS_AUTOBACKUP_OPTARGS="--strip-path 2 --exclude-received"
    local SSH_OPTARGS TARGET_PARENT_DATASET IMPACTED_DATASETS ZFS_AUTOBACKUP_TASK_OPTARGS

    [[ "$(Execute_command local "zfs list -H -o mounted $(Resolve_pool "${LOCAL_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds")" == "no" ]]   && Background_error "ERROR: $(Resolve_pool "${LOCAL_SERVER_ID}")/encrypted-ds on truenas-${LOCAL_SERVER_ID} is not mounted (and/or unlocked)."
    [[ "$(Execute_command remote "zfs list -H -o mounted $(Resolve_pool "${REMOTE_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds")" == "no" ]] && Background_error "ERROR: $(Resolve_pool "${REMOTE_SERVER_ID}")/encrypted-ds on truenas-${REMOTE_SERVER_ID} is not mounted (and/or unlocked)."

    if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-source truenas-${REMOTE_SOURCE}"
        [[ "${SCOPE}" == "vm_latest_snapshot_only" ]] && TARGET_PARENT_DATASET="$(Resolve_pool "${LOCAL_TARGET}" "${POOL_TYPE}")/encrypted-ds/vm-ds" || TARGET_PARENT_DATASET="$(Resolve_pool "${LOCAL_TARGET}" "${POOL_TYPE}")/encrypted-ds"
        IMPACTED_DATASETS="$(Execute_command $([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote) "zfs list -H | awk '{print \$1}' | xargs zfs get -o name,property all | grep \" autobackup:${TASK_SCOPE}\" | awk '{print \$1}' | xargs basename -a 2>/dev/null")"

        l_Print_scope
        l_Execute_replication_and_remount
    elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-target truenas-${REMOTE_TARGET}"
        [[ "${SCOPE}" == "vm_latest_snapshot_only" ]] && TARGET_PARENT_DATASET="$(Resolve_pool "${REMOTE_TARGET}" "${POOL_TYPE}")/encrypted-ds/vm-ds" || TARGET_PARENT_DATASET="$(Resolve_pool "${REMOTE_TARGET}" "${POOL_TYPE}")/encrypted-ds"
        IMPACTED_DATASETS="$(Execute_command $([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote) "zfs list -H | awk '{print \$1}' | xargs zfs get -o name,property all | grep \" autobackup:${TASK_SCOPE}\" | awk '{print \$1}' | xargs basename -a 2>/dev/null")"

        l_Print_scope
        l_Execute_replication_and_remount
    fi

    if [[ "${SCOPE}" != "vm_latest_snapshot_only" ]]; then
        echo
        echo "### Performing ZFS Replication completed ###"
        echo
    fi
}