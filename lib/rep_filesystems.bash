#!/usr/bin/bash
# lib/rep_filesystems.bash
# TrueNAS Filesystem Replication Orchestration

function Perform_filesystem_replication() {
    function l_Print_scope() {
        if [[ "${SCOPE}" == "all_snapshots" ]]; then
            ZFS_AUTOBACKUP_TASK_OPTARGS="--other-snapshots ${TASK_SCOPE} ${TARGET_PARENT_DATASET}"

            echo "###################################################"
            echo "### Performing ZFS Replication of all snapshots ###"
            echo "###################################################"
            ( IFS=", "; echo "Following filesystems are impacted: ${IMPACTED_DATASETS[*]}" )
            echo

        elif [[ "${SCOPE}" == "latest_snapshot_only" ]]; then
            ZFS_AUTOBACKUP_TASK_OPTARGS=" ${TASK_SCOPE} ${TARGET_PARENT_DATASET}"

            echo "#########################################################"
            echo "### Performing ZFS Replication of the latest snapshot ###"
            echo "#########################################################"
            ( IFS=", "; echo "Following filesystems are impacted: ${IMPACTED_DATASETS[*]}" )
            echo

        elif [[ "${SCOPE}" == "vm_latest_snapshot_only" ]]; then
            ZFS_AUTOBACKUP_TASK_OPTARGS=" ${TASK_SCOPE} ${TARGET_PARENT_DATASET}"

            echo "Performing ZFS Replication of the latest snapshot for the VM '${VM}'${TEST_MODE:+" (Not done because of '--test' usage!)"}"
            ( IFS=", "; echo "  Following zvols are impacted: ${IMPACTED_DATASETS[*]}" )
            echo
        fi
    }

    function l_Execute_replication_and_remount() {
        local IMPACTED_DATASET
        local ZFS_AUTOBACKUP_COMMAND="autobackup-venv/bin/python -m zfs_autobackup.ZfsAutobackup"
        local ZFS_AUTOBACKUP_PATH="${SCRIPT_DIR}/../../zfs_autobackup"
        local EXEC_MODE
        local -a UNMOUNTED_LIST=()

        # subfunction to (un)mount impacted datasets with state verification
        function l_Toggle_mounts() {
            local ACTION="$1"             # umount or mount
            local -a LIST=( "${@:2}" )    # filesystems to process

            local EXPECTED
            local CHANGED="false"
            local TARGET_SERVER_ID="$([[ -n "${LOCAL_SOURCE}" ]] && echo "${REMOTE_SERVER_ID}" || echo "${LOCAL_SERVER_ID}")"

            case "${ACTION}" in
                "mount")  EXPECTED="no" ;;
                "umount") EXPECTED="yes" ;;
            esac

            for IMPACTED_DATASET in "${LIST[@]}"; do
                if Execute_command "${EXEC_MODE}" \
                    "zfs get -H -o value type,mounted '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}' 2>/dev/null \
                     | paste -sd' ' - \
                     | grep -q '^filesystem[[:space:]]*${EXPECTED}\$'"; then
                    echo "  truenas-${TARGET_SERVER_ID} - zfs ${ACTION} '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"
                    Execute_command "${EXEC_MODE}" "zfs ${ACTION} '${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}'"
                    CHANGED="true"
                    [[ "${ACTION}" == "umount" ]] && UNMOUNTED_LIST+=( "${IMPACTED_DATASET}" )
                fi
            done
            [[ "${CHANGED}" == "true" ]] && echo
        }

        EXEC_MODE="$([[ -n "${LOCAL_TARGET}" ]] && echo local || echo remote)"
        [[ -n "${TEST_MODE}" ]] && EXEC_MODE+="_test"

        l_Toggle_mounts "umount" "${IMPACTED_DATASETS[@]}"

        # --- Run zfs_autobackup ---
        cd "${ZFS_AUTOBACKUP_PATH}" || Background_error "ERROR: Cannot cd into ${ZFS_AUTOBACKUP_PATH}"
        echo "  ${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}"
        # shellcheck disable=SC2086  # deliberately unquoted: each *_OPTARGS var holds multiple space-separated CLI flags that must word-split into separate arguments
        if ${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}; then
            echo "  ZFS Replication completed successfully"
        else
            Background_error "ERROR: ZFS Replication failed"
        fi
        echo
        cd - >/dev/null || Background_error "ERROR: Failed to cd back after zfs_autobackup run."

        l_Toggle_mounts "mount" "${UNMOUNTED_LIST[@]}"
    }

    local SCOPE="$1"
    local POOL_TYPE="$2"

    local TASK_SCOPE="${TASK}_${SCOPE}"

    local SNAPSHOT_OPTARGS="--rollback --keep-source=0 --keep-target=0 --allow-empty --snapshot-format {}-%Y-%m-%d_%H-%M"
    local ZFS_OPTARGS="--zfs-compressed --decrypt --clear-refreservation"
    local ZFS_AUTOBACKUP_OPTARGS
    local SSH_OPTARGS
    local TARGET_PARENT_DATASET
    local IMPACTED_DATASETS
    local ZFS_AUTOBACKUP_TASK_OPTARGS

    ZFS_AUTOBACKUP_OPTARGS="--strip-path 2 --exclude-received"

    [[ "$(Execute_command local "zfs list -H -o mounted $(Resolve_pool "${LOCAL_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds")" == "yes" ]]   || Background_error "ERROR: $(Resolve_pool "${LOCAL_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds on truenas-${LOCAL_SERVER_ID} is not mounted (and/or unlocked)."
    [[ "$(Execute_command remote "zfs list -H -o mounted $(Resolve_pool "${REMOTE_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds")" == "yes" ]] || Background_error "ERROR: $(Resolve_pool "${REMOTE_SERVER_ID}" "${POOL_TYPE}")/encrypted-ds on truenas-${REMOTE_SERVER_ID} is not mounted (and/or unlocked)."

    if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-source truenas-${REMOTE_SOURCE}"
        TARGET_PARENT_DATASET="$(Resolve_pool "${LOCAL_TARGET}" "${POOL_TYPE}")/encrypted-ds"
    elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-target truenas-${REMOTE_TARGET}"
        TARGET_PARENT_DATASET="$(Resolve_pool "${REMOTE_TARGET}" "${POOL_TYPE}")/encrypted-ds"
    fi

    [[ "${TARGET_PARENT_DATASET}" != "/encrypted-ds" ]] || Background_error "ERROR: Failed to resolve target pool for TARGET_PARENT_DATASET."

    mapfile -t IMPACTED_DATASETS < <(
        Execute_command "$([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote)" \
            "zfs list -H -o name \
            | xargs zfs get -o name,property all \
            | grep \" autobackup:${TASK_SCOPE}\" \
            | awk '{print \$1}' \
            | sed -E 's|.*/encrypted-ds/||'"
    )

    l_Print_scope
    l_Execute_replication_and_remount

    if [[ "${SCOPE}" != "vm_latest_snapshot_only" ]]; then
        echo
        echo "### Performing ZFS Replication completed ###"
        echo
    fi
}