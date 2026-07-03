#!/usr/bin/bash
# lib/cli.bash
# Command-line parsing and help output for sync_truenas_servers

function Help() {
    echo "Help for ${SCRIPT_FILENAME}"
    echo
    echo -e "${SCRIPT_FILENAME} [-h] [--help]\t\t\t\tDisplays this help message."
    echo -e "${SCRIPT_FILENAME} [--test] --task=<task> [--subtask=<subtask>] [--app=<app>] [--vm=<name>*]"
    echo -e "\t\t\t\t\t\t\t\tPerforms the requested replication."
    echo
    echo -e "--test"
    echo -e "\tThis option forces the script not change anything. It does stop/start containers."
    echo
    echo -e "--vm=<name>"
    echo -e "\tThis option limits VM replication to specific VMs."
    echo -e "\tRepeat this option for each VM: e.g. --vm=VM1 --vm=VM2"
    echo
    echo "Allowed tasks:"
    echo -e "master_to_backup\t\t\t\t\t\tPerform a sync from the TrueNAS-Master server to the TrueNAS-Backup server (= backup)."
    echo -e "backup_to_master\t\t\t\t\t\tPerform a sync from the TrueNAS-Backup server to the TrueNAS-Master server (= restore)."
    echo
    echo "Allowed subtasks:"
    echo -e "all\t\t\t\t\t\t\t\tPerform Application, VM and ZFS Replication / Snapshot Rollup. This is the default."
    echo -e "app_replication\t\t\t\t\t\t\tPerform Application Replication."
    echo -e "vm_replication\t\t\t\t\t\t\tPerform VM replication."
    echo -e "zfs_replication\t\t\t\t\t\t\tPerform ZFS replication (includes snapshot rollup)."
    echo -e "zfs_replication_without_snapshot_rollup\t\t\t\tPerform ZFS replication without snapshot rollup."
    echo -e "zfs_replication_with_all_snapshots\t\t\t\tPerform ZFS replication with all snapshots (includes snapshot rollup)."
    echo -e "zfs_replication_with_all_snapshots_without_snapshot_rollup\tPerform ZFS replication with all snapshots without snapshot rollup."
    echo -e "zfs_replication_with_latest_snapshot\t\t\t\tPerform ZFS replication with latest snapshot."
    echo -e "snapshot_rollup\t\t\t\t\t\t\tPerform snapshot rollup (doesn't sync any data)."
    echo
    echo "Allowed apps:"
    echo -e "immich\t\t\t\t\t\t\t\tLimit the Application Replication subtask to only copy Immich."
    echo -e "plex\t\t\t\t\t\t\t\tLimit the Application Replication subtask to only copy Plex."
    echo

    exit 0
}

function Process_command_line_options() {
    function l_Task_precheck() {
        if [[ -n "${TASK}" ]]; then
            echo "ERROR: You may only choose 1 task!"
            exit 1
        fi
    }

    local OPTION

    while [[ $# -gt 0 ]]; do
        OPTION="$1"
        shift
        case "${OPTION}" in
        -h)
            Help
            ;;
        --help)
            Help
            ;;
        --task=master_to_backup)
            l_Task_precheck
            TASK="master_to_backup"
            ;;
        --task=backup_to_master)
            l_Task_precheck
            TASK="backup_to_master"
            ;;
        --subtask=all)
            PERFORM_ROLLUP="true"
            PERFORM_APP_REP="true"
            PERFORM_VM_REP="true"
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ZFS_REP_LATEST="true"
            ;;
        --subtask=app_replication)
            PERFORM_APP_REP="true"
            ;;
        --subtask=vm_replication)
            PERFORM_VM_REP="true"
            ;;
        --subtask=zfs_replication)
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ZFS_REP_LATEST="true"
            PERFORM_ROLLUP="true"
            ;;
        --subtask=zfs_replication_without_snapshot_rollup)
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ZFS_REP_LATEST="true"
            ;;
        --subtask=zfs_replication_with_all_snapshots)
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ROLLUP="true"
            ;;
        --subtask=zfs_replication_with_all_snapshots_without_snapshot_rollup)
            PERFORM_ZFS_REP_ALL="true"
            ;;
        --subtask=zfs_replication_with_latest_snapshot)
            PERFORM_ZFS_REP_LATEST="true"
            ;;
        --subtask=snapshot_rollup)
            PERFORM_ROLLUP="true"
            ;;
        --app=*)
            # shellcheck disable=SC2076  # deliberate literal-substring match, not regex
            [[ ! " ${APP_LIST[*]} " =~ " ${OPTION#--app=} " ]] && APP_LIST+=( "${OPTION#--app=}" )
            ;;
        --vm=*)
            # shellcheck disable=SC2076  # deliberate literal-substring match, not regex
            [[ ! " ${VM_LIST[*]} " =~ " ${OPTION#--vm=} " ]] && VM_LIST+=( "${OPTION#--vm=}" )
            ;;
        --running_in_background)
            # shellcheck disable=SC2034  # consumed by bin/sync_truenas_servers
            RUNNING_IN_BACKGROUND="true"
            # shellcheck disable=SC2034  # consumed by bin/sync_truenas_servers
            LOG_FILE="$1"
            shift
            ;;
        --test)
            # shellcheck disable=SC2034  # TEST_MODE consumed throughout lib/*.bash (see architectural_patterns.md)
            TEST_MODE="true"
            ;;
        *)
            echo "ERROR: Option '${OPTION}' is invalid."
            exit 1
            ;;
        esac
    done

    [[ -z "${TASK}" ]] && Help

    # When no subtask is specified, then all subtasks are enabled by default
    if [[ -z "${PERFORM_ROLLUP}" && -z "${PERFORM_APP_REP}" && -z "${PERFORM_VM_REP}" && -z "${PERFORM_ZFS_REP_ALL}" && -z "${PERFORM_ZFS_REP_LATEST}" ]]; then
        PERFORM_ROLLUP="true"
        PERFORM_APP_REP="true"
        PERFORM_VM_REP="true"
        PERFORM_ZFS_REP_ALL="true"
        PERFORM_ZFS_REP_LATEST="true"
    fi

    # --vm is only allowed if $PERFORM_VM_REP=true
    if [[ "${#VM_LIST[@]}" -gt 0 && -z "${PERFORM_VM_REP}" ]]; then
        echo "ERROR: --vm can only be used when the vm_replication subtask is enabled."
        exit 1
    fi
    
    # --app is only allowed if $PERFORM_APP_REP=true
    if [[ "${#APP_LIST[@]}" -gt 0 && -z "${PERFORM_APP_REP}" ]]; then
        echo "ERROR: --app can only be used when the app_replication subtask is enabled."
        exit 1
    fi
    

    # Define source and target
    # shellcheck disable=SC2034  # LOCAL_SOURCE/REMOTE_SOURCE/LOCAL_TARGET/REMOTE_TARGET consumed across lib/*.bash (direction model, see architectural_patterns.md)
    case "${TASK}:${LOCAL_SERVER_ID}" in
        backup_to_master:master) REMOTE_SOURCE="backup"; LOCAL_TARGET="master" ;;
        backup_to_master:backup) LOCAL_SOURCE="backup"; REMOTE_TARGET="master" ;;
        master_to_backup:master) LOCAL_SOURCE="master"; REMOTE_TARGET="backup" ;;
        master_to_backup:backup) REMOTE_SOURCE="master"; LOCAL_TARGET="backup" ;;
    esac
}