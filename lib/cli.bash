#!/usr/bin/bash
# lib/cli.bash
# Command-line parsing and help output for sync_truenas_servers

function Help() {
    echo "Help for ${SCRIPT_FILENAME}"
    echo
    echo -e "${SCRIPT_FILENAME} [-h] [--help]\t\t\t\tDisplays this help message."
    echo -e "${SCRIPT_FILENAME} [--test] --task=<task> [--subtask=<subtask>] [--app=<app>]"
    echo -e "\t\t\t\t\t\t\t\tPerforms the requested sync."
    echo
    echo -e "Optional option = --test"
    echo -e "\tThis option forces the script not change anything. It does stop/start containers."
    echo
    echo "Allowed tasks:"
    echo -e "master_to_backup\t\t\t\t\t\tPerform a sync from the TrueNAS-Master server to the TrueNAS-Backup server (= backup)."
    echo -e "backup_to_master\t\t\t\t\t\tPerform a sync from the TrueNAS-Backup server to the TrueNAS-Master server (= restore)."
    echo
    echo "Allowed subtasks:"
    echo -e "all\t\t\t\t\t\t\t\tPerform rsync, ZFS replication and snapshot rollup. This is the default."
    echo -e "rsync\t\t\t\t\t\t\t\tPerform rsync."
    echo -e "zfs_replication\t\t\t\t\t\t\tPerform ZFS replication (includes snapshot rollup)."
    echo -e "zfs_replication_without_snapshot_rollup\t\t\t\tPerform ZFS replication without snapshot rollup."
    echo -e "zfs_replication_with_all_snapshots\t\t\t\tPerform ZFS replication with all snapshots (includes snapshot rollup)."
    echo -e "zfs_replication_with_all_snapshots_without_snapshot_rollup\tPerform ZFS replication with all snapshots without snapshot rollup."
    echo -e "zfs_replication_with_latest_snapshot\t\t\t\tPerform ZFS replication with latest snapshot."
    echo -e "snapshot_rollup\t\t\t\t\t\t\tPerform snapshot rollup (doesn't sync any data)."
    echo
    echo "Allowed apps:"
    echo -e "immich\t\t\t\t\t\t\t\tLimit the rsync subtask to only copy Immich."
    echo -e "plex\t\t\t\t\t\t\t\tLimit the rsync subtask to only copy Plex."
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

    function l_Subtask_precheck() {
            if [[ -n "${PERFORM_ROLLUP}" || -n "${PERFORM_RSYNC}" || -n "${PERFORM_ZFS_REP_ALL}" || -n "${PERFORM_ZFS_REP_LATEST}" ]]; then
                echo "ERROR: You may only choose 1 subtask!"
                exit 1
            fi
    }

    function l_App_precheck() {
        if [[ "${#APPS_LIST[@]}" -ne "2"  ]]; then
            echo "ERROR: You may only choose 1 app when using --app!"
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
            l_Subtask_precheck
            PERFORM_ROLLUP="true"
            PERFORM_RSYNC="true"
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ZFS_REP_LATEST="true"
            ;;
        --subtask=rsync)
            l_Subtask_precheck
            PERFORM_RSYNC="true"
            ;;
        --subtask=zfs_replication)
            l_Subtask_precheck
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ZFS_REP_LATEST="true"
            PERFORM_ROLLUP="true"
            ;;
        --subtask=zfs_replication_without_snapshot_rollup)
            l_Subtask_precheck
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ZFS_REP_LATEST="true"
            ;;
        --subtask=zfs_replication_with_all_snapshots)
            l_Subtask_precheck
            PERFORM_ZFS_REP_ALL="true"
            PERFORM_ROLLUP="true"
            ;;
        --subtask=zfs_replication_with_all_snapshots_without_snapshot_rollup)
            l_Subtask_precheck
            PERFORM_ZFS_REP_ALL="true"
            ;;
        --subtask=zfs_replication_with_latest_snapshot)
            l_Subtask_precheck
            PERFORM_ZFS_REP_LATEST="true"
            ;;
        --subtask=snapshot_rollup)
            l_Subtask_precheck
            PERFORM_ROLLUP="true"
            ;;
        --app=plex)
            l_App_precheck
            APPS_LIST=( "plex" )
            ;;
        --app=immich)
            l_App_precheck
            APPS_LIST=( "immich" )
            ;;
        --running_in_background)
            RUNNING_IN_BACKGROUND="true"
            LOG_FILE="$1"
            shift
            ;;
        --test)
            TEST_MODE="true"
            ;;
        *)
            echo "ERROR: Option '${OPTION}' is invalid."
            exit 1
            ;;
        esac
    done

    [[ -z "${TASK}" ]] && Help
    if [[ -z "${PERFORM_ROLLUP}" && -z "${PERFORM_RSYNC}" && -z "${PERFORM_ZFS_REP_ALL}" && -z "${PERFORM_ZFS_REP_LATEST}" ]]; then
        PERFORM_ROLLUP="true"
        PERFORM_RSYNC="true"
        PERFORM_ZFS_REP_ALL="true"
        PERFORM_ZFS_REP_LATEST="true"
    fi
    
    if   [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        REMOTE_SOURCE="backup"
        LOCAL_TARGET="master"
    elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        LOCAL_SOURCE="backup"
        REMOTE_TARGET="master"
    elif   [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        LOCAL_SOURCE="master"
        REMOTE_TARGET="backup"
    elif [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        REMOTE_SOURCE="master"
        LOCAL_TARGET="backup"
    fi
}