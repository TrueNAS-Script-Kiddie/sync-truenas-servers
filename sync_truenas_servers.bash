#!/usr/bin/bash

# TODO:
# * unmount fails if you opened the dataset NFS in explorer

declare TASK
declare PERFORM_ROLLUP
declare PERFORM_RSYNC
declare PERFORM_ZFS_REP_ALL
declare PERFORM_ZFS_REP_LATEST
declare RUNNING_IN_BACKGROUND
declare TEST_MODE

declare SCRIPT_FILENAME="$(basename $0)"
declare SCRIPT_DIR="$(dirname $0)"
declare LOG_FILE="${SCRIPT_DIR}/../logs/${SCRIPT_FILENAME}.$(date +"%Y-%m-%d_%H-%M").log"
declare -a BACKUP_OPTIONS=( "$@" )
declare SSH_CONFIG_FILE="${SCRIPT_DIR}/../.ssh/config"
declare LOCAL_SERVER_ID="$(hostname -s| sed 's/^truenas-//')"
if [[ "${LOCAL_SERVER_ID}" == "master" ]]; then
    declare REMOTE_SERVER_ID="backup"
elif [[ "${LOCAL_SERVER_ID}" == "backup" ]]; then
  declare REMOTE_SERVER_ID="master"
else
  echo "ERROR: The hostname ($(hostname -s)) of the server where you are running this, is unknown. You must run this either on truenas-master or truenas-backup."
  exit 1
fi
declare REMOTE_CMD="ssh -F ${SSH_CONFIG_FILE} truenas-${REMOTE_SERVER_ID}"

function Help() {
  echo "Help for ${SCRIPT_FILENAME}"
  echo
  echo -e "${SCRIPT_FILENAME} [-h] [--help]\t\t\t\tDisplays this help message."
  echo -e "${SCRIPT_FILENAME} [--test] --task=<task> --subtask=<subtask>\tPerforms the requested sync."
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
      --running_in_background)
        RUNNING_IN_BACKGROUND="true"
        shift
        LOGFILE="$1"
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

function Background_error() {
  echo -e "$1"
  if [[ -n "${TAIL_PID}" ]]; then
    sleep 1
    kill "${TAIL_PID}"
  else
    echo "ERROR: Couldn't find tail PID. Are you sure this is properly running in the background?"
  fi
  exit 1
}

function Execute_command() {
  local MODE="$1"
  shift
  local COMMAND="$@"
  local RETURN_VALUE="0"

  if [[ "${MODE}" == *local* ]]; then
    [[ "${MODE}" == *verbose* ]] && echo "Executing command: eval ${COMMAND}"
    [[ "${MODE}" != *test* ]]    && { eval "${COMMAND}"; RETURN_VALUE="$?"; }
  elif [[ "${MODE}" == *remote* ]]; then 
    [[ "${MODE}" == *verbose* ]] && echo "Executing command: ${REMOTE_CMD} \"${COMMAND}\""
    [[ "${MODE}" != *test* ]]    && { ${REMOTE_CMD} "${COMMAND}"; RETURN_VALUE="$?"; }
  else
    Background_error "Incorrect execution mode (${MODE}). Must contain 'local' or 'remote'."
    return 1
  fi
  return "${RETURN_VALUE}"
}

function Perform_rsync() {
  function Control_plex() {
    local ACTION="$1"
    local LOCATION="$2"

    local ACTION_INT ACTION_MSG ACTION_VERB
    local PLEX_STATUS
    local CONTROL_CMD
    local JOBID_STATUS_CMD
    local JOBID
    local CONTAINER_TYPE
    local SERVER_ID_VAR

    local TIMEOUT_COUNTER="0"
    local MAX_TIMEOUT=40

    if [[ "${ACTION}" == "start" ]]; then
      ACTION_INT="1"
      ACTION_MSG="Starting ${LOCATION} Plex again, as it was also active before."
      ACTION_VERB="starting"
    elif [[ "${ACTION}" == "stop" ]]; then
      ACTION_INT="0"
      ACTION_MSG="${LOCATION^} Plex is active. Temporary stopping it now."
      ACTION_VERB="stopping"
    else
      Background_error "The first option of Control_plex() must be either 'start' or 'stop'."
    fi
    
    if Execute_command "${LOCATION}" "which docker >/dev/null 2>&1"; then
      CONTAINER_TYPE="docker"
    else
      CONTAINER_TYPE="kubernetes"
    fi
    
    SERVER_ID_VAR="${LOCATION^^}_SERVER_ID"
    if [[ "${CONTAINER_TYPE}" == "kubernetes" ]]; then
      PLEX_STATUS="$(Execute_command ${LOCATION} "midclt call chart.release.query \"[[\\\"id\\\",\\\"=\\\",\\\"plex-${!SERVER_ID_VAR}\\\"]]\" | jq -r '.[] | .status'")"
      CONTROL_CMD="midclt call chart.release.scale plex-${!SERVER_ID_VAR} '{\"replica_count\": ${ACTION_INT}}'"
      JOBID_STATUS_CMD="midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",JOBID_TO_FILL]]\" | jq -r '.[0].state'"
    elif [[ "${CONTAINER_TYPE}" == "docker" ]]; then
      PLEX_CONTAINER_NAME="$(Execute_command ${LOCATION} "docker ps -a --format '{{.Names}}' | grep \"plex-${!SERVER_ID_VAR}\"")"
      if [[ -z "${PLEX_CONTAINER_NAME}" ]]; then
        PLEX_STATUS="not_found"
      elif [[ "$(echo "${PLEX_CONTAINER_NAME}" | wc -l)" -ne 1 ]]; then
        Background_error "Multiple docker containers found with the command: docker ps -a --format '{{.Names}}' | grep plex-${!SERVER_ID_VAR}"
      else
        PLEX_STATUS="$(Execute_command ${LOCATION} "docker inspect -f '{{.State.Status}}' ${PLEX_CONTAINER_NAME}")"
        CONTROL_CMD="docker ${ACTION} ${PLEX_CONTAINER_NAME}"
        STOPSTATUS_CMD="docker wait ${PLEX_CONTAINER_NAME}"
      fi
    fi

    if { [[ "${PLEX_STATUS}" == "STOPPED" || "${PLEX_STATUS}" == "FAILED" || "${PLEX_STATUS}" == "PAUSED" || \
            "${PLEX_STATUS}" == "created" || "${PLEX_STATUS}" == "exited" || "${PLEX_STATUS}" == "paused" ]] && \
         [[ "${ACTION}" == "start" ]] } || \
       { [[ "${PLEX_STATUS}" == "ACTIVE" || "${PLEX_STATUS}" == "DEPLOYING" || "${PLEX_STATUS}" == "PAUSED" || "${PLEX_STATUS}" == "RESTARTING" || \
           "${PLEX_STATUS}" == "running" || "${PLEX_STATUS}" == "paused" || "${PLEX_STATUS}" == "restarting" ]] && \
         [[ "${ACTION}" == "stop" ]] }; then
      echo -n "${ACTION_MSG}"
      if [[ "${CONTAINER_TYPE}" == "kubernetes" ]]; then
        if JOBID="$(Execute_command "${LOCATION}" "${CONTROL_CMD}")" && [[ -n "${JOBID}" ]]; then
          while [[ "$(Execute_command "${LOCATION}" "${JOBID_STATUS_CMD/JOBID_TO_FILL/${JOBID}}")" != "SUCCESS" ]]; do
            ((TIMEOUT_COUNTER++))
            [[ "${TIMEOUT_COUNTER}" -gt "${MAX_TIMEOUT}" ]] && Background_error "ERROR: Waiting for ${LOCATION} Plex to ${ACTION} has timed out."
            echo -n "."
            sleep 1
          done
          echo " ${ACTION^} was successful."
          [[ "${ACTION}" == "stop" ]] && eval "${LOCATION^^}_PLEX_STOPPED='true'"
        else
          echo "Failed!"
          Background_error "ERROR: Failed to obtain a \$JOBID for ${ACTION_VERB} the ${LOCATION} Plex"
        fi
      elif [[ "${CONTAINER_TYPE}" == "docker" ]]; then
        if Execute_command "${LOCATION}" "${CONTROL_CMD}" >/dev/null; then
          if [[ "${ACTION}" == "stop" && "$(Execute_command "${LOCATION}" ${STOPSTATUS_CMD})" -ne 0 ]]; then
            echo "Failed!"
            Background_error "ERROR: Failed to perform ${ACTION_VERB} the ${LOCATION} Plex"
          else
            echo " ${ACTION^} was successful."
            [[ "${ACTION}" == "stop" ]] && eval "${LOCATION^^}_PLEX_STOPPED='true'"
          fi
        else
          echo "Failed!"
          Background_error "ERROR: Failed to perform ${ACTION_VERB} the ${LOCATION} Plex"
        fi
      fi
    else
      [[ "${ACTION}" == "stop" ]] && echo "${LOCATION^} Plex was not running (status ${PLEX_STATUS}), so no need to temporary stop it..."
      [[ "${ACTION}" == "start" ]] && echo "${LOCATION^} Plex cannot be started with status ${PLEX_STATUS}..."
    fi
  }

  local -a FOLDERS_TO_RSYNC=( "Media" "Metadata" "Plug-ins" "Plug-in Support" )
  local FOLDER_TO_RSYNC
  local SOURCE_PATH TARGET_PATH
  local PLEX_PATH="/mnt/REPLACEME-pool/encrypted-ds/app-ds/plex-ds/Library/Application Support/Plex Media Server"
  local REMOTE_PLEX_STOPPED LOCAL_PLEX_STOPPED

  if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
     [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
      SOURCE_PATH="truenas-${REMOTE_SERVER_ID}:${PLEX_PATH/REPLACEME/${REMOTE_SOURCE}}"
      TARGET_PATH="${PLEX_PATH/REPLACEME/${LOCAL_TARGET}}"
  elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
       [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
      SOURCE_PATH="${PLEX_PATH/REPLACEME/${LOCAL_SOURCE}}"
      TARGET_PATH="truenas-${REMOTE_SERVER_ID}:${PLEX_PATH/REPLACEME/${REMOTE_TARGET}}"
  fi

  echo "########################"
  echo "### Performing rsync ###"
  echo "########################"
  echo

  ! Execute_command local "test -d \"${PLEX_PATH/REPLACEME/${LOCAL_SOURCE}${LOCAL_TARGET}}\""    && Background_error "ERROR: '${PLEX_PATH/REPLACEME/${LOCAL_SOURCE}${LOCAL_TARGET}}' does not exist. Is the dataset mounted and unlocked?"   
  ! Execute_command remote "test -d \"${PLEX_PATH/REPLACEME/${REMOTE_SOURCE}${REMOTE_TARGET}}\"" && Background_error "ERROR: 'truenas-${REMOTE_SERVER_ID}:${PLEX_PATH/REPLACEME/${REMOTE_SOURCE}${REMOTE_TARGET}}' does not exist. Is the dataset mounted and unlocked?"

  Control_plex stop remote
  Control_plex stop local
  echo

  for FOLDER_TO_RSYNC in "${FOLDERS_TO_RSYNC[@]}"; do
    echo "rsync -e \"ssh -F ${SSH_CONFIG_FILE}\" --delete -aHX \"${SOURCE_PATH}/${FOLDER_TO_RSYNC}/\" \"${TARGET_PATH}/${FOLDER_TO_RSYNC}/\""
    if [[ -z "${TEST_MODE}" ]]; then
      rsync -e "ssh -F ${SSH_CONFIG_FILE}" --delete -aHX "${SOURCE_PATH}/${FOLDER_TO_RSYNC}/" "${TARGET_PATH}/${FOLDER_TO_RSYNC}/"
      if [[ "$?" == "0" ]]; then
        echo "Rsync of '${FOLDER_TO_RSYNC}' completed successfully"
      else
        Background_error "ERROR: Rsync of '${FOLDER_TO_RSYNC}' failed"
      fi
    fi
    echo
  done
  
  [[ "${REMOTE_PLEX_STOPPED}" == "true" ]] && Control_plex start remote
  [[ "${LOCAL_PLEX_STOPPED}" == "true" ]]  && Control_plex start local
  echo

  echo "### Performing rsync completed ###"
  echo
}

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
    local ZFS_AUTOBACKUP_FOLDER="${SCRIPT_DIR}/zfs_autobackup"
    local EXEC_MODE

    cd "${ZFS_AUTOBACKUP_FOLDER}"
    echo "${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}"
    ${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}
    if [[ "$?" == "0" ]]; then
      echo "ZFS Replication completed successfully"
    else
      Background_error "ERROR: ZFS Replication failed"
    fi
    echo
    cd - >/dev/null

    EXEC_MODE="$([[ -n "${LOCAL_TARGET}" ]] && echo local_verbose || echo remote_verbose)"
    [[ -n "${TEST_MODE}" ]] && EXEC_MODE+="_test"

    for IMPACTED_DATASET in ${IMPACTED_DATASETS}; do
      Execute_command "${EXEC_MODE}" "zfs umount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}"
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

function Perform_rollup() {
  local EXEC_MODE

  echo "######################################"
  echo "### Performing ZFS snapshot rollup ###"
  echo "######################################"
  echo

  ROLLUP_CMD="${SCRIPT_DIR/${LOCAL_SOURCE}${REMOTE_SOURCE}/${LOCAL_TARGET}${REMOTE_TARGET}}/zfs-rollup/rollup.py -v --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 ${LOCAL_TARGET}${REMOTE_TARGET}-pool/encrypted-ds/media-ds"
  
  if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
     [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
      EXEC_MODE="local_verbose"
  elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
       [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
      EXEC_MODE="remote_verbose"
  fi
  
  [[ -n "${TEST_MODE}" ]] && EXEC_MODE+="_test"
  Execute_command "${EXEC_MODE}" "${ROLLUP_CMD}"
  if [[ "$?" == "0" ]]; then
    echo "${EXEC_MODE%%_*} snapshot rollup completed successfully"
  else
    Background_error "ERROR: ${EXEC_MODE%%_*} snapshot rollup failed"
  fi

  echo
  echo "### Performing ZFS snapshot rollup completed ###"
  echo
}

###################
### Main script ###
###################

Process_command_line_options "$@"
[[ "$(id -un)" != "root" ]] && { echo "ERROR: This script can only be run as root" ; exit 1; }

if [[ -n "${RUNNING_IN_BACKGROUND}" ]]; then
  sleep 2
  TAIL_PID="$(ps -ef | grep "tail -f ${LOG_FILE}" | grep -v grep | awk '{print $2}')"
  echo "${SCRIPT_FILENAME} is now running in the background, with parent PID ${TAIL_PID}"
  echo
  echo "######################################################################"
  echo "######################################################################"
  echo "### Performing a sync from TrueNAS-${LOCAL_SOURCE^}${REMOTE_SOURCE^} to TrueNAS-${LOCAL_TARGET^}${REMOTE_TARGET^} server ###"
  echo "######################################################################"
  echo "######################################################################"
  echo

  [[ -n "${PERFORM_RSYNC}" ]]           && Perform_rsync
  [[ -n "${PERFORM_ZFS_REP_ALL}" ]]     && Perform_zfs_rep all_snapshots
  [[ -n "${PERFORM_ROLLUP}" ]]          && Perform_rollup
  [[ -n "${PERFORM_ZFS_REP_LATEST}" ]]  && Perform_zfs_rep latest_snapshot_only

  echo "################################################################################"
  echo "################################################################################"
  echo "### Sync from TrueNAS-${LOCAL_SOURCE^}${REMOTE_SOURCE^} to TrueNAS-${LOCAL_TARGET^}${REMOTE_TARGET^} server completed successfully ###"
  echo "################################################################################"
  echo "################################################################################"
  echo

  # Email the log output
  echo -e "Subject:Sync from TrueNAS-${LOCAL_SOURCE^}${REMOTE_SOURCE^} to TrueNAS-${LOCAL_TARGET^}${REMOTE_TARGET^} server completed successfully\n\n$(cat ${LOG_FILE})" | sendmail your.email@mail.com

  [[ -n "${TAIL_PID}" ]] && kill "${TAIL_PID}"
else
  echo "Starting ${SCRIPT_FILENAME} in the background (logfile = ${LOG_FILE})."
  [[ ! -d "$(dirname "${LOG_FILE}")" ]] && mkdir "$(dirname "${LOG_FILE}")"
  nohup $0 --running_in_background "${LOG_FILE}" "${BACKUP_OPTIONS[@]}"  >>"${LOG_FILE}" 2>&1 & { sleep 1; tail -f "${LOG_FILE}"; }
fi

