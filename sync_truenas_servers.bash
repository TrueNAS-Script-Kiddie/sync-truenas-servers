#!/usr/bin/bash

# TODO: 
# * unmount fails if you opened the dataset NFS in explorer
# * stop / start Plex kubernetes instance

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
declare SSH_SERVER="truenas-master"
declare REMOTE_CMD="ssh -F ${SSH_CONFIG_FILE} ${SSH_SERVER}"

function Help() {
  echo "Help for ${SCRIPT_FILENAME}"
  echo
  echo -e "${SCRIPT_FILENAME} [-h] [--help]\t\t\t\tDisplays this help message."
  echo -e "${SCRIPT_FILENAME} [--test] --task=<task> --subtask=<subtask>\tPerforms the requested sync."
  echo
  echo -e "Optional option = --test"
  echo -e "\tThis option forces the script not change anything."
  echo
  echo "Allowed tasks:"
  echo -e "master_to_backup\t\t\t\t\t\tPerform a sync from the Master TrueNAS server to the Backup TrueNAS server (= backup)."
  echo -e "backup_to_master\t\t\t\t\t\tPerform a sync from the Backup TrueNAS server to the Master TrueNAS server (= restore)."
  echo
  echo "Allowed subtasks:"
  echo -e "all\t\t\t\t\t\t\t\tPerform rsync, ZFS replication and snapshot rollup."
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
}

function Background_error() {
  echo -e "$1"
  if [[ -n "${TAIL_PID}" ]]; then
    kill "${TAIL_PID}"
  else
    echo "ERROR: Couldn't find tail PID. Are you sure this is properly running in the background?"
  fi
  exit 1
}

function Perform_rsync() {
  function Control_plex() {
    local ACTION="$1"
    local LOCATION="$2"
    
    local ACTION_INT ACTION_MSG ACTION_VERB
    local PLEX_INSTANCE_NAME
    local PLEX_STATUS
    local CONTROL_CMD
    local JOBID_STATUS_CMD
    local JOBID    
    
    local TIMEOUT_COUNTER="0"
    local MAX_TIMEOUT=20
    
    if [[ "${ACTION}" == "start" ]]; then
      ACTION_INT="1"
      ACTION_MSG="Starting ${LOCATION} Plex again, as it was also running before."
      ACTION_VERB="starting"
    elif [[ "${ACTION}" == "stop" ]]; then
      ACTION_INT="0"
      ACTION_MSG="${LOCATION^} Plex is running or deploying. Temporary stopping it now."
      ACTION_VERB="stopping"
    else
      Background_error "The first option of Control_plex() must be either 'start' or 'stop'."
    fi
    
    if [[ "${LOCATION}" == "local" ]]; then
      PLEX_INSTANCE_NAME="plex-backup"
      [[ "${ACTION}" == "stop" ]] && LOCAL_PLEX_STATUS="$(midclt call chart.release.query "[[\"id\",\"=\",\"${PLEX_INSTANCE_NAME}\"]]" | jq -r '.[] | .status')"
      PLEX_STATUS="${LOCAL_PLEX_STATUS}"
      CONTROL_CMD="midclt call chart.release.scale ${PLEX_INSTANCE_NAME} '{\"replica_count\": ${ACTION_INT}}'"
      JOBID_STATUS_CMD="midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",JOBID_TO_FILL]]\" | jq -r '.[0].state'"
    elif [[ "${LOCATION}" == "remote" ]]; then
      PLEX_INSTANCE_NAME="plex-master"
      [[ "${ACTION}" == "stop" ]] && REMOTE_PLEX_STATUS="$(${REMOTE_CMD} "midclt call chart.release.query \"[[\\\"id\\\",\\\"=\\\",\\\"${PLEX_INSTANCE_NAME}\\\"]]\" | jq -r '.[] | .status'")"
      PLEX_STATUS="${REMOTE_PLEX_STATUS}"
      CONTROL_CMD="${REMOTE_CMD} \"midclt call chart.release.scale ${PLEX_INSTANCE_NAME} '{\\\"replica_count\\\": ${ACTION_INT}}'\""
      JOBID_STATUS_CMD="${REMOTE_CMD} \"midclt call core.get_jobs \\\"[[\\\\\\\"id\\\\\\\",\\\\\\\"=\\\\\\\",JOBID_TO_FILL]]\\\" | jq -r '.[0].state'\""
    else
      Background_error "The second option of Control_plex() must be either 'local' or 'remote'."
    fi

    if [[ "${PLEX_STATUS}" == "ACTIVE" || "${PLEX_STATUS}" == "DEPLOYING" ]]; then
      echo -n "${ACTION_MSG}"
      if JOBID="$(eval ${CONTROL_CMD})" && [[ -n "${JOBID}" ]]; then
        while [[ "$(eval ${JOBID_STATUS_CMD/JOBID_TO_FILL/${JOBID}})" != "SUCCESS" ]]; do
          ((TIMEOUT_COUNTER++))
          [[ "${TIMEOUT_COUNTER}" -gt "${MAX_TIMEOUT}" ]] && Background_error "ERROR: Waiting for ${LOCATION} Plex to ${ACTION} has timed out."
          echo -n "."
          sleep 1
        done
        echo " ${ACTION^} was successful."
      else
        echo "Failed!"
        Background_error "ERROR: Failed to obtain a \$JOBID for ${ACTION_VERB} the ${LOCATION} Plex"
      fi
    else
      [[ "${ACTION}" == "stop" ]] && echo "${LOCATION^} Plex was not running, so no need to temporary stop it..."
    fi
  }

  local -a FOLDERS_TO_RSYNC=( "Media" "Metadata" "Plug-ins" "Plug-in Support" )
  local FOLDER_TO_RSYNC
  local SOURCE_PATH
  local TARGET_PATH
  local BACKUP_PMS_PATH="/mnt/backup-pool/encrypted-ds/app-ds/plex-ds/Library/Application Support/Plex Media Server"
  local MASTER_PMS_PATH="/mnt/master-pool/encrypted-ds/app-ds/plex-ds/Library/Application Support/Plex Media Server"

  local LOCAL_PLEX_STATUS
  local REMOTE_PLEX_STATUS

  if [[ "${TASK}" == "backup_to_master" ]]; then
    SOURCE_PATH="${BACKUP_PMS_PATH}"
    TARGET_PATH="${SSH_SERVER}:${MASTER_PMS_PATH}"
  elif [[ "${TASK}" == "master_to_backup" ]]; then
    SOURCE_PATH="${SSH_SERVER}:${MASTER_PMS_PATH}"
    TARGET_PATH="${BACKUP_PMS_PATH}"
  fi
  
  echo "########################"
  echo "### Performing rsync ###"
  echo "########################"
  echo
  
  [[ ! -d "${BACKUP_PMS_PATH}" ]]                  && Background_error "ERROR: '${BACKUP_PMS_PATH}' does not exist. Is the dataset mounted and unlocked?"
  ! ${REMOTE_CMD} "test -d \"${MASTER_PMS_PATH}\"" && Background_error "ERROR: '${SSH_SERVER}:${MASTER_PMS_PATH}' does not exist. Is the dataset mounted and unlocked?"

  Control_plex stop remote
  Control_plex stop local
  echo

  # Double check that Plex isn't running anymore
  ${REMOTE_CMD} 'k3s kubectl get -n ix-plex-master pods 2>/dev/null | grep -q Running' && Background_error "ERROR: ix-plex-master is still running on the remote truenas-master server"
  k3s kubectl get -n ix-plex-backup pods 2>/dev/null | grep -q Running                 && Background_error "ERROR: ix-plex-backup is still running on the local truenas-backup server"

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

  Control_plex start remote
  Control_plex start local
  echo
  
  echo "##################################"
  echo "### Performing rsync completed ###"
  echo "##################################"
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
    local ZFS_AUTOBACKUP_FOLDER="${SCRIPT_DIR}/zfs_autobackup"
    
    cd "${ZFS_AUTOBACKUP_FOLDER}"
    echo "${ZFS_AUTOBACKUP_COMMAND} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}"
    if [[ -z "${TEST_MODE}" ]]; then
      ${ZFS_AUTOBACKUP_COMMAND} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}
      if [[ "$?" == "0" ]]; then
        echo "ZFS Replication completed successfully"
      else
        Background_error "ERROR: ZFS Replication failed"  
      fi
    fi
    echo
    cd - >/dev/null
    
    for IMPACTED_DATASET in ${IMPACTED_DATASETS}; do
      if [[ "${TASK}" == "backup_to_master" ]]; then
        echo "${REMOTE_CMD} \"zfs umount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}\""
        if [[ -z "${TEST_MODE}" ]]; then
          ${REMOTE_CMD} "zfs umount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}"
          if [[ "$?" == "0" ]]; then
            echo "Remote umount completed successfully"
          else
            Background_error "ERROR: Remote umount failed"
          fi
        fi
        echo
        
        echo "${REMOTE_CMD} \"zfs mount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}\""
        if [[ -z "${TEST_MODE}" ]]; then
          ${REMOTE_CMD} "zfs mount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}"
          if [[ "$?" == "0" ]]; then
            echo "Remote mount completed successfully"
          else
            Background_error "ERROR: Remote mount failed"
          fi
        fi
        echo
      elif [[ "${TASK}" == "master_to_backup" ]]; then
        echo "zfs umount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}"
        if [[ -z "${TEST_MODE}" ]]; then
          zfs umount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}
          if [[ "$?" == "0" ]]; then
            echo "Local umount completed successfully"
          else
            Background_error "ERROR: Local umount failed"
          fi
        fi
        echo
        
        echo "zfs mount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}"
        if [[ -z "${TEST_MODE}" ]]; then
          zfs mount ${TARGET_PARENT_DATASET}/${IMPACTED_DATASET}
          if [[ "$?" == "0" ]]; then
            echo "Local mount completed successfully"
          else
            Background_error "ERROR: Local mount failed"
          fi
        fi
        echo
      fi
    done
  }

  local SCOPE="$1"
  local TASK_SCOPE="${TASK}_${SCOPE}"
  local ZFS_AUTOBACKUP_COMMAND="autobackup-venv/bin/python -m zfs_autobackup.ZfsAutobackup"

  local SNAPSHOT_OPTARGS="--rollback --keep-source=0 --keep-target=0 --allow-empty --snapshot-format {}-%Y-%m-%d_%H-%M"
  local ZFS_OPTARGS="--zfs-compressed --decrypt --clear-refreservation"
  local ZFS_AUTOBACKUP_OPTARGS="--strip-path 2 --exclude-received"
  local SSH_OPTARGS TARGET_PARENT_DATASET IMPACTED_DATASETS ZFS_AUTOBACKUP_TASK_OPTARGS
  
  [[ "$(${REMOTE_CMD} 'zfs list -H -o mounted master-pool/encrypted-ds')" == "no" ]] && Background_error "ERROR: master-pool/encrypted-ds on truenas-master is not mounted (and/or unlocked)."
  [[ "$(zfs list -H -o mounted backup-pool/encrypted-ds)" == "no" ]]                 && Background_error "ERROR: backup-pool/encrypted-ds on truenas-backup is not mounted (and/or unlocked)."

  if [[ "${TASK}" == "backup_to_master" ]]; then
    SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-target ${SSH_SERVER}"
    TARGET_PARENT_DATASET="master-pool/encrypted-ds"
    IMPACTED_DATASETS="$(zfs list -H | awk '{print $1}' | xargs zfs get all | grep " autobackup:${TASK_SCOPE} " | awk '{print $1}' | xargs basename -a 2>/dev/null)"

    l_Check_scope
    l_Execute_replication_and_remount
  elif [[ "${TASK}" == "master_to_backup" ]]; then
    SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-source ${SSH_SERVER}"
    TARGET_PARENT_DATASET="backup-pool/encrypted-ds"
    IMPACTED_DATASETS="$(${REMOTE_CMD} "zfs list -H | awk '{print \$1}' | xargs zfs get all | grep \" autobackup:${TASK_SCOPE} \" | awk '{print \$1}' | xargs basename -a 2>/dev/null")"
    
    l_Check_scope
    l_Execute_replication_and_remount
  fi

  echo "############################################"
  echo "### Performing ZFS Replication completed ###"
  echo "############################################"
  echo
}

function Perform_rollup() {
  echo "######################################"
  echo "### Performing ZFS snapshot rollup ###"
  echo "######################################"
  echo

  if [[ "${TASK}" == "backup_to_master" ]]; then
    echo "${REMOTE_CMD} '/mnt/master-pool/homedir-ds/home/root/bin/zfs-rollup/rollup.py -v --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 master-pool/encrypted-ds/media-ds'"
    if [[ -z "${TEST_MODE}" ]]; then
      ${REMOTE_CMD} '/mnt/master-pool/homedir-ds/home/root/bin/zfs-rollup/rollup.py -v --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 master-pool/encrypted-ds/media-ds'
      if [[ "$?" == "0" ]]; then
        echo "Remote snapshot rollup completed successfully"
      else
        Background_error "ERROR: Remote snapshot rollup failed"
      fi
    fi
    echo
  elif [[ "${TASK}" == "master_to_backup" ]]; then
    echo "./zfs-rollup/rollup.py -v --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 backup-pool/encrypted-ds/media-ds"
    if [[ -z "${TEST_MODE}" ]]; then
      ${SCRIPT_DIR}/zfs-rollup/rollup.py -v --prefix auto -i hourly:48,daily:14,weekly:8,monthly:24,yearly:10 backup-pool/encrypted-ds/media-ds
      if [[ "$?" == "0" ]]; then
        echo "Local snapshot rollup completed successfully"
      else
        Background_error "ERROR: Local snapshot rollup failed"
      fi
    fi
    echo
  fi
  
  echo "################################################"
  echo "### Performing ZFS snapshot rollup completed ###"
  echo "################################################"
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
  echo "${SCRIPT_FILENAME} is now running in the background"
  echo
                                           echo "######################################################################"
                                           echo "######################################################################"
  [[ "${TASK}" == "backup_to_master" ]] && echo "### Performing a sync from TrueNAS-Backup to TrueNAS-Master server ###"
  [[ "${TASK}" == "master_to_backup" ]] && echo "### Performing a sync from TrueNAS-Master to TrueNAS-Backup server ###"
                                           echo "######################################################################"
                                           echo "######################################################################"
                                           echo 
  
  [[ -n "${PERFORM_RSYNC}" ]]           && Perform_rsync
  [[ -n "${PERFORM_ZFS_REP_ALL}" ]]     && Perform_zfs_rep all_snapshots
  [[ -n "${PERFORM_ROLLUP}" ]]          && Perform_rollup
  [[ -n "${PERFORM_ZFS_REP_LATEST}" ]]  && Perform_zfs_rep latest_snapshot_only
  
                                           echo "################################################################################"
                                           echo "################################################################################"
  [[ "${TASK}" == "backup_to_master" ]] && echo "### Sync from TrueNAS-Backup to TrueNAS-Master server completed successfully ###"
  [[ "${TASK}" == "master_to_backup" ]] && echo "### Sync from TrueNAS-Master to TrueNAS-Backup server completed successfully ###"
                                           echo "################################################################################"
                                           echo "################################################################################"
                                           echo
                                           
  [[ "${TASK}" == "backup_to_master" ]] && echo -e "Subject:Sync from TrueNAS-Backup to TrueNAS-Master server completed successfully\n\n$(cat ${LOG_FILE})" | sendmail your.email@mail.com
  [[ "${TASK}" == "master_to_backup" ]] && echo -e "Subject:Sync from TrueNAS-Master to TrueNAS-Backup server completed successfully\n\n$(cat ${LOG_FILE})" | sendmail your.email@mail.com

  [[ -n "${TAIL_PID}" ]] && kill "${TAIL_PID}"
else
  echo "Starting ${SCRIPT_FILENAME} in the background (logfile = ${LOG_FILE})."
  [[ ! -d "$(dirname "${LOG_FILE}")" ]] && mkdir "$(dirname "${LOG_FILE}")"
  nohup $0 --running_in_background "${LOG_FILE}" "${BACKUP_OPTIONS[@]}"  >>"${LOG_FILE}" 2>&1 & { sleep 1; tail -f "${LOG_FILE}"; }
fi
