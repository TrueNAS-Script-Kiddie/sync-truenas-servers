#!/usr/bin/bash

# TODO:
# * Unmount fails if you opened the dataset NFS in explorer
# * Add Immich

# Variables to set in variables.bash
# ${EMAIL_TO}
# ${SSH_CONFIG_FILE}

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
declare DB_RESTORE_LOG="${SCRIPT_DIR}/../logs/${SCRIPT_FILENAME}_DB_Restore.$(date +"%Y-%m-%d_%H-%M").log"
declare -a BACKUP_OPTIONS=( "$@" )
declare EXEC_DATE="$(date +%Y-%m-%d)"

declare LOCAL_SERVER_ID="$(hostname -s | sed 's/^truenas-//')"
if [[ "${LOCAL_SERVER_ID}" == "master" ]]; then
    declare REMOTE_SERVER_ID="backup"
elif [[ "${LOCAL_SERVER_ID}" == "backup" ]]; then
  declare REMOTE_SERVER_ID="master"
else
  echo "ERROR: The hostname ($(hostname -s)) of the server where you are running this, is unknown. You must run this either on truenas-master or truenas-backup."
  exit 1
fi
declare REMOTE_SOURCE REMOTE_TARGET LOCAL_SOURCE LOCAL_TARGET
declare -a APPS_LIST=( "plex" "immich" )

source "${SCRIPT_DIR}/variables.bash"
declare REMOTE_CMD="ssh -F ${SSH_CONFIG_FILE} truenas-${REMOTE_SERVER_ID}"

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
  function Control_app_with_checks() {
    local APP_NAME="$1"
    local ACTION="$2"
    local LOCATION="$3"

    local PERFORM_ACTION
    local APP_STATE

    eval "local FULL_APP_NAME=\"${APP_NAME}-\${${LOCATION^^}_SERVER_ID}\""
    eval "local -a STOPPED_LIST=( \"\${${LOCATION^^}_STOPPED_LIST[@]}\" )"

    APP_STATE="$(Execute_command "${LOCATION}" "midclt call app.query | jq -r '.[] | select(.name==\"${FULL_APP_NAME}\") | .state'")"

    if [[ "${ACTION}" == "start" && ! "${APP_STATE}" =~ ^(STOPPED|CRASHED)$ ]]; then
      echo "WARNING: ${FULL_APP_NAME} cannot be started because its state is not 'STOPPED' or 'CRASHED'. It is ${APP_STATE}."
    elif [[ "${ACTION}" == "start" && " ${STOPPED_LIST[@]} " =~ " ${APP_NAME} " ]]; then
      # Only if the app was previously stopped, it should be started again
      PERFORM_ACTION="true"
      echo -n "Starting ${LOCATION} ${FULL_APP_NAME} again, as it was also active before."
    elif [[ "${ACTION}" == "stop" && ! "${APP_STATE}" =~ ^(RUNNING|DEPLOYING|CRASHED)$ ]]; then
      echo "WARNING: ${FULL_APP_NAME} cannot be stopped because its state is not 'RUNNING', 'DEPLOYING' or 'CRASHED'. It is ${APP_STATE}."
    elif [[ "${ACTION}" == "stop" ]]; then
      PERFORM_ACTION="true"
      echo -n "Stopping ${LOCATION} ${FULL_APP_NAME}"
    fi

    if [[ "${PERFORM_ACTION}" == "true" ]]; then
      Control_app "${FULL_APP_NAME}" "${ACTION}" "${LOCATION}"
      [[ "${ACTION}" == "stop" ]] && eval "${LOCATION^^}_STOPPED_LIST+=( \"${APP_NAME}\" )"
    fi
  }

  function Control_app() {
    local FULL_APP_NAME="$1"
    local ACTION="$2"
    local LOCATION="$3"

    local JOBID
    local TIMEOUT_COUNTER="0"
    local MAX_TIMEOUT=60

    # TODO remove?: echo -n "Stopping the Immich application."
    if JOBID="$(Execute_command "${LOCATION}" "midclt call app.${ACTION} \"${FULL_APP_NAME}\"")" && [[ -n "${JOBID}" ]]; then
      while [[ "$(Execute_command "${LOCATION}" "midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",${JOBID}]]\" | jq -r '.[0].state'")" != "SUCCESS" ]]; do
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

  # Function to wait for container state
  function Wait_for_docker_state() {
    local LOCATION="$1"
    local CONTAINER_NAME="$2"
    local DESIRED_STATE="$3"
    local START_TIME="$(date +%s)"
    local TIMEOUT=60 # Timeout in seconds

    while true; do
      CURRENT_STATE=$(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}} {{.State}}' | grep \"${CONTAINER_NAME}\" | awk '{print \$2}'")
      [[ "$CURRENT_STATE" == "$DESIRED_STATE" ]] && break
      CURRENT_TIME="$(date +%s)"
      ELAPSED_TIME="$((CURRENT_TIME - START_TIME))"
      [[ "${ELAPSED_TIME}" -ge "${TIMEOUT}" ]] && Background_error "ERROR: Failed to put ${CONTAINER_NAME} in the ${DESIRED_STATE}. Current state: ${CURRENT_STATE}."
      sleep 1
    done
  }

  function Control_docker_containers() {
    local LOCATION="$1"
    local ACTION="$2"
    shift 2
    local CONTAINERS=("$@")
    local CONTAINER
    local DESIRED_STATE CURRENT_STATE

    if [[ "${ACTION}" == "stop" ]]; then
      DESIRED_STATE="exited"
    elif [[ "${ACTION}" == "start" ]]; then
      DESIRED_STATE="running"
    else
      echo "Invalid action: ${ACTION}. Use 'stop' or 'start'."
      return 1
    fi

    for CONTAINER in "${CONTAINERS[@]}"; do
      CURRENT_STATE=$(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}} {{.State}}' | grep \"${CONTAINER}\" | awk '{print \$2}'")
      if [[ "${CURRENT_STATE}" != "${DESIRED_STATE}" ]]; then
        echo "Container ${CONTAINER}: Changing state from ${CURRENT_STATE} to ${DESIRED_STATE}"
        Execute_command "${LOCATION}" "docker ${ACTION} \"${CONTAINER}\" >/dev/null"
        Wait_for_docker_state "${LOCATION}" "${CONTAINER}" "${DESIRED_STATE}"
      else
        echo "Container ${CONTAINER}: Is already in ${DESIRED_STATE} state."
      fi
    done
  }

  function Backup_immich_DB() {
    local LOCATION="$1"
    local SERVER_ID="$2"

    echo "### Making a backup of the Immich Postgress DB ###"
    echo

    # Dynamically find container names
    local CONTAINERS_TO_STOP=( $(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${SERVER_ID}-(server|machine-learning|redis|permissions)-[0-9]+'") )
    local CONTAINERS_TO_START=( $(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${SERVER_ID}-pgvecto-[0-9]+'") )

    # Make sure Immich is running
    if [[ "$(Execute_command "${LOCATION}" "midclt call app.query | jq -r '.[] | select(.name==\"immich-${SERVER_ID}\") | .state'")" != "RUNNING" ]]; then
      Background_error "ERROR: To backup the Immich DB, it must be in a running state."
    else
      echo "Immich app is running, proceeding..."
    fi

    # Stop the containers
    Control_docker_containers "${LOCATION}" "stop" "${CONTAINERS_TO_STOP[@]}"

    # Start the required container
    Control_docker_containers "${LOCATION}" "start" "${CONTAINERS_TO_START[@]}"

    # Backup the Postgres DB
    echo "Making a backup of the Immich DB to /mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgData/${EXEC_DATE}_immich_backup.dump.sql.gz and moving it from pgData to pgBackup"
    echo "Executing on TrueNAS-${SERVER_ID^}: docker exec -i "${CONTAINERS_TO_START[0]}" bash -c 'pg_dumpall --clean --if-exists --username=immich | gzip > "/var/lib/postgresql/data/${EXEC_DATE}_immich_backup.dump.sql.gz"'"
    echo "Executing on TrueNAS-${SERVER_ID^}: mv \"/mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgData/${EXEC_DATE}_immich_backup.dump.sql.gz\" \"/mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgBackup/${EXEC_DATE}_immich_backup.dump.sql.gz\""
    if [[ -z "${TEST_MODE}" ]]; then
      Execute_command "${LOCATION}" "docker exec -i \"${CONTAINERS_TO_START[0]}\" bash -c 'pg_dumpall --clean --if-exists --username=immich | gzip > "/var/lib/postgresql/data/${EXEC_DATE}_immich_backup.dump.sql.gz"'"
      [[ "$?" != "0" ]] && Background_error "ERROR: DB backup failed."
      Execute_command "${LOCATION}" "mv \"/mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgData/${EXEC_DATE}_immich_backup.dump.sql.gz\" \"/mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgBackup/${EXEC_DATE}_immich_backup.dump.sql.gz\""
      [[ "$?" != "0" ]] && Background_error "ERROR: DB move failed."
    fi
    echo
    echo "### Making a backup of the Immich Postgress DB has completed successfully ###"
    echo
  }

  function Restore_immich_DB() {
    function Wait_for_pg_ready() {
      local LOCATION="$1"
      local CONTAINER_NAME="$2"
      local START_TIME="$(date +%s)"
      local TIMEOUT=60 # Timeout in seconds

      echo -n "Waiting for PostgreSQL in container ${CONTAINER_NAME} to be started completely"
      while true; do
        PG_READY_OUTPUT=$(Execute_command "${LOCATION}" "docker exec -i \"${CONTAINER_NAME}\" bash -c 'pg_isready'")
        if [[ "$PG_READY_OUTPUT" == *"accepting connections"* ]]; then
          echo " Start complete."
          break
        fi
        echo -n "."
        CURRENT_TIME="$(date +%s)"
        ELAPSED_TIME="$((CURRENT_TIME - START_TIME))"
        if [[ "${ELAPSED_TIME}" -ge "${TIMEOUT}" ]]; then
          echo " Timeout. Current state is ${PG_READY_OUTPUT}."
          Background_error "ERROR: Timeout waiting for PostgreSQL in container ${CONTAINER_NAME} to be ready. Current status: ${PG_READY_OUTPUT}."
          break
        fi
        sleep 1
      done
    }

    local LOCATION="$1"
    local SERVER_ID="$2"

    echo "### Restoring the Immich Postgress DB from backup ###"
    echo

    # Dynamically find container names
    local CONTAINERS_TO_STOP=($(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${SERVER_ID}-(server|machine-learning|redis|permissions|pgvecto)-[0-9]+'"))
    local CONTAINERS_TO_START=( $(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${SERVER_ID}-pgvecto-[0-9]+'") )

    # Stop the containers
    Control_docker_containers "${LOCATION}" "stop" "${CONTAINERS_TO_STOP[@]}"

    # Remove existing Postgress DB
    echo "Removing the existing Immich DB before restoring the backup to it."
    echo "Executing on TrueNAS-${SERVER_ID^}: rm -rf \"/mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgData/\"*"
    [[ -z "${TEST_MODE}" ]] && \
      Execute_command "${LOCATION}" "rm -rf \"/mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgData/\"*"

    # Start Postgress DB
    Control_docker_containers "${LOCATION}" "start" "${CONTAINERS_TO_START[@]}"

    Wait_for_pg_ready "${LOCATION}" "${CONTAINERS_TO_START[0]}"

    # Restore Postgress DB from backup
    echo "Restoring of the Immich DB from /mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgBackup/${EXEC_DATE}_immich_backup.dump.sql.gz"
    echo "Executing on TrueNAS-${SERVER_ID^}: gunzip < \"/mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgBackup/${EXEC_DATE}_immich_backup.dump.sql.gz\" | sed \"s/SELECT pg_catalog.set_config('search_path', '', false);/SELECT pg_catalog.set_config('search_path', 'public, pg_catalog', true);/g\" | docker exec -i \"${CONTAINERS_TO_START[0]}\" psql --username=immich"
    if [[ -z "${TEST_MODE}" ]]; then
      Execute_command "${LOCATION}" "gunzip < /mnt/${SERVER_ID}-pool/encrypted-ds/app-ds/immich-ds/pgBackup/${EXEC_DATE}_immich_backup.dump.sql.gz | sed \"s/SELECT pg_catalog.set_config('search_path', '', false);/SELECT pg_catalog.set_config('search_path', 'public, pg_catalog', true);/g\" | docker exec -i ${CONTAINERS_TO_START[0]} psql --username=immich --host=localhost" >"${DB_RESTORE_LOG}" 2>&1
      [[ "$?" != "0" ]] && Background_error "ERROR: DB restore failed. Check ${DB_RESTORE_LOG} for more details."
    fi

    # Stop Immich completely
    echo -n "Stopping the Immich application completely."
    Control_app "immich-${SERVER_ID}" "stop" "${LOCATION}"

    # Start Immich completely
    echo -n "Starting the Immich application."
    Control_app "immich-${SERVER_ID}" "start" "${LOCATION}"

    echo
    echo "### Restoring the Immich Postgress DB from backup has completed successfully ###"
    echo
  }

  local -a LOCATIONS_LIST=( "local" "remote" )
  local -a PLEX_FOLDERS_TO_RSYNC_LIST=( "Media" "Metadata" "Plug-ins" "Plug-in Support" )
  local -a IMMICH_FOLDERS_TO_RSYNC_LIST=( "library" "profile" "thumbs" "uploads" "video" "pgBackup" )
  local PLEX_PATH="/mnt/LOCATION_TO_INSERT-pool/encrypted-ds/app-ds/plex-ds/Library/Application Support/Plex Media Server"
  local IMMICH_PATH="/mnt/LOCATION_TO_INSERT-pool/encrypted-ds/app-ds/immich-ds"

  local APP_NAME
  local -a REMOTE_STOPPED_LIST LOCAL_STOPPED_LIST
  local APP_PATH_VAR APP_PATH
  local LOCATION
  local SOURCE_PATH TARGET_PATH
  local -a FOLDERS_TO_RSYNC_LIST 
  local FOLDER_TO_RSYNC
  local FULL_PATH

  echo "########################"
  echo "### Performing rsync ###"
  echo "########################"
  echo

  for APP_NAME in "${APPS_LIST[@]}"; do
    REMOTE_STOPPED_LIST=()
    LOCAL_STOPPED_LIST=()

    APP_PATH_VAR="${APP_NAME^^}_PATH"
    APP_PATH="${!APP_PATH_VAR}"

    # Check if local and remote application datasets are available
    ! Execute_command local "test -d \"${APP_PATH/LOCATION_TO_INSERT/${LOCAL_SOURCE}${LOCAL_TARGET}}\""    \
      && Background_error "ERROR: '${APP_PATH/LOCATION_TO_INSERT/${LOCAL_SOURCE}${LOCAL_TARGET}}' does not exist. Is the dataset mounted and unlocked?"
    ! Execute_command remote "test -d \"${APP_PATH/LOCATION_TO_INSERT/${REMOTE_SOURCE}${REMOTE_TARGET}}\"" \
      && Background_error "ERROR: 'truenas-${REMOTE_SERVER_ID}:${APP_PATH/LOCATION_TO_INSERT/${REMOTE_SOURCE}${REMOTE_TARGET}}' does not exist. Is the dataset mounted and unlocked?"

    # Backup Immich Postgress DB
    [[ "${APP_NAME}" == "immich" ]] && Backup_immich_DB "$([[ -n "${LOCAL_SOURCE}" ]] && echo "local" || echo "remote")" ${LOCAL_SOURCE}${REMOTE_SOURCE}

    # Stop the application locally and remotely
    for LOCATION in "${LOCATIONS_LIST[@]}"; do
      Control_app_with_checks ${APP_NAME} stop ${LOCATION}
    done
    echo
    
    # Prepare the rsyncs
    if [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "master" ]] || \
      [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "backup" ]]; then
        SOURCE_PATH="truenas-${REMOTE_SERVER_ID}:${APP_PATH/LOCATION_TO_INSERT/${REMOTE_SOURCE}}"
        TARGET_PATH="${APP_PATH/LOCATION_TO_INSERT/${LOCAL_TARGET}}"
    elif [[ "${TASK}" == "backup_to_master" && "${LOCAL_SERVER_ID}" == "backup" ]] || \
        [[ "${TASK}" == "master_to_backup" && "${LOCAL_SERVER_ID}" == "master" ]]; then
        SOURCE_PATH="${APP_PATH/LOCATION_TO_INSERT/${LOCAL_SOURCE}}"
        TARGET_PATH="truenas-${REMOTE_SERVER_ID}:${APP_PATH/LOCATION_TO_INSERT/${REMOTE_TARGET}}"
    fi

    eval "local -a FOLDERS_TO_RSYNC_LIST=( \"\${${APP_NAME^^}_FOLDERS_TO_RSYNC_LIST[@]}\" )"
    for FOLDER_TO_RSYNC in "${FOLDERS_TO_RSYNC_LIST[@]}"; do
      # Check if the source and target directories exist
      for FULL_PATH in "${SOURCE_PATH}" "${TARGET_PATH}"; do
        if [[ "${FULL_PATH}" == *:* ]]; then
          Execute_command remote "test -d \"${FULL_PATH#*:}/${FOLDER_TO_RSYNC}\"" || Background_error "ERROR: '${FULL_PATH}/${FOLDER_TO_RSYNC}' does not exist. Is the dataset mounted and unlocked?"
        else
          Execute_command local "test -d \"${FULL_PATH#*:}/${FOLDER_TO_RSYNC}\"" || Background_error "ERROR: '${FULL_PATH}/${FOLDER_TO_RSYNC}' does not exist. Is the dataset mounted and unlocked?"
        fi
      done
    
      # Perform the rsyncs
      echo "rsync ${TEST_MODE:+--dry-run} -e \"ssh -F ${SSH_CONFIG_FILE}\" --delete -aHX \"${SOURCE_PATH}/${FOLDER_TO_RSYNC}/\" \"${TARGET_PATH}/${FOLDER_TO_RSYNC}/\""
      rsync ${TEST_MODE:+--dry-run} -e "ssh -F ${SSH_CONFIG_FILE}" --delete -aHX "${SOURCE_PATH}/${FOLDER_TO_RSYNC}/" "${TARGET_PATH}/${FOLDER_TO_RSYNC}/"
      if [[ "$?" == "0" ]]; then
        echo "Rsync of '${FOLDER_TO_RSYNC}' completed successfully"
      else
        Background_error "ERROR: Rsync of '${FOLDER_TO_RSYNC}' failed"
      fi
      echo
    done

    # Start the application locally and remotely if they were stopped
    for LOCATION in "${LOCATIONS_LIST[@]}"; do
      Control_app_with_checks ${APP_NAME} start ${LOCATION}
    done
    echo

    # Restore Immich Postgress DB
    [[ "${APP_NAME}" == "immich" ]] && Restore_immich_DB "$([[ -n "${LOCAL_TARGET}" ]] && echo "local" || echo "remote")" ${LOCAL_TARGET}${REMOTE_TARGET}
  done

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
  echo -e "Subject:Sync from TrueNAS-${LOCAL_SOURCE^}${REMOTE_SOURCE^} to TrueNAS-${LOCAL_TARGET^}${REMOTE_TARGET^} server completed successfully\n\n$(cat ${LOG_FILE})" | sendmail "${EMAIL_TO}"

  [[ -n "${TAIL_PID}" ]] && kill "${TAIL_PID}"
else
  echo "Starting ${SCRIPT_FILENAME} in the background (logfile = ${LOG_FILE})."
  [[ ! -d "$(dirname "${LOG_FILE}")" ]] && mkdir "$(dirname "${LOG_FILE}")"
  nohup $0 --running_in_background "${LOG_FILE}" "${BACKUP_OPTIONS[@]}"  >>"${LOG_FILE}" 2>&1 & { sleep 1; tail -f "${LOG_FILE}"; }
fi
