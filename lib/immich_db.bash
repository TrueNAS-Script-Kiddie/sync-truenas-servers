#!/usr/bin/bash
# lib/immich_db.bash
# Immich PostgreSQL DB backup and restore functions

function Backup_immich_DB() {
    echo "### Making a backup of the Immich Postgres DB ###"
    echo

    # Dynamically find container names
    local -a CONTAINERS_TO_STOP
    mapfile -t CONTAINERS_TO_STOP < <(Execute_command "${SOURCE_LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${SOURCE_SERVER_ID}-(server|machine-learning|redis|permissions)-[0-9]+'")
    local -a CONTAINERS_TO_START
    mapfile -t CONTAINERS_TO_START < <(Execute_command "${SOURCE_LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${SOURCE_SERVER_ID}-pgvecto-[0-9]+'")

    # Make sure Immich is running
    if [[ "$(Execute_command "${SOURCE_LOCATION}" "midclt call app.query | jq -r '.[] | select(.name==\"immich-${SOURCE_SERVER_ID}\") | .state'")" != "RUNNING" ]]; then
        Background_error "ERROR: To backup the Immich DB, it must be in a running state."
    else
        echo "Immich app is running, proceeding..."
    fi

    # Stop the containers
    Control_docker_containers "${SOURCE_LOCATION}" "stop" "${CONTAINERS_TO_STOP[@]}"

    # Start the required container
    Control_docker_containers "${SOURCE_LOCATION}" "start" "${CONTAINERS_TO_START[@]}"

    # Backup the Postgres DB
    echo "Making a backup of the Immich DB to /mnt/${SOURCE_POOL}/encrypted-ds/app-ds/immich-ds/immich-pgdata-ds/${EXEC_DATE}_immich_backup.dump.sql.gz and moving it from immich-pgdata-ds to immich-data-ds/backups"
    echo "Executing on TrueNAS-${SOURCE_SERVER_ID^}: docker exec -i \"${CONTAINERS_TO_START[0]}\" bash -c 'pg_dumpall --clean --if-exists --username=immich | gzip > \"/var/lib/postgresql/${EXEC_DATE}_immich_backup.dump.sql.gz\"'"
    echo "Executing on TrueNAS-${SOURCE_SERVER_ID^}: mv \"/mnt/${SOURCE_POOL}/encrypted-ds/app-ds/immich-ds/immich-pgdata-ds/${EXEC_DATE}_immich_backup.dump.sql.gz\" \"/mnt/${SOURCE_POOL}/encrypted-ds/app-ds/immich-ds/immich-data-ds/backups/${EXEC_DATE}_immich_backup.dump.sql.gz\""
    if [[ -z "${TEST_MODE}" ]]; then
        Execute_command "${SOURCE_LOCATION}" "docker exec -i \"${CONTAINERS_TO_START[0]}\" bash -c 'pg_dumpall --clean --if-exists --username=immich | gzip > "/var/lib/postgresql/${EXEC_DATE}_immich_backup.dump.sql.gz"'" \
            || Background_error "ERROR: DB backup failed."
        Execute_command "${SOURCE_LOCATION}" "mv \"/mnt/${SOURCE_POOL}/encrypted-ds/app-ds/immich-ds/immich-pgdata-ds/${EXEC_DATE}_immich_backup.dump.sql.gz\" \"/mnt/${SOURCE_POOL}/encrypted-ds/app-ds/immich-ds/immich-data-ds/backups/${EXEC_DATE}_immich_backup.dump.sql.gz\"" \
            || Background_error "ERROR: DB move failed."
    fi
    echo
    echo "### Making a backup of the Immich Postgres DB has completed successfully ###"
    echo
}

function Restore_immich_DB() {
    function Wait_for_pg_ready() {
        local CONTAINER_NAME="$1"
        local START_TIME="$(date +%s)"
        local TIMEOUT=60 # Timeout in seconds
        local PG_READY_OUTPUT
        local CURRENT_TIME
        local ELAPSED_TIME

        echo -n "Waiting for PostgreSQL in container ${CONTAINER_NAME} to be started completely"
        while true; do
        PG_READY_OUTPUT=$(Execute_command "${TARGET_LOCATION}" "docker exec -i \"${CONTAINER_NAME}\" bash -c 'pg_isready'")
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
        sleep 5
        done
    }

    echo "### Restoring the Immich Postgres DB from backup ###"
    echo

    # Dynamically find container names
    local -a CONTAINERS_TO_STOP
    mapfile -t CONTAINERS_TO_STOP < <(Execute_command "${TARGET_LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${TARGET_SERVER_ID}-(server|machine-learning|redis|permissions|pgvecto)-[0-9]+'")
    local -a CONTAINERS_TO_START
    mapfile -t CONTAINERS_TO_START < <(Execute_command "${TARGET_LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${TARGET_SERVER_ID}-pgvecto-[0-9]+'")

    # Stop the containers
    Control_docker_containers "${TARGET_LOCATION}" "stop" "${CONTAINERS_TO_STOP[@]}"

    # Remove existing Postgres DB
    echo "Removing the existing Immich DB before restoring the backup to it."
    echo "Executing on TrueNAS-${TARGET_SERVER_ID^}: rm -rf \"/mnt/${TARGET_POOL}/encrypted-ds/app-ds/immich-ds/immich-pgdata-ds/\"*"
    [[ -z "${TEST_MODE}" ]] && \
        Execute_command "${TARGET_LOCATION}" "rm -rf \"/mnt/${TARGET_POOL}/encrypted-ds/app-ds/immich-ds/immich-pgdata-ds/\"*"

    # Start Postgres DB
    Control_docker_containers "${TARGET_LOCATION}" "start" "${CONTAINERS_TO_START[@]}"

    Wait_for_pg_ready "${CONTAINERS_TO_START[0]}"
    sleep 2

    # Restore Postgres DB from backup
    echo "Restoring of the Immich DB from /mnt/${TARGET_POOL}/encrypted-ds/app-ds/immich-ds/immich-data-ds/backups/${EXEC_DATE}_immich_backup.dump.sql.gz"
    echo "Executing on TrueNAS-${TARGET_SERVER_ID^}: gunzip < \"/mnt/${TARGET_POOL}/encrypted-ds/app-ds/immich-ds/immich-data-ds/backups/${EXEC_DATE}_immich_backup.dump.sql.gz\" | sed \"s/SELECT pg_catalog.set_config('search_path', '', false);/SELECT pg_catalog.set_config('search_path', 'public, pg_catalog', true);/g\" | docker exec -i \"${CONTAINERS_TO_START[0]}\" psql --username=immich --host=localhost"
    if [[ -z "${TEST_MODE}" ]]; then
        Execute_command "${TARGET_LOCATION}" "gunzip < /mnt/${TARGET_POOL}/encrypted-ds/app-ds/immich-ds/immich-data-ds/backups/${EXEC_DATE}_immich_backup.dump.sql.gz | sed \"s/SELECT pg_catalog.set_config('search_path', '', false);/SELECT pg_catalog.set_config('search_path', 'public, pg_catalog', true);/g\" | docker exec -i \"${CONTAINERS_TO_START[0]}\" psql --username=immich --host=localhost" >"${DB_RESTORE_LOG}" 2>&1 \
            || Background_error "ERROR: DB restore failed. Check ${DB_RESTORE_LOG} for more details."
    fi

    # Stop Immich completely
    echo -n "Stopping the Immich application completely."
    Control_app "immich-${TARGET_SERVER_ID}" "stop" "${TARGET_LOCATION}"

    # Start Immich completely
    echo -n "Starting the Immich application."
    Control_app "immich-${TARGET_SERVER_ID}" "start" "${TARGET_LOCATION}"

    echo
    echo "### Restoring the Immich Postgres DB from backup has completed successfully ###"
    echo
}