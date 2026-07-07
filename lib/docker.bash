#!/usr/bin/bash
# lib/docker.bash
# Docker container control helpers

function Wait_for_docker_state() {
    local LOCATION="$1"
    local CONTAINER_NAME="$2"
    local DESIRED_STATE="$3"
    local START_TIME="$(date +%s)"
    local MAX_TIMEOUT=60
    local CURRENT_STATE
    local CURRENT_TIME
    local ELAPSED_TIME

    while true; do
        CURRENT_STATE=$(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}} {{.State}}' | awk -v c=\"${CONTAINER_NAME}\" '\$1==c {print \$2}'")
        [[ "$CURRENT_STATE" == "$DESIRED_STATE" ]] && break
        CURRENT_TIME="$(date +%s)"
        ELAPSED_TIME="$((CURRENT_TIME - START_TIME))"
        [[ "${ELAPSED_TIME}" -ge "${MAX_TIMEOUT}" ]] && Background_error "ERROR: Failed to put ${CONTAINER_NAME} in the ${DESIRED_STATE}. Current state: ${CURRENT_STATE}."
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

    case "${ACTION}" in
        stop)  DESIRED_STATE="exited" ;;
        start) DESIRED_STATE="running" ;;
        *)     echo "Invalid action: ${ACTION}. Use 'stop' or 'start'."; return 1 ;;
    esac

    for CONTAINER in "${CONTAINERS[@]}"; do
        CURRENT_STATE=$(Execute_command "${LOCATION}" "docker ps -a --format '{{.Names}} {{.State}}' | awk -v c=\"${CONTAINER}\" '\$1==c {print \$2}'")
        if [[ "${CURRENT_STATE}" != "${DESIRED_STATE}" ]]; then
            echo "Container ${CONTAINER}: Changing state from ${CURRENT_STATE} to ${DESIRED_STATE}"
            Execute_command "${LOCATION}" "docker ${ACTION} \"${CONTAINER}\" >/dev/null"
            Wait_for_docker_state "${LOCATION}" "${CONTAINER}" "${DESIRED_STATE}"
        else
            echo "Container ${CONTAINER}: Is already in ${DESIRED_STATE} state."
        fi
    done
}