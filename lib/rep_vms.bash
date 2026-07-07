#!/usr/bin/bash
# lib/rep_vms.bash
# TrueNAS VM replication Orchestration

# Workspace
VM_TMP_DIR="${SCRIPT_DIR}/../tmp/vms"
mkdir -p "${VM_TMP_DIR}/json/per_vm"

declare SOURCE_ALL_VM_JSON
declare TARGET_ALL_VM_JSON
declare -a SOURCE_EXTRACTED_VM_LIST=()

# Utility: filter by VM_LIST
function Vm_in_scope() {
    local SOURCE_OR_TARGET="$1"
    local VM="$2"
    local ITEM

    if [[ "${SOURCE_OR_TARGET}" == "SOURCE" ]]; then
        if [[ "${#VM_LIST[@]}" -eq 0 ]]; then
            # No filter list: all VMs are in scope
            return 0
        else
            for ITEM in "${VM_LIST[@]}"; do
                if [[ "${ITEM}" == "${VM}" ]]; then
                    return 0
                fi
            done
            return 1
        fi
    elif [[ "${SOURCE_OR_TARGET}" == "TARGET" ]]; then
        for ITEM in "${SOURCE_EXTRACTED_VM_LIST[@]}"; do
            if [[ "${ITEM}" == "${VM}" ]]; then
                return 0
            fi
        done
        return 1
    fi
}

# Stop or start a VM by id, waiting until the operation is FULLY complete.
#
# GRACEFUL stops only, never forced — load-bearing. A forced poweroff flips .status.state
# to STOPPED while libvirt is still tearing the domain down, which (a) would snapshot the
# source zvol before its guest cleanly shut down, and (b) leaves an orphan libvirt domain
# that vm.delete won't undefine, breaking the recreate on a UUID collision (see plan 09).
# Graceful settles fully: .state reaches STOPPED only once the domain is SHUTOFF.
#
# vm.stop is a JOB → wait for it to reach SUCCESS (core.get_jobs), not just for .state,
# then confirm STOPPED (a graceful job can 'succeed' while a still-booting/unresponsive
# guest ignored ACPI — clear abort then). vm.start is synchronous → just poll the state.
# 180s timeout → Background_error. Skipped under --test.
function Control_vm() {
    local VM_NAME="$1"
    local VM_ID="$2"
    local ACTION="$3"          # stop or start
    local LOCATION="$4"        # local or remote (of the host owning the VM)
    local SERVER_ID="$5"       # for log messages only

    local DESIRED_STATE
    local ACTION_LABEL
    local JOBID
    local JOB_STATE
    local CURRENT_STATE
    local TIMEOUT_COUNTER=0
    local MAX_TIMEOUT=180

    # Abort, first closing the in-progress "...dots" line so the error starts on its own line.
    l_Abort() { echo; Background_error "$1"; }

    case "${ACTION}" in
        stop)  DESIRED_STATE="STOPPED"; ACTION_LABEL="Stopping" ;;
        start) DESIRED_STATE="RUNNING"; ACTION_LABEL="Starting" ;;
        *)     Background_error "ERROR: Invalid VM action '${ACTION}' (expected 'stop' or 'start')." ;;
    esac

    echo -n "${ACTION_LABEL} VM '${VM_NAME}' (id ${VM_ID}) on truenas-${SERVER_ID}${TEST_MODE:+" (Not done because of '--test' usage!)"}"
    [[ -n "${TEST_MODE}" ]] && { echo; echo; return 0; }

    if [[ "${ACTION}" == "stop" ]]; then
        # Wait for the stop JOB itself to finish (not just .state) — libvirt settled before vm.delete.
        JOBID="$(Execute_command "${LOCATION}" "midclt call vm.stop ${VM_ID}")"
        [[ -n "${JOBID}" ]] || l_Abort "ERROR: vm.stop for VM '${VM_NAME}' (id ${VM_ID}) on truenas-${SERVER_ID} returned no job id."
        while true; do
            JOB_STATE="$(Execute_command "${LOCATION}" "midclt call core.get_jobs \"[[\\\"id\\\",\\\"=\\\",${JOBID}]]\" | jq -r '.[0].state'")"
            [[ "${JOB_STATE}" == "SUCCESS" ]] && break
            [[ "${JOB_STATE}" =~ ^(FAILED|ABORTED|ERROR)$ ]] \
                && l_Abort "ERROR: vm.stop for VM '${VM_NAME}' (id ${VM_ID}) on truenas-${SERVER_ID} ended in state ${JOB_STATE}."
            ((TIMEOUT_COUNTER++))
            [[ "${TIMEOUT_COUNTER}" -gt "${MAX_TIMEOUT}" ]] \
                && l_Abort "ERROR: VM '${VM_NAME}' (id ${VM_ID}) on truenas-${SERVER_ID} did not shut down within ${MAX_TIMEOUT}s (job state: ${JOB_STATE}). The guest is likely still booting or is not responding to a graceful (ACPI) shutdown — let it finish booting and retry, or stop it manually."
            echo -n "."
            sleep 1
        done
        # A graceful stop job can succeed without the guest actually powering off; make sure.
        CURRENT_STATE="$(Execute_command "${LOCATION}" "midclt call vm.query \"[[\\\"id\\\",\\\"=\\\",${VM_ID}]]\" | jq -r '.[0].status.state'")"
        [[ "${CURRENT_STATE}" == "STOPPED" ]] \
            || l_Abort "ERROR: VM '${VM_NAME}' (id ${VM_ID}) on truenas-${SERVER_ID} did not power off — state is still ${CURRENT_STATE} after the graceful shutdown. The guest ignored the ACPI shutdown (likely still booting or unresponsive) — let it finish booting and retry, or stop it manually."
    else
        # vm.start is synchronous; poll the state until the guest is actually running.
        Execute_command "${LOCATION}" "midclt call vm.start ${VM_ID} >/dev/null 2>&1" \
            || l_Abort "ERROR: Failed to start VM '${VM_NAME}' (id ${VM_ID}) on truenas-${SERVER_ID}."
        while true; do
            CURRENT_STATE="$(Execute_command "${LOCATION}" "midclt call vm.query \"[[\\\"id\\\",\\\"=\\\",${VM_ID}]]\" | jq -r '.[0].status.state'")"
            [[ "${CURRENT_STATE}" == "RUNNING" ]] && break
            ((TIMEOUT_COUNTER++))
            [[ "${TIMEOUT_COUNTER}" -gt "${MAX_TIMEOUT}" ]] \
                && l_Abort "ERROR: VM '${VM_NAME}' (id ${VM_ID}) on truenas-${SERVER_ID} did not reach RUNNING within ${MAX_TIMEOUT}s (current state: ${CURRENT_STATE})."
            echo -n "."
            sleep 1
        done
    fi
    echo " ${DESIRED_STATE}."
    echo
}

# Warn that a running target VM was disturbed by an aborted run. Registered on the cleanup
# stack when --stop-running-vms stops a running target, unregistered on a clean finish; on
# abort it fires so the operator knows the target is NOT in its pre-run state. We don't
# auto-start it — it was mid destroy/rebuild.
function Warn_target_vm_state() {
    local VM="$1"
    local SERVER_ID="$2"

    echo
    echo "  !!! WARNING: VM '${VM}' was RUNNING on truenas-${SERVER_ID} before this run."
    echo "  !!! The run stopped it to rebuild the target copy, then aborted before finishing."
    echo "  !!! Its state now differs from before the run — check it and start it manually"
    echo "  !!! on truenas-${SERVER_ID} if needed."
    echo
}

####################################
# Tasks for all VMs simultaneously #
####################################

# Extract VM definitions into a variable as JSON
function Extract_vm_definitions() {
    echo "### Extracting VM definitions ###"

    # Remove old jsons
    rm -f "${VM_TMP_DIR}/json/per_vm/"*.json 2>/dev/null

    for SOURCE_OR_TARGET in "SOURCE" "TARGET"; do
        local LOCATION_VAR LOCATION SERVER_ID_VAR SERVER_ID ALL_VM_JSON
        local -n SOURCE_OR_TARGET_ALL_VM_JSON_REF="${SOURCE_OR_TARGET}_ALL_VM_JSON"
        local VM_NAMES

        LOCATION_VAR="${SOURCE_OR_TARGET}_LOCATION"
        LOCATION="${!LOCATION_VAR}"
        SERVER_ID_VAR="${LOCATION^^}_SERVER_ID"
        SERVER_ID="${!SERVER_ID_VAR}"

        ALL_VM_JSON="$(Execute_command "${LOCATION}" "midclt call vm.query" 2>/dev/null)" \
            || Background_error "ERROR: truenas-${SERVER_ID} - Failed to extract VM definitions"

        # shellcheck disable=SC2034  # nameref write-through to SOURCE_ALL_VM_JSON/TARGET_ALL_VM_JSON; shellcheck can't resolve the dynamically-named target
        SOURCE_OR_TARGET_ALL_VM_JSON_REF="${ALL_VM_JSON}"

        if jq -e 'length == 0' <<<"${ALL_VM_JSON}" >/dev/null; then
            echo "truenas-${SERVER_ID} - No VMs found"
            continue
        fi

        mapfile -t VM_NAMES < <(jq -r '.[].name' <<<"${ALL_VM_JSON}")
        for VM in "${VM_NAMES[@]}"; do
            if ! Vm_in_scope "${SOURCE_OR_TARGET}" "${VM}"; then
                echo "truenas-${SERVER_ID} - VM '${VM}' not in scope for replication"
                continue
            fi

            jq --arg VM "${VM}" '.[] | select(.name == $VM)' <<<"${ALL_VM_JSON}" \
                > "${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${SERVER_ID}.${SOURCE_OR_TARGET,,}.json"

            echo "truenas-${SERVER_ID} - Extracted ${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${SERVER_ID}.${SOURCE_OR_TARGET,,}.json"

            [[ "${SOURCE_OR_TARGET}" == "SOURCE" ]] && SOURCE_EXTRACTED_VM_LIST+=("${VM}")
        done
    done
}

##############################
# Tasks per VMs individually #
##############################

# Transform a VM definition from the source host into a destination‑ready definition
# using vm_device_mappings.json
#
# Usage: Transform_vm_definition <SOURCE_SERVER_ID> <TARGET_SERVER_ID>
function Transform_vm_definition() {
    local MAP_FILE="${SCRIPT_DIR}/../config/vm_device_mappings.json"
    local VM_DECISION_LOG
    local VALID

    echo "Transforming source VM definition into destination VM definition..."

    # 0) Sanity: mapping file
    [[ -f "${MAP_FILE}" ]] || Background_error "ERROR: mapping file not found: ${MAP_FILE}"

    # 1) Store decision log for debug-on-error
    VM_DECISION_LOG=$(
        jq -r --arg SOURCE_SERVER_ID "${SOURCE_SERVER_ID}" --arg TARGET_SERVER_ID "${TARGET_SERVER_ID}" --argfile MAP_FILE "${MAP_FILE}" '
        def DEVS: (.devices // (.[0].devices));
        DEVS[]
        | . as $DEVICE
        | $DEVICE.attributes.dtype as $DTYPE
        | ($MAP_FILE[$DTYPE] // null) as $DEVICE_MAP
        | "DEC: DTYPE=" + ($DTYPE // "null")
            + " MAP_PRESENT=" + (((($DEVICE_MAP|type) != "null"))|tostring)
        ,
        ( if ($DEVICE_MAP|type) != "null" then
            $DEVICE_MAP
            | to_entries[]
            | select(.key != "_doc")
            | .key as $ATTR
            | ($DEVICE.attributes[$ATTR] // "" ) as $SOURCE_VALUE
            | ($SOURCE_VALUE | tostring) as $SOURCE_VALUES
            | ( first(
                  .value[]
                  | . as $RULE_MATCH
                  | ( ($RULE_MATCH.scope == null) or ($SOURCE_VALUES | contains($RULE_MATCH.scope)) ) as $SCOPE_MATCH
                  | ( $SOURCE_VALUES == ($RULE_MATCH[$SOURCE_SERVER_ID] | tostring) ) as $SOURCE_EQUALS_RULE
                  | ( $SOURCE_VALUES | contains($RULE_MATCH[$SOURCE_SERVER_ID] | tostring) ) as $SOURCE_CONTAINS_RULE
                  | select( $SCOPE_MATCH and ( $SOURCE_EQUALS_RULE or $SOURCE_CONTAINS_RULE ) )
              ) ) as $RULE_MATCH
            | if $RULE_MATCH != null then
                "  dtype=" + $DTYPE
                + " attribute=" + $ATTR
                + " source_value=" + ($SOURCE_VALUE|tostring)
                + " source_rule=" + (($RULE_MATCH[$SOURCE_SERVER_ID] // "null")|tostring)
                + " target_rule=" + (($RULE_MATCH[$TARGET_SERVER_ID] // "null")|tostring)
                + " scope=" + (($RULE_MATCH.scope // "null")|tostring)
                + " | apply=true"
              else empty end
          else empty end )
        ' "${SOURCE_VM_JSON_FILE}"
    )

    # 2) Print mappings that will be done in 3) + warn about unmapped DISK paths
    jq -r \
    --arg SOURCE_SERVER_ID "${SOURCE_SERVER_ID}" \
    --arg TARGET_SERVER_ID "${TARGET_SERVER_ID}" \
    --argfile MAP_FILE "${MAP_FILE}" '
    def DEVICES: (.devices // (.[0].devices));
    def SCOPES: ($MAP_FILE.DISK.path | map(.scope) | map(select(. != null)));

    DEVICES[]
    | . as $DEVICE
    | $DEVICE.attributes.dtype as $DTYPE
    | ($MAP_FILE[$DTYPE] // null) as $DEVICE_MAP
    | if ($DEVICE_MAP|type) == "null" then
        # no mapping rules for this dtype
        if $DTYPE == "DISK" then
            # warn if path not covered by any scope
            ($DEVICE.attributes.path // "" ) as $DISK_PATH
            | if (SCOPES | any($DISK_PATH | contains(.))) then empty
              else "WARNING\tpath\t\($DISK_PATH) → (no mapping rule)"
              end
        else empty end
      else
        (
            $DEVICE_MAP
            | to_entries[]
            | select(.key != "_doc")
            | .key as $ATTR
            | ($DEVICE.attributes[$ATTR] // "" ) as $SOURCE_VALUE
            | ($SOURCE_VALUE | tostring) as $SOURCE_STR
            | ( first(
                  .value[]
                  | . as $RULE_MATCH
                  | (($RULE_MATCH.scope == null) or ($SOURCE_STR | contains($RULE_MATCH.scope))) as $SCOPE_MATCH
                  | ($SOURCE_STR == ($RULE_MATCH[$SOURCE_SERVER_ID]|tostring)) as $EQUALS
                  | ($SOURCE_STR | contains($RULE_MATCH[$SOURCE_SERVER_ID]|tostring)) as $CONTAINS
                  | select($SCOPE_MATCH and ($EQUALS or $CONTAINS))
              ) ) as $RULE_MATCH
            | if $RULE_MATCH != null then
                if ($SOURCE_STR == ($RULE_MATCH[$SOURCE_SERVER_ID]|tostring)) then
                  "\($DTYPE)\t\($ATTR)\t\($SOURCE_VALUE) → \($RULE_MATCH[$TARGET_SERVER_ID])"
                elif ($SOURCE_STR | contains($RULE_MATCH[$SOURCE_SERVER_ID]|tostring)) then
                  "\($DTYPE)\t\($ATTR)\t\($SOURCE_VALUE) → \($SOURCE_STR | sub(($RULE_MATCH[$SOURCE_SERVER_ID]|tostring); ($RULE_MATCH[$TARGET_SERVER_ID]|tostring)))"
                else empty end
              else empty end
        )
      end
    ' "${SOURCE_VM_JSON_FILE}" | column -t -s $'\t' | sed 's/^/  /'

    # 3) Apply JSON mappings and write to $TRANSFORMED_VM_JSON_FILE
    jq --arg SOURCE_SERVER_ID "${SOURCE_SERVER_ID}" --arg TARGET_SERVER_ID "${TARGET_SERVER_ID}" --argfile MAP_FILE "${MAP_FILE}" '
      def apply_device($DEVICE):
        $DEVICE
        | .attributes.dtype as $DTYPE
        | ($MAP_FILE[$DTYPE] // null) as $DEVICE_MAP
        | if ($DEVICE_MAP|type) == "null" then .
          else
            reduce ($DEVICE_MAP | to_entries[] | select(.key != "_doc")) as $DEVICE_ENTRY (.;
              (.attributes[$DEVICE_ENTRY.key] // "" ) as $OLD_VALUE
              | ($OLD_VALUE | tostring) as $OLD_STR
              | ( first(
                    $DEVICE_ENTRY.value[]
                    | . as $RULE_MATCH
                    | ($RULE_MATCH[$SOURCE_SERVER_ID]|tostring) as $SRC_RULE
                    | (($RULE_MATCH.scope == null) or ($OLD_STR | contains($RULE_MATCH.scope))) as $SCOPE_OK
                    | ($OLD_STR == $SRC_RULE) as $EQUALS
                    | ($OLD_STR | contains($SRC_RULE)) as $CONTAINS
                    | select($SCOPE_OK and ($EQUALS or $CONTAINS))
                ) ) as $MATCH
              | if $MATCH != null then
                  ($MATCH[$SOURCE_SERVER_ID]|tostring) as $SRC_RULE
                  | if ($OLD_STR == $SRC_RULE) then
                      .attributes[$DEVICE_ENTRY.key] = $MATCH[$TARGET_SERVER_ID]
                    elif ($OLD_STR | contains($SRC_RULE)) then
                      .attributes[$DEVICE_ENTRY.key] = ($OLD_STR | sub($SRC_RULE; ($MATCH[$TARGET_SERVER_ID]|tostring)))
                    else .
                    end
                else .
                end
            )
          end;

      if .devices then
        .devices |= [ .[] | (apply_device(.) // .) ]
      elif (type == "array") and (.[0].devices) then
        .[0].devices |= [ .[0].devices[] | (apply_device(.) // .) ]
      else
        .
      end
    ' "${SOURCE_VM_JSON_FILE}" > "${TRANSFORMED_VM_JSON_FILE}"

    if [[ $? -ne 0 ]]; then
        echo
        echo "VM Decision Log:"
        printf '%s\n' "${VM_DECISION_LOG}"
        echo
        Background_error "ERROR transforming ${SOURCE_VM_JSON_FILE}"
    fi

    # 5) Validation: prove devices are objects (not null) and dtype exists
    VALID=$(jq -r '
      def devs: (.devices // (.[0].devices));
      if devs then
        (devs | map(type) | all(. == "object")) and
        (devs | all(.attributes and (.attributes.dtype != null)))
      else
        false
      end
    ' "${TRANSFORMED_VM_JSON_FILE}")

    if [[ "${VALID}" != "true" ]]; then
      echo
      cat "${TRANSFORMED_VM_JSON_FILE}"
      echo
      Background_error "ERROR: Transformed JSON failed validation (null/non-object devices)."
    fi

    # 6) Final: publish transformed/destination JSON file
    echo "  truenas-${LOCAL_SERVER_ID} - Transformed source json into ${TRANSFORMED_VM_JSON_FILE}"
    echo
}

# Cleanup all lingering VM disk autobackup tags (pool-wide, scope-only)
function Cleanup_vm_disk_tags() {
    local SOURCE_LOCATION="$1"
    local SERVER_ID_VAR
    local SERVER_ID
    local POOL_NAME
    local TASK_SCOPE
    local DS
    local TAGGED_DATASET_LIST

    SERVER_ID_VAR="${SOURCE_LOCATION^^}_SERVER_ID"
    SERVER_ID="${!SERVER_ID_VAR}"
    POOL_NAME="$(Resolve_pool "${SERVER_ID}" "fast")"
    [[ -n "${POOL_NAME}" ]] || Background_error "ERROR: Failed to resolve pool for server '${SERVER_ID}'."
    TASK_SCOPE="${TASK}_vm_latest_snapshot_only"

    mapfile -t TAGGED_DATASET_LIST < <(Execute_command "${SOURCE_LOCATION}" \
        "zfs get -H -o name,value -t filesystem,volume \"autobackup:${TASK_SCOPE}\" -r \"${POOL_NAME}\" \
         | awk '\$2==\"true\" {print \$1}'")

    if [[ ${#TAGGED_DATASET_LIST[@]} -gt 0 ]]; then
        echo "Cleaning up the 'autobackup'-tag on all datasets..."
        for DS in "${TAGGED_DATASET_LIST[@]}"; do
            if Execute_command "${SOURCE_LOCATION}" "zfs inherit autobackup:${TASK_SCOPE} \"${DS}\""; then
                echo "  'autobackup:${TASK_SCOPE}'-tag removed from '${DS}'"
            else
                Background_error "ERROR: Failed to remove 'autobackup:${TASK_SCOPE}'-tag from ${DS}"
            fi
        done
        echo
    fi
}

# Tag ZFS datasets or ZVOLs explicitly listed in VM definition (per VM, per device)
# Only tag datasets/zvols under <POOL>/encrypted-ds/vm-ds/...
function Tag_vm_disks() {
    local SOURCE_LOCATION="$1"
    local VM="$2"

    local DEVNODE_DISK_PATH
    local REL_DISK_PATH
    local ZFS_DISK_PATH

    local TAGGED=0
    local SERVER_ID_VAR="${SOURCE_LOCATION^^}_SERVER_ID"
    local SERVER_ID="${!SERVER_ID_VAR}"
    local POOL_NAME="$(Resolve_pool "${SERVER_ID}" "fast")"
    [[ -n "${POOL_NAME}" ]] || Background_error "ERROR: Failed to resolve pool for server '${SERVER_ID}'."
    local TASK_SCOPE="${TASK}_vm_latest_snapshot_only"

    mapfile -t DISK_PATHS < <(jq -r --arg VM "$VM" '
    .[] | select(.name==$VM)
    | .devices[]
    | select(.attributes.dtype=="DISK")
    | .attributes.path
    ' <<< "$SOURCE_ALL_VM_JSON")

    # Iterate the array (no stdin involved)
    for DEVNODE_DISK_PATH in "${DISK_PATHS[@]}"; do
        if [[ "${DEVNODE_DISK_PATH}" == "/dev/zvol/${POOL_NAME}/encrypted-ds/vm-ds/"* ]]; then
            REL_DISK_PATH="${DEVNODE_DISK_PATH#/dev/zvol/"${POOL_NAME}"/}"
            ZFS_DISK_PATH="${POOL_NAME}/${REL_DISK_PATH}"

            # Ensure commands don’t read from stdin even if wrappers do
            if Execute_command "${SOURCE_LOCATION}" \
                "zfs list -H \"${ZFS_DISK_PATH}\" >/dev/null 2>&1"; then
                if [[ $TAGGED -eq 0 ]]; then
                    echo "Adding 'autobackup'-tag to the disks of the VM '${VM}'..."
                    TAGGED=1
                fi
                echo "  truenas-${SOURCE_SERVER_ID} - zfs set autobackup:${TASK_SCOPE}=true \"${ZFS_DISK_PATH}\""
                Execute_command "${SOURCE_LOCATION}" \
                    "zfs set autobackup:${TASK_SCOPE}=true \"${ZFS_DISK_PATH}\"" \
                    || Background_error "ERROR: Failed to tag ZVOL ${ZFS_DISK_PATH} for VM '${VM}'"
            else
                Background_error "ERROR: VM '${VM}' refers to non-existent ZVOL ${ZFS_DISK_PATH}"
            fi
        else
            echo "WARNING: VM '${VM}' device path '${DEVNODE_DISK_PATH}' outside encrypted-ds/vm-ds"
        fi
    done

    [[ $TAGGED -eq 1 ]] && echo
}

# Delete an existing VM definition on the destination
function Delete_vm_on_destination() {
    local TARGET_LOCATION="$1"
    local VM="$2"

    local VM_ID

    VM_ID="$(jq -r --arg VM "${VM}" '.[] | select(.name==$VM) | .id' <<< "${TARGET_ALL_VM_JSON}")"
    if [[ -n "${VM_ID}" && "${VM_ID}" != "null" ]]; then
        echo "Deleting VM '${VM}' (id ${VM_ID}) on truenas-${TARGET_SERVER_ID}...${TEST_MODE:+" (Not done because of '--test' usage!)"}"
        echo "  truenas-${TARGET_SERVER_ID} - midclt call vm.delete ${VM_ID}"
        if [[ -z "${TEST_MODE}" ]]; then
            Execute_command "${TARGET_LOCATION}" "midclt call vm.delete ${VM_ID} >/dev/null 2>&1" \
                || Background_error "ERROR: Failed to delete VM '${VM}' on truenas-${TARGET_SERVER_ID}"
        fi
        echo
    fi
}

# Cleanup orphan zvols for a single VM on the destination
function Audit_and_cleanup_vm_storage() {
    local TARGET_LOCATION="$1"
    local VM="$2"

    local -a SOURCE_VM_PATH_LIST
    local -a TARGET_VM_PATH_LIST

    # Extract all device paths (zvols and non‑zvols) for this VM on SOURCE
    mapfile -t SOURCE_VM_PATH_LIST < <(jq -r --arg VM "${VM}" '
        .[] | select(.name==$VM) | .devices
        | to_entries[] | .value.attributes.path
        | select(. != null)
    ' <<< "${SOURCE_ALL_VM_JSON}")

    # Extract all device paths (zvols and non‑zvols) for this VM on TARGET
    mapfile -t TARGET_VM_PATH_LIST < <(jq -r --arg VM "${VM}" '
        .[] | select(.name==$VM) | .devices
        | to_entries[] | .value.attributes.path
        | select(. != null)
    ' <<< "${TARGET_ALL_VM_JSON}")

    local TARGET_VM_PATH
    local REL_SOURCE_VM_PATH
    local -a ORPHAN_PATH_LIST=()
    for TARGET_VM_PATH in "${TARGET_VM_PATH_LIST[@]}"; do
        # skip if target path is present in source (loop element‑wise, space‑safe)
        for REL_SOURCE_VM_PATH in "${SOURCE_VM_PATH_LIST[@]#*/${SOURCE_POOL}/}"; do
            [[ "${REL_SOURCE_VM_PATH}" == "${TARGET_VM_PATH#*/"${TARGET_POOL}"/}" ]] && continue 2   # jump to next TARGET_VM_PATH
        done

        # skip if more than one VM on target references it
        [[ "$(jq --arg TARGET_VM_PATH "${TARGET_VM_PATH}" '[ .[] | .devices | to_entries[] | select(.value.attributes.path==$TARGET_VM_PATH) ] | length' <<< "${TARGET_ALL_VM_JSON}")" -ne "1" ]] && continue

        # skip if not under encrypted-ds/vm-ds
        [[ "${TARGET_VM_PATH}" == *"/encrypted-ds/vm-ds/"* ]] || continue

        local SOURCE_VM_PATH="${TARGET_VM_PATH/${TARGET_POOL}/${SOURCE_POOL}}"
        case "${TARGET_VM_PATH}" in
            /mnt/*)
                ! Execute_command "${SOURCE_LOCATION}" "test -f '${SOURCE_VM_PATH}'"                                 && ORPHAN_PATH_LIST+=( "${TARGET_VM_PATH}" )
                ;;
            /dev/zvol/*)
                ! Execute_command "${SOURCE_LOCATION}" "zfs list -H '${SOURCE_VM_PATH#/dev/zvol/}' >/dev/null 2>&1"   && ORPHAN_PATH_LIST+=( "${TARGET_VM_PATH}" )
                ;;
            *)
                continue
                ;;
        esac
    done

    local ORPHAN_PATH
    if [[ "${#ORPHAN_PATH_LIST[@]}" -gt 0 ]]; then
        echo "Cleaning up orphan paths for VM '${VM}':${TEST_MODE:+" (Not done because of '--test' usage!)"}"
        for ORPHAN_PATH in "${ORPHAN_PATH_LIST[@]}"; do
            case "${ORPHAN_PATH}" in
                /mnt/*)
                    echo "  truenas-${TARGET_SERVER_ID} - rm -f ${ORPHAN_PATH}"
                    if [[ -z "${TEST_MODE}" ]]; then
                        Execute_command "${TARGET_LOCATION}" "rm -f \"${ORPHAN_PATH}\"" \
                            || Background_error "ERROR: Failed to remove orphan file '${ORPHAN_PATH}'"
                    fi
                    ;;
                /dev/zvol/*)
                    echo "  truenas-${TARGET_SERVER_ID} - zfs destroy -r ${ORPHAN_PATH#/dev/zvol/}"
                    if [[ -z "${TEST_MODE}" ]]; then
                        Execute_command "${TARGET_LOCATION}" "zfs destroy -r \"${ORPHAN_PATH#/dev/zvol/}\"" \
                            || Background_error "ERROR: Failed to destroy orphan zvol '${ORPHAN_PATH#/dev/zvol/}'"
                    fi
                    ;;
            esac
        done
        echo
    fi
}

# Rsync file-backed VM disks (qcow2/raw images) and ISOs (per VM, per file)
function Rsync_vm_file_disks() {
    local SOURCE_LOCATION="$1"
    local VM="$2"

    local DEV_ID
    local SOURCE_PATH
    local TARGET_PATH
    local RSYNC_SOURCE
    local RSYNC_TARGET

    local RSYNCED=0
    local FILE_DEVICE_LIST
    local FILE_DEVICE

    # Collect RAW and CDROM devices (file-backed)
    mapfile -t FILE_DEVICE_LIST < <(jq -r --arg VM "${VM}" '
        .[] | select(.name==$VM) | .devices[]
        | select(.attributes.dtype=="RAW" or .attributes.dtype=="CDROM")
        | "\(.id) \(.attributes.path)"
    ' <<< "${SOURCE_ALL_VM_JSON}")

    for FILE_DEVICE in "${FILE_DEVICE_LIST[@]}"; do
        DEV_ID="${FILE_DEVICE%% *}"
        SOURCE_PATH="${FILE_DEVICE#* }"

        # Only rsync if the file is inside vm-ds
        if [[ "${SOURCE_PATH}" == "/mnt/${SOURCE_POOL}/encrypted-ds/vm-ds/"* ]]; then
            # Must be a file
            Execute_command "${SOURCE_LOCATION}" "test -f \"${SOURCE_PATH}\"" || continue

            # Find the corresponding target path by device id
            TARGET_PATH="$(jq -r --argjson DEV_ID "${DEV_ID}" '
                .devices[]
                | select(.id==$DEV_ID)
                | .attributes.path
            ' "${TRANSFORMED_VM_JSON_FILE}")"

            if [[ -z "${TARGET_PATH}" || "${TARGET_PATH}" == "null" ]]; then
                Background_error "ERROR: No matching target path for device id=${DEV_ID} in VM '${VM}'"
                continue
            fi

            # Default specs
            RSYNC_SOURCE="${SOURCE_PATH}"
            RSYNC_TARGET="${TARGET_PATH}"

            # Add remote prefix depending on direction
            case "${TASK}:${LOCAL_SERVER_ID}" in
                backup_to_master:master|master_to_backup:backup)
                    RSYNC_SOURCE="truenas-${REMOTE_SERVER_ID}:${SOURCE_PATH}"
                    ;;
                backup_to_master:backup|master_to_backup:master)
                    RSYNC_TARGET="truenas-${REMOTE_SERVER_ID}:${TARGET_PATH}"
                    ;;
                *)
                    Background_error "ERROR: Unrecognized TASK/server role when building rsync specs for VM '${VM}'"
                    continue
                    ;;
            esac

            # Ensure target directory exists
            Execute_command "${TARGET_LOCATION}${TEST_MODE:+"_test"}" "mkdir -p \"${TARGET_PATH%/*}\"" \
                || Background_error "ERROR: Failed to create target dir ${TARGET_PATH%/*} for VM '${VM}'"

            # Print general header once
            if [[ ${RSYNCED} -eq 0 ]]; then
                echo "Rsyncing file-backed disks/ISOs for VM '${VM}'...${TEST_MODE:+" (Not done because of '--test' usage!)"}"
                RSYNCED=1
            fi

            # Perform rsync (single file)
            echo "  ${SOURCE_PATH} → ${TARGET_PATH}"
            if rsync ${TEST_MODE:+--dry-run} -e "ssh -F ${SSH_CONFIG_FILE}" --delete -aHX \
                "${RSYNC_SOURCE}" "${RSYNC_TARGET}"; then
                :
            else
                Background_error "ERROR: Rsync of device id=${DEV_ID} ('${SOURCE_PATH}') failed for VM '${VM}'"
            fi
        fi
    done
    [[ ${RSYNCED} -eq 1 ]] && echo
}

# Replicate one VM’s datasets/zvols
function Replicate_vm() {
    local SOURCE_LOCATION="$1"
    local VM="$2"

    # Scope your replication runner to this VM only.
    Perform_filesystem_replication "vm_latest_snapshot_only" "fast"
}

# Verify zvols exist and recreate VM on destination
function Verify_and_recreate_vm() {
    local TARGET_LOCATION="$1"
    local VM="$2"

    local MISSING=()
    local DISK_PATH_LIST
    local DEVICE_LIST

    # Verify all storage-bearing devices exist: DISK (zvol), RAW (file), CDROM (ISO)
    mapfile -t DISK_PATH_LIST < <(jq -r '
        .devices | to_entries[]
        | select((.value.dtype // .value.attributes.dtype)=="DISK"
              or (.value.dtype // .value.attributes.dtype)=="RAW"
              or (.value.dtype // .value.attributes.dtype)=="CDROM")
        | .value.attributes.path
    ' "${TRANSFORMED_VM_JSON_FILE}")

    for DISK_PATH in "${DISK_PATH_LIST[@]}"; do
        local REL_DISK_PATH=""
        if [[ "${DISK_PATH}" =~ ^/dev/zvol/ ]]; then
            # DISK: ZVOL must exist on target
            REL_DISK_PATH="${DISK_PATH#/dev/zvol/}"   # pool/dataset/...
            Execute_command "${TARGET_LOCATION}" "zfs list -H \"${REL_DISK_PATH}\" &>/dev/null" \
                || MISSING+=( "${REL_DISK_PATH}" )

        elif [[ "${DISK_PATH}" =~ ^/mnt/ ]]; then
            # RAW/CDROM: any file path under /mnt must exist (replication handled elsewhere)
            Execute_command "${TARGET_LOCATION}" "test -f \"${DISK_PATH}\"" \
                || MISSING+=( "${DISK_PATH}" )

        else
            # RAW: Raw block device path (e.g., /dev/sdX) or other: ensure block device exists locally
            if [[ -b "${DISK_PATH}" ]]; then
                :
            else
                MISSING+=( "${DISK_PATH}" )
            fi
        fi
    done

    if [[ "${#MISSING[@]}" -gt 0 ]]; then
        echo "Failed to create VM '${VM}': missing storage: ${MISSING[*]}"
        echo
        return 1
    fi

    # Create the VM shell
    echo "Creating VM '${VM}' on truenas-${TARGET_SERVER_ID} from json...${TEST_MODE:+" (Not done because of '--test' usage!)"} "
    local SHELL_JSON
    SHELL_JSON="$(jq -c 'del(.id, .status, .display_available, .devices)' "${TRANSFORMED_VM_JSON_FILE}")"

    local CREATE_OUT
    if ! CREATE_OUT="$(Execute_command "${TARGET_LOCATION}${TEST_MODE:+"_test"}" "midclt call vm.create '${SHELL_JSON}'")"; then
        echo "Failed."
        return 1
    fi

    local VM_ID
    VM_ID="$(jq -r '.id' <<<"${CREATE_OUT}")"
    echo "  Created VM shell for '${VM}' with id=${VM_ID}"

    # Add devices one by one
    mapfile -t DEVICE_LIST < <(jq -c '
        .devices | to_entries[]
        | {
            dtype: (.value.dtype // .value.attributes.dtype),
            attributes: (
                .value.attributes
                | del(.id, .vm, .order)
            )
        }
    ' "${TRANSFORMED_VM_JSON_FILE}")

    for DEVICE in "${DEVICE_LIST[@]}"; do
        local DTYPE ATTRS PAYLOAD
        DTYPE="$(jq -r '.dtype' <<<"${DEVICE}")"

        if [[ "${DTYPE}" == "RAW" ]]; then
            # RAW: build attributes and force exists:true
            ATTRS="$(jq -c '.attributes | del(.exists) + {exists:true}' <<<"${DEVICE}")"
        else
            # All other types: just take attributes as-is
            ATTRS="$(jq -c '.attributes' <<<"${DEVICE}")"
        fi

        if [[ -z "${TEST_MODE}" ]]; then
            PAYLOAD="$(jq -n \
                --argjson attrs "${ATTRS}" \
                --arg dtype "${DTYPE}" \
                --argjson vm "${VM_ID}" \
                '{vm: $vm|tonumber, attributes: ($attrs + {dtype: $dtype})}')"
        fi

        echo -n "  Adding ${DTYPE} device... "
        local CREATE_OUT
        if CREATE_OUT="$(Execute_command "${TARGET_LOCATION}${TEST_MODE:+"_test"}" "midclt call vm.device.create '${PAYLOAD}' 2>&1")"; then
            echo "Done."
        else
            echo -e "Failed to add ${DTYPE} (path=$(jq -r '.path // empty' <<<"${ATTRS}"))\n\n${CREATE_OUT}\n"
            return 1
        fi
    done
    echo
}

#######################################
# High-level orchestrator
#######################################

function Perform_vm_replication() {
    local SOURCE_LOCATION
    local TARGET_LOCATION
    local SOURCE_POOL
    local TARGET_POOL
    local SOURCE_SERVER_ID
    local TARGET_SERVER_ID

    local SUCCEEDED=0
    local FAILED=0
    local NOTFOUND=0

    local SOURCE_VM_JSON_FILE
    local TARGET_VM_JSON_FILE
    local TRANSFORMED_VM_JSON_FILE

    local PROCESSED_VM_LIST=()
    local VM_LIST_FROM_SOURCE
    local VM

    local SOURCE_VM_ID SOURCE_VM_STATE TARGET_VM_ID TARGET_VM_STATE
    local SOURCE_STOPPED_BY_US TARGET_WAS_RUNNING

    echo "#################################"
    echo "### Performing VM Replication ###"
    echo "#################################"
    echo

    # Prepare vars
    [[ -n "${LOCAL_SOURCE}" ]] && SOURCE_LOCATION="local" || SOURCE_LOCATION="remote"
    [[ -n "${LOCAL_TARGET}" ]] && TARGET_LOCATION="local" || TARGET_LOCATION="remote"

    if [[ "${SOURCE_LOCATION}" == "local" ]]; then
        SOURCE_POOL="$(Resolve_pool "${LOCAL_SERVER_ID}" "fast")"
        SOURCE_SERVER_ID="${LOCAL_SERVER_ID}"
        TARGET_POOL="$(Resolve_pool "${REMOTE_SERVER_ID}" "fast")"
        TARGET_SERVER_ID="${REMOTE_SERVER_ID}"
    else
        SOURCE_POOL="$(Resolve_pool "${REMOTE_SERVER_ID}" "fast")"
        SOURCE_SERVER_ID="${REMOTE_SERVER_ID}"
        TARGET_POOL="$(Resolve_pool "${LOCAL_SERVER_ID}" "fast")"
        TARGET_SERVER_ID="${LOCAL_SERVER_ID}"
    fi

    [[ -n "${SOURCE_POOL}" && -n "${TARGET_POOL}" ]] || Background_error "ERROR: Failed to resolve source/target pool (source='${SOURCE_POOL}', target='${TARGET_POOL}')."

    # Extract VM definitions into JSONs
    Extract_vm_definitions

    # === Loop through each source VM ===
    mapfile -t VM_LIST_FROM_SOURCE < <(jq -r '.[].name' <<< "${SOURCE_ALL_VM_JSON}")
    for VM in "${VM_LIST_FROM_SOURCE[@]}"; do
        Vm_in_scope "SOURCE" "${VM}" || continue
        PROCESSED_VM_LIST+=("${VM}")

        echo
        echo "### Replicating VM: ${VM} ###"
        echo

        SOURCE_VM_JSON_FILE="${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${SOURCE_SERVER_ID}.source.json"
        TARGET_VM_JSON_FILE="${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${TARGET_SERVER_ID}.target.json"
        TRANSFORMED_VM_JSON_FILE="${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${TARGET_SERVER_ID}.transformed.json"

        if [[ ! -f "${SOURCE_VM_JSON_FILE}" ]]; then
            echo "Missing per‑VM JSON: ${SOURCE_VM_JSON_FILE}"
            echo
            ((FAILED++))
            continue
        fi

        # a. Determine source/target power state (and ids, for optional stop/start).
        SOURCE_STOPPED_BY_US="false"
        TARGET_WAS_RUNNING="false"
        SOURCE_VM_ID="$(jq -r --arg VM "${VM}" '.[] | select(.name==$VM) | .id' <<< "${SOURCE_ALL_VM_JSON}")"
        SOURCE_VM_STATE="$(jq -r --arg VM "${VM}" '.[] | select(.name==$VM) | .status.state' <<< "${SOURCE_ALL_VM_JSON}")"
        TARGET_VM_ID="$(jq -r --arg VM "${VM}" '.[] | select(.name==$VM) | .id' <<< "${TARGET_ALL_VM_JSON}")"
        TARGET_VM_STATE="$(jq -r --arg VM "${VM}" '.[] | select(.name==$VM) | .status.state' <<< "${TARGET_ALL_VM_JSON}")"

        # The source must be STOPPED to snapshot it cleanly. If running:
        #   --stop-running-vms -> stop now, restart at the end / on abort (source is only read,
        #                         so restoring its running state is always safe)
        #   otherwise          -> skip this VM (default behaviour)
        if [[ "${SOURCE_VM_STATE}" != "STOPPED" ]]; then
            if [[ -z "${STOP_RUNNING_VMS}" ]]; then
                echo "Replication skipped for VM ${VM} (source state='${SOURCE_VM_STATE}'; pass --stop-running-vms to stop it automatically)"
                echo
                ((FAILED++))
                continue
            fi
            # Register the restore BEFORE stopping, so even a timed-out stop still tries to restart it.
            # shellcheck disable=SC2016  # single-quoted on purpose: expand at cleanup (fire) time
            Register_cleanup 'Control_vm "${VM}" "${SOURCE_VM_ID}" start "${SOURCE_LOCATION}" "${SOURCE_SERVER_ID}"'
            Control_vm "${VM}" "${SOURCE_VM_ID}" stop "${SOURCE_LOCATION}" "${SOURCE_SERVER_ID}"
            SOURCE_STOPPED_BY_US="true"
        fi

        # The target may legitimately be RUNNING (dual-active); it must be STOPPED before
        # Delete_vm_on_destination. Same opt-in policy as the source, but its old instance is
        # deliberately destroyed/rebuilt, so on abort we only WARN (below) — never auto-start.
        if [[ -n "${TARGET_VM_STATE}" && "${TARGET_VM_STATE}" != "STOPPED" ]]; then
            if [[ -z "${STOP_RUNNING_VMS}" ]]; then
                # (Reachable only when the source was already stopped, so nothing to restore here.)
                echo "Replication skipped for VM ${VM} (target state='${TARGET_VM_STATE}'; pass --stop-running-vms to stop it automatically)"
                echo
                ((FAILED++))
                continue
            fi
            TARGET_WAS_RUNNING="true"
        fi

        Transform_vm_definition "${SOURCE_SERVER_ID}" "${TARGET_SERVER_ID}"
        Cleanup_vm_disk_tags "${SOURCE_LOCATION}"
        Tag_vm_disks "${SOURCE_LOCATION}" "${VM}"

        # Stop the running target just before deleting it (minimal stopped window). Register
        # the warn-on-abort HERE, not at detection, so an abort earlier (target untouched)
        # doesn't warn misleadingly. (Control_vm stops gracefully — see its header for why.)
        if [[ "${TARGET_WAS_RUNNING}" == "true" ]]; then
            # shellcheck disable=SC2016  # single-quoted on purpose: expand at cleanup (fire) time
            Register_cleanup 'Warn_target_vm_state "${VM}" "${TARGET_SERVER_ID}"'
            Control_vm "${VM}" "${TARGET_VM_ID}" stop "${TARGET_LOCATION}" "${TARGET_SERVER_ID}"
        fi
        Delete_vm_on_destination "${TARGET_LOCATION}" "${VM}"
        Audit_and_cleanup_vm_storage "${TARGET_LOCATION}" "${VM}"
        Rsync_vm_file_disks "${SOURCE_LOCATION}" "${VM}"
        Replicate_vm "${SOURCE_LOCATION}" "${VM}"
        Cleanup_vm_disk_tags "${SOURCE_LOCATION}"

        if Verify_and_recreate_vm "${TARGET_LOCATION}" "${VM}"; then
            # The rebuilt target is created STOPPED; start it only if the previous target
            # instance was running when we began. Re-query for its new id first.
            if [[ "${TARGET_WAS_RUNNING}" == "true" ]]; then
                TARGET_VM_ID="$(jq -r --arg VM "${VM}" '.[] | select(.name==$VM) | .id' <<< "$(Execute_command "${TARGET_LOCATION}" "midclt call vm.query")")"
                Control_vm "${VM}" "${TARGET_VM_ID}" start "${TARGET_LOCATION}" "${TARGET_SERVER_ID}"
                Unregister_cleanup   # target restored -> drop the warn-on-abort
            fi
            ((SUCCEEDED++))
        else
            ((FAILED++))
            # Graceful per-VM failure (no script abort): the target is left rebuilt-but-stopped.
            # Clear the warn-on-abort and note the left-stopped state plainly instead.
            if [[ "${TARGET_WAS_RUNNING}" == "true" ]]; then
                Unregister_cleanup
                echo "NOTE: VM '${VM}' was running on truenas-${TARGET_SERVER_ID} before this run, but its target rebuild did not complete; it is left stopped."
            fi
            echo
        fi

        # Restore the source we stopped for this VM (clean path). On a script abort the
        # cleanup stack handles this instead.
        if [[ "${SOURCE_STOPPED_BY_US}" == "true" ]]; then
            Control_vm "${VM}" "${SOURCE_VM_ID}" start "${SOURCE_LOCATION}" "${SOURCE_SERVER_ID}"
            Unregister_cleanup
        fi
    done

    # Post-loop: check if all requested VMs were actually processed
    if [[ "${#VM_LIST[@]}" -gt 0 ]]; then
        for REQ in "${VM_LIST[@]}"; do
            # shellcheck disable=SC2076  # deliberate literal-substring match, not regex
            if [[ ! " ${PROCESSED_VM_LIST[*]} " =~ " ${REQ} " ]]; then
                echo "Requested VM '${REQ}' not found on source"
                ((NOTFOUND++))
            fi
        done
    fi

    echo "### VM Replication Summary: ${SUCCEEDED} succeeded, ${FAILED} failed, ${NOTFOUND} not found ###"
    echo
}