#!/usr/bin/bash
# lib/vms.bash
# Virtual Machine replication orchestration

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

    if [[ "${SOURCE_OR_TARGET}" == "SOURCE" ]]; then
        if [[ "${#VM_LIST[@]}" -eq 0 ]]; then
            return 0
        else
            [[ " ${VM_LIST[*]} " =~ " ${VM} " ]]
            return $?
        fi
    elif [[ "${SOURCE_OR_TARGET}" == "TARGET" ]]; then
        [[ " ${SOURCE_EXTRACTED_VM_LIST[*]} " =~ " ${VM} " ]]
        return $?
    fi
}

# Validate that a VM is STOPPED
function Vm_is_stopped() {
    local SOURCE_OR_TARGET="$1"
    local VM="$2"

    local JSON_VAR="${SOURCE_OR_TARGET^^}_ALL_VM_JSON"
    local JSON="${!JSON_VAR}"

    local STATE
    STATE="$(jq -r --arg VM "${VM}" '.[] | select(.name == $VM) | .status.state' <<< "${JSON}")"

    if [[ "${STATE}" != "STOPPED" ]]; then
        echo "Replication skipped for VM ${VM} (state='${STATE}')"
        return 1
    fi
}

####################################
# Tasks for all VMs simultaneously #
####################################

# Extract VM definitions into a variable as JSON
function Extract_vm_definitions() {
    echo "### Extracting VM definitions ###"

    # Remove old jsons
    rm -f "${VM_TMP_DIR}/json/per_vm/"*.json 2>/dev/null

    for SOURCE_OR_TARGET in SOURCE TARGET; do
        local LOCATION SERVER_ID_VAR SERVER_ID ALL_VM_JSON
        local -n SOURCE_OR_TARGET_ALL_VM_JSON_REF="${SOURCE_OR_TARGET}_ALL_VM_JSON"

        LOCATION="${!SOURCE_OR_TARGET}"
        SERVER_ID_VAR="${LOCATION^^}_SERVER_ID"
        SERVER_ID="${!SERVER_ID_VAR}"

        ALL_VM_JSON="$(Execute_command "${LOCATION}" "midclt call vm.query" 2>/dev/null)" \
            || Background_error "ERROR: truenas-${SERVER_ID} - Failed to extract VM definitions"

        SOURCE_OR_TARGET_ALL_VM_JSON_REF="${ALL_VM_JSON}"

        if jq -e 'length == 0' <<<"${ALL_VM_JSON}" >/dev/null; then
            echo "truenas-${SERVER_ID} - No VMs found"
            continue
        fi

        while IFS= read -r VM; do
            if ! Vm_in_scope "${SOURCE_OR_TARGET}" "${VM}"; then
                echo "truenas-${SERVER_ID} - VM '${VM}' not in scope for replication"
                continue
            fi

            jq --arg VM "${VM}" '.[] | select(.name == $VM)' <<<"${ALL_VM_JSON}" \
                > "${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${SERVER_ID}.${SOURCE_OR_TARGET,,}.json"

            echo "truenas-${SERVER_ID} - Extracted ${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${SERVER_ID}.${SOURCE_OR_TARGET,,}.json"

            # Record SOURCE VMs for later TARGET filtering
            [[ "${SOURCE_OR_TARGET}" == "SOURCE" ]] && SOURCE_EXTRACTED_VM_LIST+=("${VM}")
        done < <(jq -r '.[].name' <<<"${ALL_VM_JSON}")
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
        | . as $DEV
        | $DEV.attributes.dtype as $DTYPE
        | ($MAP_FILE[$DTYPE] // null) as $DMAP
        | "DEC: DTYPE=" + ($DTYPE // "null")
            + " MAP_PRESENT=" + (((($DMAP|type) != "null"))|tostring)
        ,
        ( if ($DMAP|type) != "null" then
            $DMAP
            | to_entries[]
            | select(.key != "_doc")
            | .key as $ATTR
            | .value[] as $RULE
            | ($DEV.attributes[$ATTR] // empty) as $SOURCE_VALUE
            | ($SOURCE_VALUE | tostring) as $SOURCE_VALUES
            | ( ($RULE.scope == null) or ($SOURCE_VALUES | contains($RULE.scope)) ) as $SCOPE_MATCH
            | ( $SOURCE_VALUES == ($RULE[$SOURCE_SERVER_ID] // "") ) as $SOURCE_EQUALS_RULE
            | ( $SOURCE_VALUES | contains($RULE[$SOURCE_SERVER_ID] // "") ) as $SOURCE_CONTAINS_RULE
            | "  dtype=" + $DTYPE
            + " attribute=" + $ATTR
            + " source_value=" + ($SOURCE_VALUE|tostring)
            + " source_rule=" + (($RULE[$SOURCE_SERVER_ID] // "null")|tostring)
            + " target_rule=" + (($RULE[$TARGET_SERVER_ID] // "null")|tostring)
            + " scope=" + (($RULE.scope // "null")|tostring)
            + " | scope_match=" + ($SCOPE_MATCH|tostring)
            + " source_equals_rule=" + ($SOURCE_EQUALS_RULE|tostring)
            + " source_contains_rule=" + ($SOURCE_CONTAINS_RULE|tostring)
            + " apply=" + (( $SCOPE_MATCH and ( $SOURCE_EQUALS_RULE or $SOURCE_CONTAINS_RULE ) )|tostring)
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
            ($DEVICE.attributes.path // empty) as $DISK_PATH
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
            | .value[] as $RULE
            | ($DEVICE.attributes[$ATTR] // empty) as $SOURCE_VALUE
            | ($SOURCE_VALUE | tostring) as $SOURCE_STR
            | if (($RULE.scope == null) or ($SOURCE_STR | contains($RULE.scope))) then
                if ($SOURCE_STR == $RULE[$SOURCE_SERVER_ID]) then
                  "\($DTYPE)\t\($ATTR)\t\($SOURCE_VALUE) → \($RULE[$TARGET_SERVER_ID])"
                elif ($SOURCE_STR | contains($RULE[$SOURCE_SERVER_ID])) then
                  "\($DTYPE)\t\($ATTR)\t\($SOURCE_VALUE) → \($SOURCE_STR | sub($RULE[$SOURCE_SERVER_ID]; $RULE[$TARGET_SERVER_ID]))"
                else empty end
              else empty end
        )
      end
    ' "${SOURCE_VM_JSON_FILE}" | column -t -s $'\t' | sed 's/^/  /'

    # 3) Apply JSON mappings and write to $TRANSFORMED_VM_JSON_FILE
    jq --arg SOURCE_SERVER_ID "${SOURCE_SERVER_ID}" --arg TARGET_SERVER_ID "${TARGET_SERVER_ID}" --argfile MAP_FILE "${MAP_FILE}" '
      def apply_device($dev):
        $dev
        | .attributes.dtype as $dtype
        | ($MAP_FILE[$dtype] // null) as $dmap
        | if ($dmap|type) == "null" then .
          else
            reduce ($dmap | to_entries[] | select(.key != "_doc")) as $e (.;
              reduce ($e.value[]) as $rule (.;
                (.attributes[$e.key] // empty) as $old
                | if $old == "" then .
                  else
                    ($old|tostring) as $olds
                    | if (($rule.scope == null) or ($olds|contains($rule.scope))) then
                        if ($olds == $rule[$SOURCE_SERVER_ID]) then
                          .attributes[$e.key] = $rule[$TARGET_SERVER_ID]
                        elif ($olds|contains($rule[$SOURCE_SERVER_ID])) then
                          .attributes[$e.key] = ($olds|sub($rule[$SOURCE_SERVER_ID];$rule[$TARGET_SERVER_ID]))
                        else .
                        end
                      else .
                      end
                  end
              )
            )
          end;

      if .devices then
        .devices |= map(apply_device(.))
      elif (type == "array") and (.[0].devices) then
        .[0].devices |= map(apply_device(.))
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
    echo "  truenas-${TARGET_SERVER_ID} - Transformed ${TRANSFORMED_VM_JSON_FILE}"
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
    local TAGGED_DATASETS

    SERVER_ID_VAR="${SOURCE_LOCATION^^}_SERVER_ID"
    SERVER_ID="${!SERVER_ID_VAR}"
    POOL_NAME="$(Resolve_pool "${SERVER_ID}" "fast")"
    TASK_SCOPE="${TASK}_vm_latest_snapshot_only"

    # Collect all datasets with the tag
    TAGGED_DATASETS="$(Execute_command "${SOURCE_LOCATION}" \
        "zfs get -H -o name,value -t filesystem,volume \"autobackup:${TASK_SCOPE}\" -r \"${POOL_NAME}\" \
         | awk '\$2==\"true\" {print \$1}'")"

    if [[ -n "${TAGGED_DATASETS}" ]]; then
        echo "Cleaning up the 'autobackup'-tag on all datasets..."
        while IFS= read -r DS; do
            Execute_command "${SOURCE_LOCATION}" "zfs inherit autobackup:${TASK_SCOPE} \"${DS}\"" \
                && echo "  'autobackup:${TASK_SCOPE}'-tag removed from '${DS}'" \
                || Background_error "ERROR: Failed to remove 'autobackup:${TASK_SCOPE}'-tag from ${DS}"
        done <<< "${TAGGED_DATASETS}"
        echo
    fi
}

# Tag ZFS datasets or ZVOLs explicitly listed in VM definition (per VM, per device)
# Only tag datasets/zvols under <POOL>/encrypted-ds/vm-ds/...
function Tag_vm_disks() {
    local SOURCE_LOCATION="$1"
    local VM="$2"

    local DEVICE_PATH
    local REL
    local FULL

    local TAGGED=0
    local SERVER_ID_VAR="${SOURCE_LOCATION^^}_SERVER_ID"
    local SERVER_ID="${!SERVER_ID_VAR}"
    local POOL_NAME="$(Resolve_pool "${SERVER_ID}" "fast")"
    local TASK_SCOPE="${TASK}_vm_latest_snapshot_only"

    # Collect all DISK device paths for this VM
    while IFS= read -r DEVICE_PATH; do
        if [[ "${DEVICE_PATH}" == "/dev/zvol/${POOL_NAME}/encrypted-ds/vm-ds/"* ]]; then
            REL="${DEVICE_PATH#/dev/zvol/${POOL_NAME}/}"
            FULL="${POOL_NAME}/${REL}"
            if Execute_command "${SOURCE_LOCATION}" "zfs list -H \"${FULL}\" >/dev/null 2>&1"; then
                if [[ $TAGGED -eq 0 ]]; then
                    echo "Adding 'autobackup'-tag to the disks of the VM '${VM}'..."
                    TAGGED=1
                fi
                Execute_command "${SOURCE_LOCATION}" "zfs set autobackup:${TASK_SCOPE}=true \"${FULL}\"" \
                    || Background_error "ERROR: Failed to tag ZVOL ${FULL} for VM '${VM}'"
                echo "  'autobackup:${TASK_SCOPE}'-tag added to '${DEVICE_PATH}'."
            else
                Background_error "ERROR: VM '${VM}' refers to non-existent ZVOL ${FULL}"
            fi

        elif [[ "${DEVICE_PATH}" == "/mnt/${POOL_NAME}/encrypted-ds/vm-ds/"* ]]; then
            REL="${DEVICE_PATH#/mnt/${POOL_NAME}/}"
            FULL="${POOL_NAME}/${REL}"
            if Execute_command "${SOURCE_LOCATION}" "zfs list -H \"${FULL}\" >/dev/null 2>&1"; then
                if [[ $TAGGED -eq 0 ]]; then
                    echo "Adding 'autobackup'-tag to the disks of the VM '${VM}'..."
                    TAGGED=1
                fi
                Execute_command "${SOURCE_LOCATION}" "zfs set autobackup:${TASK_SCOPE}=true \"${FULL}\"" \
                    || Background_error "ERROR: Failed to tag dataset ${FULL} for VM '${VM}'"
                echo "  'autobackup:${TASK_SCOPE}'-tag added to '${DEVICE_PATH}'."
            elif Execute_command "${SOURCE_LOCATION}" "test -f \"${DEVICE_PATH}\""; then
                :
            else
                Background_error "ERROR: VM '${VM}' path '${DEVICE_PATH}' is not a dataset or file."
            fi

        else
            echo "WARNING: VM '${VM}' device path '${DEVICE_PATH}' outside encrypted-ds/vm-ds"
        fi
    done < <(jq -r --arg VM "${VM}" '
        .[] | select(.name==$VM) | .devices
        | to_entries[]
        | select(.value.attributes.dtype=="DISK")
        | .value.attributes.path
    ' <<< "${SOURCE_ALL_VM_JSON}")

    [[ $TAGGED -eq 1 ]] && echo
}

# Delete an existing VM definition on the destination
function Delete_vm_on_destination() {
    local TARGET_LOCATION="$1"
    local VM="$2"

    local VM_ID

    VM_ID="$(jq -r --arg VM "${VM}" '.[] | select(.name==$VM) | .id' <<< "${TARGET_ALL_VM_JSON}")"
    if [[ -n "${VM_ID}" && "${VM_ID}" != "null" ]]; then
        echo "Deleting VM '${VM}' (id ${VM_ID}) on truenas-${TARGET_SERVER_ID}..."
        if [[ -z "${TEST_MODE}" ]]; then
            echo "  ${TARGET_LOCATION^} execute of: midclt call vm.delete ${VM_ID}"
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

    local -a SOURCE_VM_ZVOL_LIST
    local -a TARGET_VM_ZVOL_LIST
    local -a SOURCE_VM_PATH_LIST
    local -a TARGET_VM_PATH_LIST

    # Extract zvols for this VM on SOURCE
    mapfile -t SOURCE_VM_ZVOL_LIST < <(jq -r --arg VM "${VM}" --arg POOL "${SOURCE_POOL}" '
        .[] | select(.name==$VM) | .devices
        | to_entries[] | select(.value.attributes.dtype=="DISK")
        | .value.attributes.path
        | select(startswith("/dev/zvol/" + $POOL + "/encrypted-ds/vm-ds/"))
        | sub("^/dev/zvol/" + $POOL + "/encrypted-ds/vm-ds/"; "")
    ' <<< "${SOURCE_ALL_VM_JSON}")

    # Extract zvols for this VM on TARGET
    mapfile -t TARGET_VM_ZVOL_LIST < <(jq -r --arg VM "${VM}" --arg POOL "${TARGET_POOL}" '
        .[] | select(.name==$VM) | .devices
        | to_entries[] | select(.value.attributes.dtype=="DISK")
        | .value.attributes.path
        | select(startswith("/dev/zvol/" + $POOL + "/encrypted-ds/vm-ds/"))
        | sub("^/dev/zvol/" + $POOL + "/encrypted-ds/vm-ds/"; "")
    ' <<< "${TARGET_ALL_VM_JSON}")

    # Extract all device paths (zvols and non‑zvols) for this VM
    mapfile -t SOURCE_VM_PATH_LIST < <(jq -r --arg VM "${VM}" '
        .[] | select(.name==$VM) | .devices
        | to_entries[] | .value.attributes.path
    ' <<< "${SOURCE_ALL_VM_JSON}")

    mapfile -t TARGET_VM_PATH_LIST < <(jq -r --arg VM "${VM}" '
        .[] | select(.name==$VM) | .devices
        | to_entries[] | .value.attributes.path
    ' <<< "${TARGET_ALL_VM_JSON}")

    # --- Orphan zvol cleanup ---
    local ZVOL_REL_PATH
    local ZVOL_FULL_PATH
    local -a ORPHAN_ZVOL_LIST=()

    for ZVOL_REL_PATH in "${TARGET_VM_ZVOL_LIST[@]}"; do
        if [[ ! " ${SOURCE_VM_ZVOL_LIST[*]} " =~ " ${ZVOL_REL_PATH} " ]]; then
            local REF_COUNT
            REF_COUNT=$(jq -r --arg REL "${ZVOL_REL_PATH}" --arg POOL "${TARGET_POOL}" '
                [ .[] | .devices
                  | to_entries[]
                  | select(.value.attributes.dtype=="DISK")
                  | .value.attributes.path
                  | select(startswith("/dev/zvol/" + $POOL + "/encrypted-ds/vm-ds/"))
                  | sub("^/dev/zvol/" + $POOL + "/encrypted-ds/vm-ds/"; "")
                  | select(.==$REL)
                ] | length
            ' <<< "${TARGET_ALL_VM_JSON}")

            if [[ "${REF_COUNT}" -eq 1 ]]; then
                ORPHAN_ZVOL_LIST+=( "${ZVOL_REL_PATH}" )
            fi
        fi
    done

    if [[ "${#ORPHAN_ZVOL_LIST[@]}" -gt 0 ]]; then
        echo "Cleaning up orphan zvols for VM '${VM}':"
        for ZVOL_REL_PATH in "${ORPHAN_ZVOL_LIST[@]}"; do
            ZVOL_FULL_PATH="${TARGET_POOL}/encrypted-ds/vm-ds/${ZVOL_REL_PATH}"
            echo "  Removing zvol: ${ZVOL_FULL_PATH}"
            if [[ -z "${TEST_MODE}" ]]; then
                echo "Execute_command ${TARGET_LOCATION} zfs destroy -r ${ZVOL_FULL_PATH}"
#                Execute_command "${TARGET_LOCATION}" "zfs destroy -r \"${ZVOL_FULL_PATH}\"" \
#                    || Background_error "ERROR: Failed to destroy orphan zvol ${ZVOL_FULL_PATH}"
            fi
        done
    fi

    # --- Non‑zvol warnings ---
    local SOURCE_DEVICE_PATH
    local TARGET_DEVICE_PATH
    local -a ORPHAN_NONZVOL_LIST=()

    for TARGET_DEVICE_PATH in "${TARGET_VM_PATH_LIST[@]}"; do
        if [[ "${TARGET_DEVICE_PATH}" == /dev/zvol/${TARGET_POOL}/encrypted-ds/vm-ds/* ]]; then
            continue
        fi
        if [[ "${TARGET_DEVICE_PATH}" == ${TARGET_POOL}/encrypted-ds/vm-ds/* ]]; then
            local ON_SOURCE="no"
            for SOURCE_DEVICE_PATH in "${SOURCE_VM_PATH_LIST[@]}"; do
                [[ "${SOURCE_DEVICE_PATH}" == "${TARGET_DEVICE_PATH}" ]] && ON_SOURCE="yes" && break
            done

            local USED_ELSEWHERE="no"
            jq -e --arg VM "${VM}" --arg PATH "${TARGET_DEVICE_PATH}" '
                .[] | select(.name!=$VM) | .devices
                | to_entries[] | select(.value.attributes.path==$PATH)
            ' <<< "${TARGET_ALL_VM_JSON}" >/dev/null && USED_ELSEWHERE="yes"

            if [[ "${ON_SOURCE}" == "no" && "${USED_ELSEWHERE}" == "no" ]]; then
                ORPHAN_NONZVOL_LIST+=( "${TARGET_DEVICE_PATH}" )
            fi
        fi
    done

    if [[ "${#ORPHAN_NONZVOL_LIST[@]}" -gt 0 ]]; then
        echo "WARNING: VM '${VM}' has orphan non‑zvol paths in vm-ds:"
        for TARGET_DEVICE_PATH in "${ORPHAN_NONZVOL_LIST[@]}"; do
            echo "  Orphan path: ${TARGET_DEVICE_PATH}"
        done
    fi
}

# Rsync file-backed VM disks (qcow2/raw images) listed in VM definition (per VM, per file)
function Rsync_vm_file_disks() {
    local SOURCE_LOCATION="$1"
    local VM="$2"

    local DEV_ID
    local SOURCE_PATH
    local TARGET_PATH
    local RSYNC_SOURCE
    local RSYNC_TARGET

    local RSYNCED=0

    # Iterate over DISK devices in the source VM, carrying the device id
    jq -r --arg VM "${VM}" '
        .[] | select(.name==$VM) | .devices[]
        | select(.attributes.dtype=="DISK")
        | "\(.id) \(.attributes.path)"
    ' <<< "${SOURCE_ALL_VM_JSON}" | while read -r DEV_ID SOURCE_PATH; do

        # Only handle file-backed disks under /mnt/<POOL>/...
        if [[ "${SOURCE_PATH}" == "/mnt/${SOURCE_POOL}/"* ]]; then
            # Must be a file
            Execute_command "${SOURCE_LOCATION}" "test -f \"${SOURCE_PATH}\"" || {
                continue
            }

            # Find the corresponding target path by device id
            TARGET_PATH="$(jq -r --arg VM "${VM}" --argjson ID "${DEV_ID}" '
                .[] | select(.name==$VM) | .devices[]
                | select(.id==$ID)
                | .attributes.path
            ' <<< "${TARGET_ALL_VM_JSON}")"

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
            Execute_command "${TARGET}" "mkdir -p \"${TARGET_PATH%/*}\"" \
                || Background_error "ERROR: Failed to create target dir ${TARGET_PATH%/*} for VM '${VM}'"

            # Print general header once
            if [[ $RSYNCED -eq 0 ]]; then
                echo "Rsyncing file-backed disks for VM '${VM}'..."
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
}

# Replicate one VM’s datasets/zvols
function Replicate_vm() {
    local SOURCE_LOCATION="$1"
    local VM="$2"

    # Scope your replication runner to this VM only.
    Perform_zfs_rep "vm_latest_snapshot_only" "fast"
}

# Verify zvols exist and recreate VM on destination
function Verify_and_recreate_vm() {
    local TARGET_LOCATION="$1"
    local VM="$2"

    local MISSING=()

    # Verify all disks exist
    while IFS= read -r DISK_PATH; do
        if [[ "${DISK_PATH}" =~ ^/dev/zvol/ ]]; then
            local REL="${DISK_PATH#/dev/zvol/}"
            Execute_command "${TARGET_LOCATION}" "zfs list -H \"${REL}\" &>/dev/null" \
                || MISSING+=( "${REL}" )
        fi
    done < <(jq -r '
        .devices | to_entries[]
        | select((.value.dtype // .value.attributes.dtype)=="DISK")
        | .value.attributes.path
    ' "${TRANSFORMED_VM_JSON_FILE}")

    if [[ "${#MISSING[@]}" -gt 0 ]]; then
        echo "Failed to create VM '${VM}': missing zvols: ${MISSING[*]}"
        echo
        return 1
    fi

    # Create the VM shell
    echo "Creating VM '${VM}' on truenas-${TARGET_SERVER_ID} from json... "
    local SHELL_JSON
    SHELL_JSON="$(jq -c 'del(.id, .status, .display_available, .devices)' "${TRANSFORMED_VM_JSON_FILE}")"

    local CREATE_OUT
    if ! CREATE_OUT="$(Execute_command "${TARGET_LOCATION}" "midclt call vm.create '${SHELL_JSON}'")"; then
        echo "Failed."
        return 1
    fi

    local VM_ID
    VM_ID="$(jq -r '.id' <<<"${CREATE_OUT}")"
    echo "  Created VM shell for '${VM}' with id=${VM_ID}"

    # Add devices one by one
    while IFS= read -r DEVICE; do
        local DTYPE ATTRS PAYLOAD
        DTYPE="$(jq -r '.dtype' <<<"${DEVICE}")"
        ATTRS="$(jq -c '.attributes' <<<"${DEVICE}")"

        # Build payload with dtype inside attributes
        PAYLOAD="$(jq -n \
            --argjson attrs "${ATTRS}" \
            --arg dtype "${DTYPE}" \
            --argjson vm "${VM_ID}" \
            '{vm: $vm|tonumber, attributes: ($attrs + {dtype: $dtype})}')"

        echo -n "  Adding ${DTYPE} device... "
        if Execute_command "${TARGET_LOCATION}" "midclt call vm.device.create '${PAYLOAD}' &>/dev/null"; then
            echo "Ok."
        else
            echo "Failed."
            return 1
        fi
    done < <(jq -c '
        .devices | to_entries[]
        | {
            dtype: (.value.dtype // .value.attributes.dtype),
            attributes: (
                .value.attributes
                | del(.id, .vm, .order)
            )
        }
    ' "${TRANSFORMED_VM_JSON_FILE}")
    echo
}

#######################################
# High-level orchestrator
#######################################

function Perform_vm_replication() {
    local SOURCE
    local TARGET

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

    echo "#########################################"
    echo "### Starting VM replication workflow  ###"
    echo "#########################################"
    echo

    # Prepare vars
    [[ -n "${LOCAL_SOURCE}" ]] && SOURCE="local" || SOURCE="remote"
    [[ -n "${LOCAL_TARGET}" ]] && TARGET="local" || TARGET="remote"

    if [[ "${SOURCE}" == "local" ]]; then
        SOURCE_POOL="$(Resolve_pool "${LOCAL_SERVER_ID}" "fast")"
        SOURCE_SERVER_ID="${LOCAL_SERVER_ID}"
    else
        SOURCE_POOL="$(Resolve_pool "${REMOTE_SERVER_ID}" "fast")"
        SOURCE_SERVER_ID="${REMOTE_SERVER_ID}"
    fi
    if [[ "${TARGET}" == "local" ]]; then
        TARGET_POOL="$(Resolve_pool "${LOCAL_SERVER_ID}" "fast")"
        TARGET_SERVER_ID="${LOCAL_SERVER_ID}"
    else
        TARGET_POOL="$(Resolve_pool "${REMOTE_SERVER_ID}" "fast")"
        TARGET_SERVER_ID="${REMOTE_SERVER_ID}"
    fi

    # Extract VM definitions into JSONs
    Extract_vm_definitions

    # === Loop through each source VM ===
    while IFS= read -r VM; do
        Vm_in_scope "SOURCE" "${VM}" || continue
        PROCESSED_VM_LIST+=("${VM}")

        echo
        echo "### Replicating VM: ${VM} ###"

        SOURCE_VM_JSON_FILE="${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${SOURCE_SERVER_ID}.source.json"
        TARGET_VM_JSON_FILE="${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${TARGET_SERVER_ID}.target.json"
        TRANSFORMED_VM_JSON_FILE="${VM_TMP_DIR}/json/per_vm/${VM}.truenas-${TARGET_SERVER_ID}.transformed.json"

        if [[ ! -f "${SOURCE_VM_JSON_FILE}" ]]; then
            echo "Missing per‑VM JSON: ${SOURCE_VM_JSON_FILE}"
            ((FAILED++))
            continue
        fi

        # a. Validate state
        if ! Vm_is_stopped "SOURCE" "${VM}"; then
            ((FAILED++))
            continue
        fi

        Transform_vm_definition "${SOURCE_SERVER_ID}" "${TARGET_SERVER_ID}"
        Cleanup_vm_disk_tags "${SOURCE}"
        Tag_vm_disks "${SOURCE}" "${VM}"
        Delete_vm_on_destination "${TARGET}" "${VM}"
        Audit_and_cleanup_vm_storage "${TARGET}" "${VM}"
        Rsync_vm_file_disks "${SOURCE}" "${VM}"
        Replicate_vm "${SOURCE}" "${VM}"
        Cleanup_vm_disk_tags "${SOURCE}"

        if Verify_and_recreate_vm "${TARGET}" "${VM}"; then
            ((SUCCEEDED++))
        else
            ((FAILED++))
        fi
    done < <(jq -r '.[].name' <<< "${SOURCE_ALL_VM_JSON}")

    # Post-loop: check if all requested VMs were actually processed
    if [[ "${#VM_LIST[@]}" -gt 0 ]]; then
        for REQ in "${VM_LIST[@]}"; do
            if [[ ! " ${PROCESSED_VM_LIST[*]} " =~ " ${REQ} " ]]; then
                echo "Requested VM '${REQ}' not found on source"
                ((NOTFOUND++))
            fi
        done
    fi

    echo "### Summary: ${SUCCEEDED} succeeded, ${FAILED} failed, ${NOTFOUND} not found ###"
    echo
}
