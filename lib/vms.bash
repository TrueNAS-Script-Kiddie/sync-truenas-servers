#!/usr/bin/bash
# lib/vms.bash
# Virtual Machine replication orchestration

#######################################
# Preflight checks (both servers)
#######################################

function Export_vm_definitions() {
    local LOCATION="$1"
    echo "### Exporting VM definitions on ${LOCATION} ###"
    # midclt call vm.query > "${VM_TMP_DIR}/json/all/vms.${LOCATION}.json"
}

function Extract_zvol_list() {
    local LOCATION="$1"
    echo "### Extracting ZVOL list for ${LOCATION} ###"
    # mapfile -t VM_ZVOL_LIST_${LOCATION^^} < <(jq ... )
}

function Validate_vm_states() {
    local LOCATION="$1"
    echo "### Validating VM states on ${LOCATION} ###"
    # Abort if any VM != STOPPED
}

function Validate_vm_disks() {
    local LOCATION="$1"
    echo "### Validating VM disks on ${LOCATION} ###"
    # Ensure each VM has at least one disk and ZVOL exists
}

#######################################
# Source server prep
#######################################

function Split_vm_json() {
    local LOCATION="$1"
    echo "### Splitting per-VM JSONs on ${LOCATION} ###"
    # jq split into ${VM_TMP_DIR}/json/per_vm/*.json
}

function Tag_vm_zvols() {
    local LOCATION="$1"
    echo "### Tagging VM ZVOLs on ${LOCATION} for replication ###"
    # zfs set autobackup:... on each ZVOL
}

#######################################
# Destination server prep
#######################################

function Adjust_vm_definitions_for_destination() {
    echo "### Adjusting VM definitions for destination ###"
    # NICs, display ports, bind addresses, pool paths
}

function Delete_all_vm_definitions() {
    echo "### Deleting all VM definitions on destination ###"
    # midclt call vm.delete ...
}

function Early_cleanup_orphan_zvols() {
    echo "### Cleaning up orphaned ZVOLs on destination ###"
    # Compare source vs dest lists, zfs destroy orphans
}

#######################################
# Replication (reuse existing)
#######################################

function Perform_vm_zfs_replication() {
    echo "### Performing ZFS replication for VMs ###"
    Perform_zfs_rep all_snapshots   # or latest_snapshot_only
}

#######################################
# Destination recreate
#######################################

function Verify_zvols_after_replication() {
    echo "### Verifying ZVOLs after replication ###"
    # Ensure every disk path in JSON resolves
}

function Recreate_vms_from_json() {
    echo "### Recreating VMs from JSON ###"
    # midclt call vm.create < adjusted JSON
}

#######################################
# Postflight
#######################################

function Audit_vm_definitions() {
    echo "### Auditing VM definitions ###"
    # Export again, compare with source, log differences
}

function Cleanup_vm_tags_on_source() {
    echo "### Cleaning up autobackup tags on source ###"
    # zfs inherit autobackup:...
}

#######################################
# High-level orchestrator
#######################################

function Perform_vm_replication() {
    echo "#########################################"
    echo "### Starting VM replication workflow  ###"
    echo "#########################################"
    echo

    # Preflight
    Export_vm_definitions local
    Export_vm_definitions remote
    Extract_zvol_list local
    Extract_zvol_list remote
    Validate_vm_states local
    Validate_vm_states remote
    Validate_vm_disks local
    Validate_vm_disks remote

    # Source prep
    Split_vm_json local
    Tag_vm_zvols local

    # Destination prep
    Adjust_vm_definitions_for_destination
    Delete_all_vm_definitions
    Early_cleanup_orphan_zvols

    # Replication
    Perform_vm_zfs_replication

    # Destination recreate
    Verify_zvols_after_replication
    Recreate_vms_from_json

    # Postflight
    Audit_vm_definitions
    Cleanup_vm_tags_on_source

    echo
    echo "### VM replication workflow completed successfully ###"
    echo
}
