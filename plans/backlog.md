# Backlog — small open items

## Bug: VM UUID collision on the target's delete → recreate

Under `--stop-running-vms` the target rebuild intermittently fails with
`operation failed: domain '<old-id>_<name>' is already defined with uuid <uuid>`, even with fully
graceful stops (seen 2026-08-23 on FedoraBC and FedoraF; the same VMs succeed on other runs).

Cause: `Transform_vm_definition` copies the source VM's `uuid` verbatim, so every recreate asks
libvirt for the **same** UUID. `vm.delete` returning does not guarantee the old libvirt domain is
already undefined — when it isn't, `vm.create` collides with it.

Fix: after `Delete_vm_on_destination`, poll until the domain is gone before creating (bounded
wait, see the wait-loop pattern). Keep the UUID in every VM mode (next item): it is the guest's
SMBIOS UUID, and a new one on every sync would break Windows activation and UUID-bound licences.

Recovery until fixed: [docs/operations.md](../docs/operations.md#vm-orphan-libvirt-domain).

## VMs: per-VM choice between "twin" and "own MAC"

Today every VM is a **twin**: UUID and every NIC's `mac` are copied verbatim (only `nic_attach`,
paths and display ports are rewritten). Failover is transparent (same MAC → same DHCP address)
and UUID- or MAC-locked licences work on both hosts, but only one copy may run at a time — two
running copies put one MAC twice on the LAN.

Add a per-VM setting in `config/vm_device_mappings.json`, default `twin`:

| | twin (default) | own MAC |
|---|---|---|
| UUID | copied | copied (harmless across two hypervisors) |
| MAC per NIC | copied | target keeps its own; if none or equal to the source's, `mac` is dropped so TrueNAS generates one |
| Both copies running | no | yes, if the guest takes its IP from DHCP keyed on the MAC (no static IP, no DUID-based client id) |
| Failover | transparent | clients that reach the VM by IP need its truenas-backup address, or the DHCP reservations are swapped ([docs/operations.md](../docs/operations.md#failover--truenas-backup-takes-over)) |
| MAC-locked licences | work on both hosts | work on one host only |

With own MAC, both copies are still disk clones (hostname, machine-id, SSH host keys), and
anything the guest does outward (writes to shared storage, scheduled jobs) runs twice.

Unaffected either way: the guest sees each host's CPU (`cpu_mode` `HOST-MODEL`), so a Windows
hardware ID already differs between the hosts.

Build in the VM transform after its Python port ([app-replication.md](app-replication.md) step 1).

## Exit-code-aware partial failure for `zfs_autobackup`

Its exit code 1–254 = that many datasets failed, the rest completed; 255 = crash. Today any
non-zero aborts the whole run. Treat 1–254 as a partial failure and let the remaining subtasks
(rollup, later scopes) run. Limits the damage of [dataset busy](../docs/dataset-busy-investigation.md),
doesn't address its cause.

## `Backup_immich_DB` dumps without waiting for Postgres

It starts the `pgvecto` container and runs `pg_dumpall` straight away. Low risk (existing
cluster, no bootstrap phase; `pg_dumpall` uses the socket), but "container running" ≠ "Postgres
ready". Add a socket-side readiness wait like `Restore_immich_DB`'s.

## `midclt call -job` instead of polling (optional)

`midclt call -job app.stop <name>` blocks until the job ends and exits non-zero on failure, which
would replace the JOBID/poll loops in `Control_app`. Check on a host first: exit codes, and that
its output doesn't clutter the log.

## Catalog-version pre-check before `app.create` (optional)

`app.available` only reports `latest_version`, so a missing version is detected only by the
create job failing. Find an API that lists installable versions, or keep relying on the failure.
