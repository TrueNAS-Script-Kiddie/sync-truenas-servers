# Backlog — small open items

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

Build it into `transform_vm` in `lib/transform.py`.

## `midclt call -job` instead of polling (optional)

`midclt call -job app.stop <name>` blocks until the job ends and exits non-zero on failure, which
would replace the JOBID/poll loops in `Control_app`. Check on a host first: exit codes, and that
its output doesn't clutter the log.

## Log file name per minute

`LOG_FILE` (and `DB_RESTORE_LOG`) in `bin/sync_truenas_servers` are named `%Y-%m-%d_%H-%M`, so two
runs started in the same minute share one log: the second appends to it and its `tail -f` first
replays the previous run's output. Add seconds (`%H-%M-%S`).

## Catalog-version pre-check before `app.create` (optional)

`app.available` only reports `latest_version`, so a missing version is detected only by the
create job failing. Find an API that lists installable versions, or keep relying on the failure.
