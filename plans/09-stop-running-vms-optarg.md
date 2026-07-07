# Plan 09 — `--stop-running-vms` (stop/restart VMs around replication)

**Status: implemented and verified (2026-07-07).** Both source-side and
target-side paths confirmed on real runs. Also folds in plan 01 item 8
(target-VM-running check). Lint-clean (`bash -n`, `shellcheck -x`).

## What it does

By default a VM that is running at replication time is **skipped** (counted as
failed). With `--stop-running-vms` (opt-in, because a VM — unlike a headless
app container — can be in active interactive use), a running VM is stopped
before replicating it and started again afterwards **if this run stopped it**.

Applies to **both sides**, with an asymmetry that matters:

- **Source running** → graceful stop, replicate, then start again. Restart is
  **guaranteed on abort too** (via the cleanup stack): the source is only ever
  read (snapshotted), never modified, so restoring its running state is safe.
- **Target running** → graceful stop (so it can be deleted + rebuilt), and on
  **success** start the freshly-rebuilt copy (only if it was running before).
  On **abort** the target is deliberately **NOT** auto-started — its old
  instance was intentionally destroyed mid-sync, so it may be half-rebuilt.
  Instead a loud `Warn_target_vm_state` warning fires telling the operator the
  target's state differs from before the run and needs a manual check.

## Two load-bearing rules (learned the hard way — do not "optimize" away)

1. **Stops must be GRACEFUL, never forced** — for *both* sides. Beyond the
   source needing a clean shutdown before its zvol is snapshotted, a **forced**
   poweroff of the *target* flips `.status.state` to STOPPED while libvirt is
   still mid-teardown; the following `vm.delete` then fails to undefine the
   domain, leaving an **orphan libvirt domain** that still holds the VM's UUID
   — which makes the next `vm.create` fail with `domain '<id>_<name>' is
   already defined with uuid …` and the rebuilt VM lands in `ERROR`. A graceful
   stop lets the guest and libvirt fully settle (`.state` reaches STOPPED only
   once the domain is really `SHUTOFF`), so `vm.delete` undefines cleanly and
   there is no orphan. Confirmed by direct experiment: force-stop→delete leaves
   an orphan; graceful-stop→delete does not.
2. **Wait for the `vm.stop` JOB, not just `.status.state`.** `vm.stop` is a
   middleware *job*; `Control_vm` waits for it to reach `SUCCESS` via
   `core.get_jobs`, then confirms `.state == STOPPED`. (`vm.start` is
   synchronous — for it we just poll the state.)

Trade-off of graceful-only: if a target guest is **still booting** or is
otherwise unresponsive to ACPI, the stop can't complete — there is no way to
detect "still booting" from outside the guest (`vm.query` shows `RUNNING`
either way). In that case the run **aborts cleanly** after the 180s timeout
with a clear message ("guest still booting or not responding to
graceful shutdown — let it finish booting and retry, or stop it manually"),
the cleanup stack restarts the source, and the target warning fires. So the
practical discipline is: don't kick off a `--stop-running-vms` run in the
first ~minute after starting a VM.

## Where it lives

- `lib/cli.bash` — `--stop-running-vms` flag (sets `STOP_RUNNING_VMS`),
  guarded to require the `vm_replication` subtask, documented in `Help`.
- `bin/sync_truenas_servers` — `declare STOP_RUNNING_VMS`.
- `lib/rep_vms.bash`:
  - `Control_vm <name> <id> <stop|start> <location> <server_id>` — graceful
    stop (waits for the job + confirms STOPPED) / synchronous start (polls
    RUNNING); 180s timeout → `Background_error` with an actionable message;
    skipped under `--test`.
  - `Warn_target_vm_state` — the abort-warning described above.
  - `Perform_vm_replication` loop — replaces the old `Vm_is_stopped` skip with
    per-side state checks + stop/restart, wired through the shared cleanup stack
    (`Register_cleanup`/`Unregister_cleanup`, see `architectural_patterns.md`).
    Registrations are strictly nested: source-restart first, target-warn later
    (registered only when the target is actually stopped, so an abort before
    that point produces no misleading warning), both unregistered on the clean
    path — so on abort the stack pops correctly (filesystem remount from
    `Replicate_vm`, then the target warning, then the source restart).
  - `Vm_is_stopped` was removed (its only caller is gone).

## Verified (2026-07-07)

- **midclt semantics:** `vm.stop` is a job, `vm.start` is synchronous;
  `vm.delete` undefines the libvirt domain cleanly **only** once a graceful
  stop has fully settled (proven by sampling `.status` through a stop and by
  isolated stop→delete experiments).
- **Source-side:** real `master_to_backup --vm=FedoraF --stop-running-vms`
  (source RUNNING on master, target STOPPED) — graceful stop, replicate,
  recreate, source restarted to RUNNING, `1 succeeded`. Also confirmed the
  source is never snapshotted while still shutting down: a graceful stop's
  `.state` reaches STOPPED only after the guest is fully off (`pid=null`,
  `domain_state=SHUTOFF`).
- **Target-side:** real run with FedoraF RUNNING on the *target* (backup),
  STOPPED on master — graceful target stop (~8s guest shutdown), delete,
  replicate, recreate, target restarted to RUNNING (it was running before),
  `1 succeeded`, and **no orphan domain** left in `virsh list --all`.

## Still to verify (optional, low priority)

1. **Abort path (fuller):** kill the process mid-replication of a stopped-by-us
   VM and confirm the source is restarted (cleanup stack) and, if the target
   was running, the warning appears and the target is NOT auto-started. (The
   warning path itself was already seen firing correctly during debugging.)
2. **`--test`:** confirm a running VM logs the intended stop/replicate/start
   sequence with the `(Not done because of '--test' usage!)` markers and no
   real power change.

## Appendix — recovering from a pre-fix orphan (should no longer occur)

If an old forced-stop-era orphan is ever encountered (`vm.create` fails with
`domain '<id>_<name>' is already defined`): force-delete the ERROR-state VM
(GUI Force Delete, or `midclt call vm.delete <id> '{"force": true}'` — leave
"Delete Virtual Machine Data" / `zvols` OFF so the replicated disk is kept),
then undefine the leftover domain:
`virsh -c 'qemu+unix:///system?socket=/run/truenas_libvirt/libvirt-sock' undefine <id>_<name>`.
The TrueNAS libvirt socket is non-standard (the default `libvirtd.socket` is
masked); that URI is how `virsh` connects.
