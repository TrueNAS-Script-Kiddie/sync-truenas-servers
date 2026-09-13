# Plan 09 — `--stop-running-vms` (stop/restart VMs around replication)

**Status: `--stop-running-vms` itself implemented and verified (2026-07-07).**
Both source-side and target-side paths confirmed on real runs. Also folds in
plan 01 item 8 (target-VM-running check). Lint-clean (`bash -n`,
`shellcheck -x`).

**⚠️ Open bug (found 2026-08-23): the target-side delete→recreate step can
still fail on a UUID collision, even with a fully graceful stop.** See
"Known open bug" below — the "no orphan" claim in this plan only holds for
the forced-poweroff case it was written to fix; a second, still-unfixed
mechanism produces the same symptom.

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

## Known open bug — UUID collision on delete→recreate (unresolved)

Confirmed 2026-08-23 on real runs, **with fully graceful stops** (job waited
for `SUCCESS`, `.state` confirmed `STOPPED` — the exact discipline this plan
requires). The target-side rebuild still intermittently fails with:

```
operation failed: domain '<old-id>_<name>' is already defined with uuid <uuid>
```

**Root cause: `Transform_vm_definition` copies the source VM's `uuid` field
into the destination definition verbatim, and it is never regenerated.**
Confirmed by inspecting the per-VM JSON: FedoraF's transformed JSON always
carries `uuid: bb499b25-2296-418e-a71c-12bd0759ae96` — the master VM's own
libvirt UUID — on every single replication run, no matter how many times the
destination copy is deleted and rebuilt. Because that UUID never changes,
`vm.create`'s `defineXML` call always asks libvirt to register a domain
under the *exact same UUID as the previous destination attempt*. If the
prior `vm.delete`'s libvirt-undefine hasn't fully completed by the time the
following `vm.create` runs — a race, since `vm.delete` returning success
does not guarantee the underlying libvirt domain is synchronously
undefined — the new `defineXML` collides with the not-yet-cleared old
domain, which is still holding that UUID.

This is a genuine, reproducible middleware/timing race, not caused by a
logged-in session, a manually-created VM, or anything external — the
colliding UUID is provably always the source VM's own fixed UUID, which
only this script's transform step ever copies over. It hit two independent
VMs (FedoraBC, FedoraF) on 2026-08-23, on different runs, each time with
that VM's own fixed source UUID in the error — never a random/foreign one.
FedoraBC's delete→recreate has since succeeded cleanly on other runs the
same day, confirming this is a race (sometimes won, sometimes lost), not a
deterministic failure — so it will keep recurring under
`--stop-running-vms` until fixed.

**Not yet fixed.** Two candidate fixes, not yet implemented:
1. Stop copying `.uuid` into the destination definition (strip it in
   `Transform_vm_definition`, same way `.id`/`.status`/`.devices` are
   already stripped) — let TrueNAS/libvirt generate a fresh UUID for the
   destination domain, decoupling it entirely from whatever the previous
   destination attempt's (possibly-still-lingering) domain object holds.
2. After `Delete_vm_on_destination`, actively poll (`virsh list --all` or
   `vm.query`) until the domain is confirmed gone before proceeding to
   `Create_vm_on_destination`, instead of trusting `vm.delete`'s return
   value alone.
Fix 1 is probably sufficient on its own and removes the dependency on
`vm.delete` timing entirely; fix 2 closes the race directly if UUIDs must
stay matched for some undocumented reason. Until one lands, treat this as a
recurring failure mode of `--stop-running-vms`, not a one-off — see the
appendix below for manual recovery.

## Appendix — recovering from an orphan domain (recurring, see bug above)

When `vm.create` fails with `domain '<id>_<name>' is already defined`:
force-delete the ERROR-state VM (GUI Force Delete, or
`midclt call vm.delete <id> '{"force": true}'` — leave "Delete Virtual
Machine Data" / `zvols` OFF so the replicated disk is kept), then undefine
the leftover domain:
`virsh -c 'qemu+unix:///system?socket=/run/truenas_libvirt/libvirt-sock' undefine <id>_<name>`.
The TrueNAS libvirt socket is non-standard (the default `libvirtd.socket` is
masked); that URI is how `virsh` connects. Then re-run VM replication for
that VM — the next attempt has a good chance of winning the race, but is
not guaranteed to (see bug above).
