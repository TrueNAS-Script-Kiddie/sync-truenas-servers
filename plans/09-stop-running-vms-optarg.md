# Plan 09 — Optional VM stop/start around replication (`--stop-running-vms`)

## The ask

Today, if a VM is running at replication time, `Vm_is_stopped`
([lib/rep_vms.bash:42-56](../lib/rep_vms.bash#L42-L56)) just skips it:

```bash
function Vm_is_stopped() {
    ...
    if [[ "${STATE}" != "STOPPED" ]]; then
        echo "Replication skipped for VM ${VM} (state='${STATE}')"
        return 1
    fi
}
```

`Perform_vm_replication`'s loop treats that as a per-VM failure and moves on
([lib/rep_vms.bash:734-738](../lib/rep_vms.bash#L734-L738)):

```bash
if ! Vm_is_stopped "SOURCE" "${VM}"; then
    ((FAILED++))
    continue
fi
```

Requested: an explicit, **off-by-default** CLI option that, when passed,
stops a running source VM before syncing it and starts it back up again
afterward — the same shape as app replication already does
(`Control_app_with_checks` in `lib/rep_apps.bash` stops/starts apps around
their rsync). **Explicitly not the default** — confirmed correct in
discussion: a VM can be in active interactive use (an RDP/console session,
a long-running task, a game session) in a way a headless app container
essentially never is, so silently powering one off carries a materially
higher risk of interrupting real work. An explicit `--stop-running-vms` flag
keeps today's safe "skip and report" behavior as the default, and only takes
the more invasive action when you've deliberately asked for it (e.g. an
unattended night where you know nobody's using any VM).

---

## Phase 0 — verify `midclt` VM semantics live (do this before writing the poll loop)

`Control_app` ([lib/rep_apps.bash:5-30](../lib/rep_apps.bash#L5-L30)) works
because `midclt call app.stop "<name>"` returns a **job ID**, polled via
`midclt call core.get_jobs "[[\"id\",\"=\",<id>]]"` until `.[0].state` is
`SUCCESS`/`FAILED`/`ABORTED`/`ERROR`. VMs are a different middleware
namespace (`vm.*`, not `app.*`) and there's no confirmation yet that
`vm.stop`/`vm.start` follow the same job-based pattern rather than, say,
returning a plain boolean synchronously. **Confirm live on a TrueNAS host
before coding the wait loop**, same spirit as plan 06's phase 0 for
`app.config`/`app.create`:

```bash
midclt call vm.query | jq -r '.[] | select(.name=="<some running VM>") | .id'
midclt call vm.stop <id>        # what does this return? a job id, or immediate true/false?
midclt call vm.query | jq -r '.[] | select(.id==<id>) | .status.state'   # what values appear mid-stop? (RUNNING → ??? → STOPPED)
midclt call vm.start <id>       # same question
```

`Vm_is_stopped` already establishes `.status.state` is the field to read,
and that `STOPPED` is the terminal "off" value — but the *transitional*
state(s) while a VM is powering down (if any are exposed at all, as opposed
to `vm.stop` blocking until it's actually off) aren't known yet. Whatever
the answer, follow the existing "state-machine loop with timeout" shape
used everywhere else in this codebase (`Control_app`, `Wait_for_docker_state`,
`Wait_for_pg_ready` — 60s timeout, `Background_error` on expiry) rather than
inventing a new retry strategy.

---

## Design

### 1. New CLI option — `lib/cli.bash`

Add `--stop-running-vms` as a plain boolean flag, parsed like `--test`:

```bash
--stop-running-vms)
    # shellcheck disable=SC2034  # consumed in lib/rep_vms.bash
    STOP_RUNNING_VMS="true"
    ;;
```

Guard it the same way `--vm`/`--app` are already guarded at the end of
`Process_command_line_options` (only meaningful alongside
`vm_replication`):

```bash
if [[ -n "${STOP_RUNNING_VMS}" && -z "${PERFORM_VM_REP}" ]]; then
    echo "ERROR: --stop-running-vms can only be used when the vm_replication subtask is enabled."
    exit 1
fi
```

Document it in `Help()` next to the existing `--vm=<name>` entry.

### 2. New `Control_vm` — `lib/rep_vms.bash`

Mirrors `Control_app`, adjusted for whatever phase 0 finds — issue
`midclt call vm.${ACTION} <id>`, then poll `.status.state` (via
`vm.query`, same as `Vm_is_stopped` already does) until it reaches the
expected terminal state, with the standard 60s-timeout /
`Background_error` shape. Takes the VM's numeric `id` (already extracted
from `SOURCE_ALL_VM_JSON` elsewhere, e.g. `Delete_vm_on_destination`'s
`jq -r ... | .id` lookup), not the VM name — `vm.*` middleware calls key
off `id`, unlike `app.*` which keys off name.

### 3. Replace the hard skip in `Perform_vm_replication`

Where the loop currently hard-skips a running VM
([lib/rep_vms.bash:734-738](../lib/rep_vms.bash#L734-L738)), branch on
`STOP_RUNNING_VMS`:

- **Not set (default, today's behavior, unchanged):** running VM → skip,
  count as failed, exactly as now.
- **Set:** running VM → stop it via `Control_vm`, remember that this VM
  was stopped **by this run** (so restart only fires for VMs *we* stopped,
  never one that was already off for an unrelated, intentional reason —
  same "only touch what you yourself changed" principle as plan 01 item 14's
  `UNMOUNTED_LIST`), then continue into the existing replication steps.

### 4. Guaranteed restart — apply the exact lesson from plan 01 item 14

Once a VM is stopped for this run, every downstream step in that VM's loop
body (`Transform_vm_definition`, `Tag_vm_disks`, `Rsync_vm_file_disks`,
`Replicate_vm`, `Verify_and_recreate_vm`, …) can call `Background_error`,
which `exit`s the **entire script** immediately — exactly the mechanism
that left six ZFS datasets unmounted for hours in plan 01 item 14. Without
the same safeguard here, a failure partway through one VM's replication
would leave that VM powered off indefinitely, with nothing to tell anyone
why. **Use the shared cleanup stack** (`Register_cleanup` /
`Unregister_cleanup` / `Run_cleanup` in `lib/common.bash` — introduced
2026-07-03 as the generalization of plan 01 item 14's trap, upgraded to a
LIFO **stack** on 2026-07-04 specifically so this plan composes with the
filesystem replication's own registration): `Register_cleanup` a
restart-this-one-VM command immediately after the stop succeeds, and
`Unregister_cleanup` once the explicit restart at the end of that VM's
processing runs normally. `Background_error` runs the stack before killing
the tail (restart output visible live), and the global
`trap 'Run_cleanup; Kill_tail' EXIT` in `bin/sync_truenas_servers` covers
exits that bypass `Background_error`. Nesting is handled by design: the VM
entry is registered *before* `Replicate_vm` (a failure inside replication
must restart the VM — that's the point), and
`Perform_filesystem_replication`'s own inner register/unregister cycle
sits safely on top of it; on an abort inside that window, `Run_cleanup`
pops newest-first, so the datasets are remounted *before* the VM that owns
them is restarted. Two implementation cautions remain: (1) since VMs are
processed one at a time in a loop (unlike the filesystems case), the
register/unregister pair must run **fresh on every loop iteration** — a
stale entry could restart the wrong VM (or one already correctly
restarted) after a later iteration's failure; (2) keep the registered
command single-quoted so its variables expand at fire time, matching how
`rep_filesystems.bash` registers its remount.

### 5. Source-side only — no target-side equivalent needed

The target VM is deleted (`Delete_vm_on_destination`) and recreated fresh
(`Verify_and_recreate_vm`) every run, and this script never starts it
automatically — so there's no target-side running state to preserve or
restore. Only the source VM's stop/restart is in scope here.

---

## Verification checklist (once implemented)

1. `bash -n` + `shellcheck -x` on `lib/cli.bash` and `lib/rep_vms.bash`.
2. `--test` run without `--stop-running-vms`: confirm a running VM is still
   skipped exactly as today (no regression to default behavior).
3. `--test` run with `--stop-running-vms` against a genuinely running,
   low-stakes VM: confirm the stop/replicate/restart sequence is logged
   correctly (introspection still happens under `--test`, matching this
   codebase's `--test` discipline — see `architectural_patterns.md`).
4. A real run, same VM: confirm it's actually stopped, actually replicated,
   and actually running again afterward (`midclt call vm.query | jq ...`).
5. A deliberate failure injection partway through one VM's processing
   (e.g. kill the process during `Replicate_vm`, same technique used to
   validate plan 01 item 14): confirm the trap still restarts that VM
   despite the abort.
