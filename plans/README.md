# Plans — analysis of sync_truenas_servers (2026-07-02)

Full-code review of `bin/`, `lib/`, `config/`, `Makefile` and all project md files.
Split into independent plans, ordered by value/risk. Each plan is written to be
implemented **one numbered item at a time** by any model, with exact locations,
replacement snippets, and verification steps.

| Plan | What | Risk | Do it? |
|---|---|---|---|
| [01-bug-fixes.md](01-bug-fixes.md) | 14 concrete bugs (11 original + 3 found via plan 08's investigation). 11 low-risk/easily-tested ones done and lint-verified (2026-07-03), 8 of them confirmed on both a `--test` and a real run; items 13/14 confirmed live via a deliberate kill-test. Item 14 (a failed run left 6 production datasets unmounted for hours) is fixed via the shared LIFO cleanup stack (`Register_cleanup`/`Unregister_cleanup`/`Run_cleanup` + `Kill_tail` in `lib/common.bash` — scoped to what the run itself unmounted, NOT a blind "remount everything"; `Background_error` runs the stack before killing the tail so restore output is visible live, with a global `trap 'Run_cleanup; Kill_tail' EXIT` as safety net that also fixed a bare-`exit`-leaves-tail-hanging latent bug; stack semantics let plan 09's VM restart nest around it); 3 deferred (Immich restore guard, dataset-selection query change, VM target-state check) | Low (done items); Medium (3 deferred) | Tackle the 3 deferred ones next |
| [02-safety-and-notifications.md](02-safety-and-notifications.md) | Failure email, single-instance lock, log/dump retention | Low | **Yes** — real-run testing on 2026-07-03 (plan 08) directly demonstrated the missing-failure-email gap |
| [03-makefile-and-hygiene.md](03-makefile-and-hygiene.md) | Makefile is broken in 3 ways; add `.gitattributes` | Low | Yes (or delete the Makefile if SFTP is the real deploy path) |
| [04-simplifications.md](04-simplifications.md) | Behavior-preserving refactors: direction helper, dead code, `--app` validation at parse time | Medium | Yes, after 01 |
| [05-python-vm-transform.md](05-python-vm-transform.md) | Optional: port the 140-line jq transform to a small, offline-testable Python script | Medium | Optional |
| [06-app-definition-replication.md](06-app-definition-replication.md) | App-definition replication via `midclt app.config`/`app.create` (VM-pattern mirror), config convergence, phased name convergence | Medium–High | Yes (phased); closes the real failover gap |
| [07-shellcheck-findings.md](07-shellcheck-findings.md) | Full-codebase shellcheck triage (every tracked `.bash` file). Everything actionable is done, lint-verified, and confirmed on both a `--test` and a real run (2026-07-03): root-cause fixes (1/1a/1b), all 10 real fixes (2a-2j), all false-positive disables (3a/3d/3e/3f). 3b/3c intentionally left for other plans' real fixes; one valid-but-deferred item kept visible on purpose (4a) | Low | Done |
| [08-known-operational-issues.md](08-known-operational-issues.md) | Immich Postgres collation-version warning (Immich/Postgres admin matter, no action here); ZFS "dataset is busy" receive failures — an initial "stale handle, unchecked umount" theory (fixed as plan 01 item 12) was **falsified by real evidence**: the failure occurs mid-transfer, hours in, most often on the first-processed (largest) dataset. Automatic diagnostics (items 13/14) confirmed working via a deliberate kill-test (2026-07-03), which also surfaced and then **retracted** a Veeam-lease lead (you confirmed the failure also occurs with Veeam not running). Target-share-disable-for-the-whole-duration and auditing `truenas-backup`'s scheduled tasks remain the two live leads | N/A (documentation) / Low (fixed piece, doesn't solve the main issue) | Read; diagnostics are in place and proven to work — next real occurrence should finally produce useful evidence |
| [09-stop-running-vms-optarg.md](09-stop-running-vms-optarg.md) | New, explicit **off-by-default** `--stop-running-vms` CLI option: stop a running source VM before syncing it, restart it after — mirroring app replication's existing stop/rsync/start pattern, but opt-in because a VM (unlike a headless app container) can be in active interactive use. Needs a phase 0 live-verification of `midclt vm.stop`/`vm.start` semantics first (same spirit as plan 06 phase 0), and a `trap`-guaranteed restart per VM (same lesson as plan 01 item 14) | Medium (touches a live VM's power state; mitigated by opt-in flag + guaranteed-restart trap + phase 0 verification) | Do phase 0 first, then implement |

## Global guardrails (apply to EVERY plan)

1. **Never change the Immich container stop/start order** in `lib/immich_db.bash`.
   The sequence (stop writers → start pgvecto → dump / stop all → wipe → start
   pgvecto → restore → full app stop → start) is the only order that works.
2. **Never use bare `exit` inside `lib/` functions** — always `Background_error`
   (the script runs backgrounded under `nohup | tail -f`; a bare `exit` orphans
   the tail). `cli.bash` parse-time code is the only exception.
3. **Preserve the `LOCAL_SOURCE`/`REMOTE_SOURCE`/`LOCAL_TARGET`/`REMOTE_TARGET`
   direction model** (exactly one of each pair set). Plans may *add* derived
   variables, never remove or repurpose these.
4. **`--test` discipline**: state-changing commands must be suppressed in
   TEST_MODE (via the `test` token in `Execute_command` mode, or a
   `[[ -z "${TEST_MODE}" ]]` guard). Introspection and app/container
   stop/start still happen in test mode — that is intentional.
5. **The dump path is load-bearing**: the Immich dump is written into
   `immich-pgdata-ds/` then moved to `immich-data-ds/backups/` because
   `backups` is in `app_dir_list` and rides the existing rsync. Don't touch
   either path.
6. After every edit: `bash -n <file>`. Before trusting a change:
   `./bin/sync_truenas_servers --test --task=master_to_backup` on a TrueNAS host.

## Answers to the open questions

### Q3 — Rename apps so both servers use the same app name (drop `-master`/`-backup`)?

**Revised recommendation (2026-07-02): converge — but via the
app-definition-replication pipeline, not as a manual reinstall.** An earlier
analysis (correctly) established that the suffixed names and divergent ports
are *convention, not requirement*: TrueNAS scopes app names per host, and
different hosts mean no port conflicts. The only things that genuinely must
diverge are external identities (Plex GUID/claim, anything registered with a
third party) and per-host values like certificate IDs. Everything else is
self-imposed drift, and every artificial divergence is one more transform rule
and one more thing to remember.

Two corrections to that analysis, and one reinforcement:

- **Name convergence is not "small."** TrueNAS cannot rename an installed
  app, so same-names means delete + recreate on **both** hosts (master too).
  Do it last, per app, using the pipeline — each migration then doubles as a
  failover drill. See plan 06 phase 3, which adds a per-app
  `per_server_name_suffix` transition flag so apps migrate one at a time.
- The suffix interpolation lives in **code**, not `apps.json`
  (`lib/rep_apps.bash:44`, the docker greps in `lib/immich_db.bash`) — the
  code saving from dropping it is real but modest (~10–15 lines).
- **Reinforcement:** identical ports on both hosts is not just cleanup — the
  `data` floating-alias failover (hosts-file repoint) is only transparent for
  web services if both hosts serve on the same port. Port convergence is
  *required* by the homelab direction, independent of naming.

The genuinely load-bearing insight from that analysis: **demand 1 (fast
failover) is not fully served by this repo today** — data replication assumes
the target app is already installed, and installing it is the manual,
error-prone step. `midclt call app.config` / `app.create` / `app.update` make
an extract→transform→apply pipeline feasible, structurally identical to the
existing VM pattern. That is [plan 06](06-app-definition-replication.md):
phase 0 verifies the midclt payload shapes on a live host, phase 1 audits and
converges the artificial config drift (ports included), phase 2 builds the
`app_definition_replication` subtask, phase 3 converges names.

Until phase 3 completes, the suffix logic stays in the code — plans 01–04
deliberately leave it untouched.

### Q4 — Complete rewrite in bash?

**No.** The architecture is genuinely sound: one module per concern, JSON-driven
config, property-driven ZFS selection, a consistent local/remote multiplexer.
What's wrong is fixable in place (plans 01–04). A big-bang rewrite would churn
battle-tested operational knowledge (container ordering, midclt quirks, the
dump-path contract) with **no test suite to catch regressions** — validation is
only possible by live runs against production data. Rewrites also reset the
"this line exists because of a hard-won lesson" history that the rules files
only partially capture.

### Q5 — Rewrite in Python?

**Not as a whole, not now.** The honest trade-off:

*What Python would genuinely improve:*
- Error handling: exceptions instead of the `Background_error`/kill-the-tail
  mechanism and its subshell blind spot (plan 01 item 6).
- No `eval`/quoting tower in `Execute_command` (remote commands are currently
  strings evaluated through 2–3 quoting layers).
- The VM-definition transform (140 lines of dense jq) becomes ~40 readable
  lines.
- Testability: the pure-logic parts could get offline unit tests.

*What argues against:*
- The bulk of the code is orchestration of external commands (`zfs`, `rsync`,
  `midclt`, `docker`, `ssh`) — Python turns each into `subprocess.run` calls
  with the same failure modes, minus bash's conciseness for exactly this job.
- The code is battle-tested; a rewrite by a smaller model, validated only by
  destructive live runs (`zfs destroy`, `rm -rf`, `vm.delete`), is precisely
  how a working backup system gets broken.
- Python on TrueNAS is available (zfs_autobackup already runs in a venv), so
  the *option* stays open.

**Recommended path:** stay bash, do plans 01–04. If appetite remains, do plan
05 — porting only the VM JSON transform to Python. It's the one component that
is pure data-in/data-out, testable offline on Windows against the saved
`tmp/vms/json/per_vm/*.json` files, and verifiable by diffing its output
against the jq implementation before switching. That's the correct first (and
possibly only) Python beachhead.
