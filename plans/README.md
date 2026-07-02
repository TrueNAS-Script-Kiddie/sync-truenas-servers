# Plans — analysis of sync_truenas_servers (2026-07-02)

Full-code review of `bin/`, `lib/`, `config/`, `Makefile` and all project md files.
Split into independent plans, ordered by value/risk. Each plan is written to be
implemented **one numbered item at a time** by any model, with exact locations,
replacement snippets, and verification steps.

| Plan | What | Risk | Do it? |
|---|---|---|---|
| [01-bug-fixes.md](01-bug-fixes.md) | 11 concrete bugs, each with a surgical fix | Low | **Yes, first** |
| [02-safety-and-notifications.md](02-safety-and-notifications.md) | Failure email, single-instance lock, log/dump retention | Low | **Yes** |
| [03-makefile-and-hygiene.md](03-makefile-and-hygiene.md) | Makefile is broken in 3 ways; add `.gitattributes` | Low | Yes (or delete the Makefile if SFTP is the real deploy path) |
| [04-simplifications.md](04-simplifications.md) | Behavior-preserving refactors: direction helper, dead code, `--app` validation at parse time | Medium | Yes, after 01 |
| [05-python-vm-transform.md](05-python-vm-transform.md) | Optional: port the 140-line jq transform to a small, offline-testable Python script | Medium | Optional |

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

**Recommendation: keep the suffixed names.** Reasoning:

- The suffix is only consumed in ~5 places (`Control_app_with_checks` name
  composition, the two docker greps in `immich_db.bash`, two `Control_app`
  calls). Dropping it saves perhaps 10–15 lines and one indirect expansion —
  a small win.
- TrueNAS (Goldeye) cannot rename an installed app; the name is fixed at
  install. Equalizing the names means **delete + reinstall of every app on
  both servers**, re-attaching host-path storage and reconfiguring. That is a
  risky, hours-long migration to buy a minor simplification.
- The suffix carries real operational information in a dual-active setup:
  container names, `midclt` output, and log lines self-identify which server
  they belong to. When you're ssh'd into the wrong box, `immich-backup` in a
  prompt/log is a cheap mistake-preventer — and "ran the sync in the wrong
  direction" is the single most dangerous operator error this system has.
- Pragmatic middle ground: **if an app ever gets reinstalled anyway**, install
  it without the suffix on both servers and make the suffix per-app config
  (e.g. an optional `"name_suffix": false` field in `apps.json`). Don't
  reinstall just for this.

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
