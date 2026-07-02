# Plan 07 — Shellcheck findings triage

**Coverage: complete (2026-07-03).** `shellcheck -x` (CLI, installed via
`scoop install shellcheck`, matching the VSCode extension's engine) was run
against every tracked bash script in the repo, plus the untracked
`config/config.local.bash`:

```
bin/sync_truenas_servers  config/config.example.bash  config/config.local.bash
lib/cli.bash  lib/common.bash  lib/docker.bash  lib/immich_db.bash
lib/rep_apps.bash  lib/rep_filesystems.bash  lib/rep_vms.bash  lib/snap_rollup.bash
```

`config/config.example.bash`, `config/config.local.bash`, and
`lib/snap_rollup.bash` are fully clean. Every finding from every other file is
triaged below. Each is classified below. **A large share are false positives
caused by shellcheck analyzing files in isolation** — it can't see across the
flat `source lib/*.bash` chain that `bin/sync_truenas_servers` builds at
runtime (this cross-file sharing is intentional; see
`architectural_patterns.md` "Module sourcing is flat and order-sensitive").
Item 1 fixed the root cause of that noise for `bin/sync_truenas_servers`
itself; item 1b explains why the *same class* of false positive still shows
up when a `lib/*.bash` file is linted on its own, and why that's structural
rather than something to patch away file-by-file.

Read `plans/README.md` guardrails first. Independent items; verify with
`bash -n` after each. **To reproduce any of this yourself:**
`shellcheck -x <file>` (shellcheck is now installed via scoop on this
machine).

---

## 1. Root cause of ~11 false positives: shellcheck can't follow the sourcing chain — ✅ DONE (2026-07-03)

SC1091 ("Not following: X was not specified as input") fired on every
`source "${SCRIPT_DIR}/../lib/*.bash"` line in `bin/sync_truenas_servers`
(lines 12, 58–65) because the path is built from a runtime variable
(`${SCRIPT_DIR}`), which shellcheck can't resolve statically. That failure to
follow is what then caused the **cascade of SC2034 "appears unused"** false
positives for every variable that's only consumed in a sourced `lib/*.bash`
file: `REMOTE_CMD`, `TASK`, `TEST_MODE`, `EXEC_DATE`, `DB_RESTORE_LOG`,
`APP_LIST`, `VM_LIST` in `bin/sync_truenas_servers`. None of these were real
— every one is read in a sourced file (e.g. `REMOTE_CMD` is used in
`Execute_command`, `lib/common.bash:27-28`).

**Applied:** a `# shellcheck source=...` comment above each `source` line in
`bin/sync_truenas_servers` (pointing the `config.local.bash` source at the
tracked `config.example.bash`, which has the same shape, since the real file
is gitignored), **plus** `"shellcheck.customArgs": ["-x"]` in
`.vscode/settings.json` — the directive alone resolves *what* the path would
be, but the VSCode extension additionally needs `-x` (external sources)
enabled before it will actually open and follow a resolved path. Confirmed
both were needed: after adding just the directives, diagnostics still showed
"see shellcheck -x" on every source line; only after enabling `-x` and
reloading the window did they clear.

**Verified 2026-07-03:** re-lint of `bin/sync_truenas_servers` (open in the
editor) now returns **zero** diagnostics, down from the original ~15 hits.

**One thing this fix does NOT cover** — `EMAIL_TO` in `config/config.local.bash`
(and `config/config.example.bash`) needed a *separate* fix (item 1a below).
The direction there is reversed: those files are the ones being *sourced*,
and `-x`/`source=` only makes a file follow its own sources — it can't make
shellcheck aware that some other script sources *this* file and uses its
variables. That's a distinct, unfixable-by-directive-here false positive.

### 1a. `EMAIL_TO` in `config/config.local.bash` / `config/config.example.bash` — ✅ DONE (2026-07-03)

Fixed with an inline disable directive in both files (the tracked example
and the local file), since this is exactly the documented use case for a
targeted `disable` comment rather than a `source=` hint:

```bash
#!/usr/bin/bash
# shellcheck disable=SC2034  # consumed by bin/sync_truenas_servers (and lib/common.bash), which sources this file
declare EMAIL_TO="..."
```

Not independently re-verified live (the file wasn't open in an editor tab at
verification time, so no fresh diagnostics were computed for it) — the
directive is the textbook-correct fix for this exact situation, so treat as
done; spot-check next time `config.local.bash`/`config.example.bash` is open
in the editor.

### 1b. Structural: every `lib/*.bash` file will show false "unused" hits when linted *standalone* — no per-file fix; use the canonical command instead

Linting `lib/cli.bash` alone (`shellcheck -x lib/cli.bash`) reports SC2034
"appears unused" for `RUNNING_IN_BACKGROUND`, `LOG_FILE`, `TEST_MODE`,
`LOCAL_SOURCE`, `REMOTE_SOURCE`, `LOCAL_TARGET`, `REMOTE_TARGET`
([lib/cli.bash:113,114,118,155,156](../lib/cli.bash#L113)). All seven are
genuinely used — by `bin/sync_truenas_servers` and by sibling `lib/*.bash`
files that get sourced after `cli.bash` in the flat namespace. **This is the
mirror image of item 1**, not a new bug: item 1 fixed the direction where
`bin/sync_truenas_servers` sources a file and needs to see into it; this is
the direction where a `lib/*.bash` file's own variables are consumed by
*its consumer* (or a sibling), and shellcheck has no static way to look
"outward" from a file to whoever sources it. `# shellcheck source=` directives
can't help here — there's no `source` line to annotate in `cli.bash` itself.

**Verified this isn't a live problem in practice:** `shellcheck -x
bin/sync_truenas_servers` (the actual entry point, which is how the program
is really executed) reports **zero** of these seven — confirming that once
everything is linted together through the real entry point, cross-file usage
is fully understood and there's no real dead-code issue here.

**Recommendation — don't chase this with per-variable `disable` comments**
(unlike the one-off `EMAIL_TO` case in 1a, this would mean dozens of comments
sprinkled across every `lib/*.bash` file, and would risk *hiding* a real
future dead-variable bug under the noise). Instead, adopt **`shellcheck -x
bin/sync_truenas_servers` as the project's one authoritative lint command**,
and accept that opening an individual `lib/*.bash` file standalone in the
editor will show some expected-noisy "unused" hits for its cross-file-consumed
globals. Concretely:
- Add one line to `AGENTS.md`/`CLAUDE.md` "Essential commands":
  `shellcheck -x bin/sync_truenas_servers   # lints every lib/*.bash file together, in context`
- Optional: add a `lint` target to the `Makefile` that runs the same command,
  so it's discoverable without reading the docs.

This note also applies to the `SOURCE_OR_TARGET_ALL_VM_JSON_REF` and
`TARGET_VM_JSON_FILE` findings in section 4 below — read that section for the
two that are real vs. this same structural cause.

---

## 2. Real, worth fixing

### 2a. `rep_apps.bash:55` — inconsistent array-join idiom (SC2199)

```bash
elif [[ " ${STOPPED_LIST[@]} " =~ " ${APP_NAME} " ]]; then
```

The rest of the codebase uses the explicit, correct form for this exact
"space-padded membership test" idiom — see
[lib/cli.bash:107](../lib/cli.bash#L107) and
[lib/cli.bash:110](../lib/cli.bash#L110): `"${APP_LIST[*]}"` (star, not at).
Under the default `IFS`, `[@]` and `[*]` produce the same space-joined string
inside a quoted context, so this is **not a live bug** — but it's a latent one
(breaks if `IFS` is ever non-default) and it's the one place in the codebase
that doesn't match the established convention. Fix for consistency:

```bash
elif [[ " ${STOPPED_LIST[*]} " =~ " ${APP_NAME} " ]]; then
```

### 2b. `bin/sync_truenas_servers:108` — unquoted `$0` (SC2086)

```bash
nohup $0 --running_in_background "${LOG_FILE}" "${BACKUP_OPTIONS[@]}"  >>"${LOG_FILE}" 2>&1 & { sleep 1; tail -f "${LOG_FILE}"; }
```

If the script is ever invoked via a path containing a space, the unquoted
`$0` word-splits and breaks re-exec. Fix: quote it — `nohup "$0" ...`.

**Note:** plan 02 item 2 (flock) already rewrites this exact line and
includes this quote fix as part of that change. If you do plan 02, this is
already covered — don't do it twice. If you're doing shellcheck cleanup
*without* plan 02, apply just the quoting here.

### 2c. `bin/sync_truenas_servers:6` — `SCRIPT_DIR` failure is silent (SC2155, elevated)

```bash
declare SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
```

SC2155 ("declare and assign separately") fires on six lines in this file
(5, 6, 23, 44, 45, 46). For `LOG_FILE`/`DB_RESTORE_LOG`/`EXEC_DATE`
(`date +...`) and `SCRIPT_FILENAME` (`basename`), a failure is
near-impossible and the impact of missing it is low — not worth the noise of
splitting those. **`SCRIPT_DIR` is the one exception**: every path in the
script (`config/`, `lib/`, `logs/`) is built from it, so a silent failure
here (leaving `SCRIPT_DIR` empty) doesn't abort — it cascades into confusing
downstream errors about missing files in `/../lib` etc. Add an explicit
check right after:

```bash
declare SCRIPT_DIR
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)" \
    || { echo "ERROR: Failed to resolve SCRIPT_DIR"; exit 1; }
```

Leave the other five SC2155 hits alone — fixing them is real but low-value
churn for no behavior change.

### 2d. `immich_db.bash` — `$?`-based checks on the DB backup/restore path (SC2181) — elevated priority

```bash
Execute_command "${SOURCE_LOCATION}" "docker exec -i ... pg_dumpall ..."
[[ "$?" != "0" ]] && Background_error "ERROR: DB backup failed."
```

Three instances: [lib/immich_db.bash:32](../lib/immich_db.bash#L32),
[:34](../lib/immich_db.bash#L34), [:96](../lib/immich_db.bash#L96). Shellcheck
flags this as style (SC2181), and it's **not currently broken** — there's no
intervening command between the `Execute_command` call and the `$?` check on
any of the three, so the exit code being tested is correct today. The reason
to fix it anyway: this is uniquely fragile in *this specific file*, because
any future edit that inserts so much as a diagnostic `echo` between the
command and the check (an easy, innocent-looking change) would silently
start checking that `echo`'s exit status (always 0) instead of the real
command's — turning a real DB backup/restore failure into a silent no-op on
the one path the user has explicitly flagged as load-bearing and
correct-by-hard-experience. The fix removes the fragility with zero behavior
change:

```bash
Execute_command "${SOURCE_LOCATION}" "docker exec -i ... pg_dumpall ..." \
    || Background_error "ERROR: DB backup failed."
```

(Same transform for the `mv` at line 34 and the `gunzip | ... | psql` at
line 96.) The equivalent, lower-stakes instance at
[lib/rep_vms.bash:253](../lib/rep_vms.bash#L253) (guarding a jq transform,
not DB safety) is **not** included here — real but much lower priority; skip
unless you're already restructuring that function.

### 2e. `immich_db.bash` — array-from-command-substitution inconsistent with the rest of the codebase (SC2207)

```bash
local CONTAINERS_TO_STOP=( $(Execute_command "${SOURCE_LOCATION}" "docker ps -a ... | grep -E '...'") )
```

Four instances: [lib/immich_db.bash:10](../lib/immich_db.bash#L10),
[:11](../lib/immich_db.bash#L11), [:73](../lib/immich_db.bash#L73),
[:74](../lib/immich_db.bash#L74). Docker container names can't contain
whitespace, so this doesn't misbehave today — but every other place in the
codebase that captures multi-line command output into an array uses
`mapfile -t VAR < <(cmd)` (see `Extract_vm_definitions`, `Tag_vm_disks`, the
`IMPACTED_DATASETS` reads in `rep_filesystems.bash`). This file is the one
holdout using the older word-splitting form. Fix, e.g.:

```bash
local -a CONTAINERS_TO_STOP
mapfile -t CONTAINERS_TO_STOP < <(Execute_command "${SOURCE_LOCATION}" "docker ps -a --format '{{.Names}}' | grep -E 'immich-${SOURCE_SERVER_ID}-(server|machine-learning|redis|permissions)-[0-9]+'")
```

Apply the same shape to the other three.

### 2f. `immich_db.bash:28` — malformed quoting in a log-preview string (SC2027/SC2086)

```bash
echo "Executing on TrueNAS-${SOURCE_SERVER_ID^}: docker exec -i "${CONTAINERS_TO_START[0]}" bash -c 'pg_dumpall ... > "/var/lib/postgresql/${EXEC_DATE}_immich_backup.dump.sql.gz"'"
```

This is a **preview/log line only** — it's never executed, just printed for
the operator — but the quoting is genuinely malformed: the embedded
`"${CONTAINERS_TO_START[0]}"` closes and reopens the outer string rather than
nesting inside it. It happens to still print the intended text today (bash
string-concatenates adjacent quoted segments), which is exactly what makes
this fragile: it *looks* broken, and a future edit "cleaning up" the quoting
without understanding why it currently works could actually break it. Given
this project relies on the log as the primary post-hoc diagnostic record
(directly relevant if `Backup_immich_DB` ever fails and someone reads the log
to figure out what happened), it's worth making the quoting unambiguous:

```bash
echo "Executing on TrueNAS-${SOURCE_SERVER_ID^}: docker exec -i \"${CONTAINERS_TO_START[0]}\" bash -c 'pg_dumpall --clean --if-exists --username=immich | gzip > \"/var/lib/postgresql/${EXEC_DATE}_immich_backup.dump.sql.gz\"'"
```

This is the same theme as [plan 01 item 11](01-bug-fixes.md) (echoed preview
drifting from/misrepresenting the real command in this exact file) — do both
together if you're already in this function.

### 2g. `rep_filesystems.bash:83` — unchecked `cd -` (SC2164)

```bash
cd - >/dev/null
```

If this `cd` back fails (directory removed mid-run, permissions, etc.), the
function silently continues running from the wrong working directory. Cheap
fix, matches the project's own error convention:

```bash
cd - >/dev/null || Background_error "ERROR: Failed to cd back after zfs_autobackup run."
```

### 2h. `rep_vms.bash:340,412` — unquoted expansion inside a `${VAR#pattern}` removal (SC2295)

```bash
REL_DISK_PATH="${DEVNODE_DISK_PATH#/dev/zvol/${POOL_NAME}/}"
...
[[ "${REL_SOURCE_VM_PATH}" == "${TARGET_VM_PATH#*/${TARGET_POOL}/}" ]]
```

The right-hand side of `#`/`##` is always glob-pattern matching, regardless
of the outer double quotes — only a *separately*-quoted inner expansion is
treated as a literal string. Since `Resolve_pool` only ever returns one of a
few fixed literal names (`master-pool`, `ssdmaster-pool`, `backup-pool`, none
containing glob metacharacters), there's **no live bug today** — but it's a
latent one if pool naming ever changes. Cheap, safe fix:

```bash
REL_DISK_PATH="${DEVNODE_DISK_PATH#/dev/zvol/"${POOL_NAME}"/}"
...
[[ "${REL_SOURCE_VM_PATH}" == "${TARGET_VM_PATH#*/"${TARGET_POOL}"/}" ]]
```

### 2i. `rep_vms.bash:307` — `A && B || C` is not if/then/else (SC2015) — low priority, informational

```bash
Execute_command "${SOURCE_LOCATION}" "zfs inherit autobackup:${TASK_SCOPE} \"${DS}\"" \
    && echo "  'autobackup:${TASK_SCOPE}'-tag removed from '${DS}'" \
    || Background_error "ERROR: Failed to remove 'autobackup:${TASK_SCOPE}'-tag from ${DS}"
```

Classic pitfall: if `Execute_command` (A) succeeds but the `echo` (B)
somehow fails, `Background_error` (C) fires anyway — a false failure abort
even though the real operation (removing the ZFS tag) succeeded. In practice
`echo` failing here would require something like a closed stdout, so the
real-world risk is near zero — but the consequence (aborting an otherwise-
successful run on a misleading error) is disproportionate to that risk.
Low priority; fix by restructuring as `if ... ; then ... ; else ... ; fi` if
you're touching this function anyway. **Not audited elsewhere** — this exact
shape likely recurs in other `cmd && echo ... || Background_error` call sites
across `rep_vms.bash`; shellcheck only flags the instances its heuristics
catch, so treat this as a spot example of a pattern, not an exhaustive list.
A full audit of this idiom is out of scope for this plan; consider it for a
future pass if it's ever bitten in practice.

### 2j. `rep_filesystems.bash:117` — unquoted command substitution (SC2046) — optional, near-zero risk

```bash
Execute_command $([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote) \
```

The substitution only ever yields the single word `local` or `remote` — no
spaces, no glob characters — so this doesn't misbehave today. Quoting is free
and slightly more defensible style if you're already touching this line:

```bash
Execute_command "$([[ -n "${LOCAL_SOURCE}" ]] && echo local || echo remote)" \
```

Not worth a dedicated pass on its own.

---

## 3. False positives — no action needed (documented so they aren't "fixed" incorrectly)

### 3a. `rep_apps.bash:55` — SC2076 "remove quotes from RHS of =~"

```bash
elif [[ " ${STOPPED_LIST[@]} " =~ " ${APP_NAME} " ]]; then
```

Shellcheck suggests removing the quotes so `=~` treats the RHS as a regex.
**Do not do this.** The quoting here is deliberate: it forces `=~` into a
*literal substring* match instead of a regex match, which matters because
`APP_NAME` comes from `config/apps.json` and could contain characters that
are regex metacharacters (`.`, `+`, etc.) without being intended as one. This
is the same idiom used (correctly) in `cli.bash:107,110`. Only item 2a above
(the `[@]`→`[*]` swap) is worth doing here; leave the quoting as-is.

### 3b. `bin/sync_truenas_servers:108` — SC2094 "read and write the same file in the same pipeline"

```bash
nohup $0 ... >>"${LOG_FILE}" 2>&1 & { sleep 1; tail -f "${LOG_FILE}"; }
```

This isn't a `sort file > file`-style clobber. It's two separate processes —
the backgrounded child **appending** to `LOG_FILE` and the foreground
`tail -f` **following** it — which is exactly the intended live-log-streaming
design. Shellcheck's heuristic just sees the same filename twice in one
compound command line and flags it defensively. No change needed.

### 3c. `bin/sync_truenas_servers:76` — SC2009 "consider pgrep instead of grepping ps"

```bash
TAIL_PID="$(ps -ef | grep "tail -f ${LOG_FILE}" | grep -v grep | awk '{print $2}')"
```

Valid modernization suggestion, not a bug — the current `grep -v grep`
self-match guard already handles the classic pitfall. Optional cleanup, not
worth doing on its own:

```bash
TAIL_PID="$(pgrep -f "tail -f ${LOG_FILE}")"
```

Skip unless you're already touching this line for another reason (e.g. plan
02 item 2 touches the surrounding block).

### 3d. All SC2034 "appears unused" findings, and the SC1091 findings that cause them

Covered by item 1 above — these are the false-positive cascade, not
independent findings. `REMOTE_STOPPED_LIST`/`LOCAL_STOPPED_LIST` in
`rep_apps.bash:148-149` are the one exception worth a note: those *are*
real dynamic-scoping usages (via `eval`/indirect expansion in
`Control_app_with_checks`), which is precisely the pattern plan 04 item 5
replaces with namerefs. Doing plan 04 item 5 will make shellcheck understand
the usage natively (namerefs are far more transparent to static analysis
than `eval` + indirect `${!VAR}` expansion) — another point in favor of that
refactor, beyond the readability win already described there.

### 3e. `cli.bash:107,110` and `rep_vms.bash:752` — SC2076 "remove quotes from RHS of =~"

Same class as 3a, same rationale, same "do not fix": these are the two
places that already use the *correct* `[*]` form of the space-padded
membership-test idiom (`cli.bash:107,110` is in fact what item 2a's fix in
`rep_apps.bash` is matching *toward*), and `rep_vms.bash:752`
(`[[ ! " ${PROCESSED_VM_LIST[*]} " =~ " ${REQ} " ]]`) is another correct
instance of the same pattern. Shellcheck's SC2076 suggestion is generically
wrong for this idiom regardless of `[@]` vs `[*]` — leave all three as-is.

### 3f. `rep_filesystems.bash:77` — SC2086 on the `zfs_autobackup` invocation — DO NOT "fix"

```bash
if ${ZFS_AUTOBACKUP_COMMAND}${TEST_MODE:+ --test} --verbose ${SSH_OPTARGS} ${SNAPSHOT_OPTARGS} ${ZFS_OPTARGS} ${ZFS_AUTOBACKUP_OPTARGS} ${ZFS_AUTOBACKUP_TASK_OPTARGS}; then
```

Flagging this explicitly because it's exactly the kind of "fix" that looks
obviously correct and would quietly break the tool: `SNAPSHOT_OPTARGS`,
`ZFS_OPTARGS`, `SSH_OPTARGS`, etc. are each a **string of multiple
space-separated CLI flags** (e.g.
`--rollback --keep-source=0 --keep-target=0 --allow-empty --snapshot-format {}-%Y-%m-%d_%H-%M`),
deliberately left unquoted so the shell word-splits them into separate
arguments to `zfs_autobackup`. Quoting any of them — the shellcheck-suggested
fix — would pass the whole multi-flag string as one single argument and
break the invocation. Leave unquoted. (This is the one item in this whole
plan where "fixing the shellcheck warning" would introduce a real bug where
none exists today — treat it as the canary for why every item in this
document was verified against context rather than applied mechanically.)

### 3g. `rep_vms.bash:82` — `SOURCE_OR_TARGET_ALL_VM_JSON_REF` "appears unused" (SC2034) — false positive (dynamic nameref target)

```bash
local -n SOURCE_OR_TARGET_ALL_VM_JSON_REF="${SOURCE_OR_TARGET}_ALL_VM_JSON"
...
SOURCE_OR_TARGET_ALL_VM_JSON_REF="${ALL_VM_JSON}"
```

This *is* used — it's a nameref (`local -n`) bound to a **dynamically
constructed** target name (`"${SOURCE_OR_TARGET}_ALL_VM_JSON"`, resolving to
`SOURCE_ALL_VM_JSON` or `TARGET_ALL_VM_JSON` at runtime). Assigning to the
nameref writes through to that global, which is read extensively elsewhere
(`lib/rep_vms.bash:335,372,397,404,416,480,708` — confirmed by grep).
Shellcheck can't resolve a nameref whose target name is itself a variable
expansion, so it can't see the write-through counts as a use. No fix needed
— this is the nameref-specific sibling of the item 1b phenomenon (shellcheck
losing track of dynamic cross-references), not a dead-code bug.

---

## 4. Findings already tracked by other plans (no new action — cross-referenced here for completeness)

These showed up in the full-codebase shellcheck pass but duplicate something
already scheduled elsewhere. Listed so this plan can claim complete coverage
without re-describing the fix twice.

| Finding | File:line | Already covered by |
|---|---|---|
| SC2317 unreachable `return 1` after `Background_error` | [lib/common.bash:32](../lib/common.bash#L32) | [Plan 04 item 7](04-simplifications.md) |
| SC2034 `TARGET_VM_JSON_FILE` assigned, never read | [lib/rep_vms.bash:718](../lib/rep_vms.bash#L718) | [Plan 04 item 7](04-simplifications.md) — confirmed genuinely dead (grepped; only the one assignment site exists) |
| SC2155 `POOL_NAME` declare+assign | [lib/rep_vms.bash:327](../lib/rep_vms.bash#L327) | [Plan 01 item 6](01-bug-fixes.md) already adds an empty-value guard here; the SC2155 form itself is cosmetic and not separately worth splitting |
| SC2155 `TARGET_SERVER_ID` declare+assign | [lib/rep_filesystems.bash:48](../lib/rep_filesystems.bash#L48) | [Plan 04 item 1](04-simplifications.md) deletes this local override entirely (superseded by `Resolve_direction`) |
| SC2155 (misc: `START_TIME` in `docker.bash:9`/`immich_db.bash:44`) | low-priority style, same bucket as [item 2c](#2c-binsync_truenas_servers6--script_dir-failure-is-silent-sc2155-elevated)'s "leave alone" five | not tracked elsewhere; intentionally skipped, same reasoning as 2c |
| SC2181 `$?` check on jq transform validity | [lib/rep_vms.bash:253](../lib/rep_vms.bash#L253) | Same style class as [item 2d](#2d-immich_dbbash--based-checks-on-the-db-backup-restore-path-sc2181--elevated-priority), but lower stakes (not the DB path) — optional, not scheduled |

---

## Verification checklist

1. Apply item 1 (source directives) and item 1b (canonical lint command
   doc line); re-run `shellcheck -x bin/sync_truenas_servers` — confirm the
   SC1091/SC2034 cascade is gone (already verified 2026-07-03).
2. Apply 2a–2j individually; `bash -n` after each. 2d and 2f touch the Immich
   DB path specifically — after those two, do a real (or `--test`)
   `app_replication` run for `immich` and confirm the log preview lines and
   backup/restore both still succeed.
3. Full `--test --task=master_to_backup` run after all of section 2 — behavior
   must be unchanged (none of 2a/2b/2c/2e/2g/2h/2j alter semantics under
   normal input; 2c/2d/2g only change behavior on an already-fatal-in-practice
   failure mode; 2f/2i are log-text/robustness only).
4. Do **not** apply the 3f "fix" — verify by running `--test` after any other
   change in `rep_filesystems.bash` that the `zfs_autobackup` command line in
   the log still shows multiple separate flags, not one long quoted blob.
