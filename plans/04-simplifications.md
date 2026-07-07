# Plan 04 — Simplifications (behavior-preserving)

Refactors only — output and behavior must be identical afterwards. Do this
**after plan 01** (some line numbers shift). Read `plans/README.md` guardrails
first; especially: keep the `LOCAL_*`/`REMOTE_*` variables — this plan *adds
derived* variables on top, it does not replace the direction model.

Implement items in order; each is independently verifiable with
`bash -n` + a `--test` run whose log should be diff-identical (modulo
timestamps) to a pre-change `--test` run. **Capture that baseline log first.**

## Done (2026-07-07)

The low-risk items — **item 4** and all of **item 7** — are implemented; all
touched files pass `bash -n`. Still open (larger / needs host verification):
**item 1** (direction/pool helper refactor), **item 2** (rollup path trick),
**item 3** (app-list validation), **item 5** (`eval`→nameref), **item 6**
(`midclt -job`, needs a host).

| # | Where | Change |
|---|---|---|
| 4 | `common.bash::Resolve_pool` | Dropped the dead `\|\| == "fast"` first-arg branch; empty input now `return 1` instead of silently `return 0` with an empty pool. Proven unreachable in the current program (hostname gate at `bin:34-37` + exhaustive direction `case` make every `$1` non-empty), so behavior is unchanged — kept as future-proofing. |
| 7 | `snap_rollup.bash:2,13` | Fixed stale `lib/rollup.bash` header comment; declared `ROLLUP_CMD` `local`. |
| 7 | `docker.bash:41-47` | Indented the `if`/`else` body. |
| 7 | `cli.bash` | `Help` takes an optional exit code (`exit "${1:-0}"`); missing `--task` now `Help 1` (non-zero) instead of `exit 0`. |
| 7 | `rep_vms.bash` | Deleted the write-only `TARGET_VM_JSON_FILE` assignment; added a `SKIPPED` counter so source/target-not-stopped skips are no longer miscounted as `FAILED`, and reported it in the summary line. |
| 7 | `immich_db.bash:68`, `common.bash:87` | Deleted unreachable code after `Background_error`. |
| 7 | `rep_filesystems.bash:13,22,29` | `IFS=", "` only ever joined on `,` (bash uses IFS's first char); changed to `IFS=","` to match actual output. |

⚠️ Not yet run: the `--test` baseline diff from the verification checklist —
do that on a TrueNAS host before this ships.

---

## 1. Factor the duplicated direction/pool resolution into one helper

The same ~14-line block appears verbatim in
[lib/rep_apps.bash:104-117](../lib/rep_apps.bash#L104-L117) and
[lib/rep_vms.bash:689-702](../lib/rep_vms.bash#L689-L702), and the same
direction knowledge is re-derived as `case`/`if` chains in
[lib/rep_filesystems.bash:106-114](../lib/rep_filesystems.bash#L106-L114),
[lib/snap_rollup.bash:15-21](../lib/snap_rollup.bash#L15-L21) and
[lib/rep_vms.bash:508-519](../lib/rep_vms.bash#L508-L519) (`Rsync_vm_file_disks`).

**Change:** add to `lib/common.bash`:

```bash
# Derives the source/target view of the direction model. Sets globals:
#   SOURCE_LOCATION / TARGET_LOCATION   ("local" or "remote")
#   SOURCE_SERVER_ID / TARGET_SERVER_ID ("master" or "backup")
# Pools stay per-module via Resolve_pool (POOL_TYPE differs per subtask).
function Resolve_direction() {
    [[ -n "${LOCAL_SOURCE}" ]] && SOURCE_LOCATION="local" || SOURCE_LOCATION="remote"
    [[ -n "${LOCAL_TARGET}" ]] && TARGET_LOCATION="local" || TARGET_LOCATION="remote"
    if [[ "${SOURCE_LOCATION}" == "local" ]]; then
        SOURCE_SERVER_ID="${LOCAL_SERVER_ID}"
        TARGET_SERVER_ID="${REMOTE_SERVER_ID}"
    else
        SOURCE_SERVER_ID="${REMOTE_SERVER_ID}"
        TARGET_SERVER_ID="${LOCAL_SERVER_ID}"
    fi
}
```

Call it once in `bin/sync_truenas_servers` right after
`Process_command_line_options "$@"`, and declare the four new globals next to
the other `declare`s. Then, module by module:

- `rep_apps.bash` / `rep_vms.bash`: delete the local declarations of
  `SOURCE_LOCATION`/`TARGET_LOCATION`/`SOURCE_SERVER_ID`/`TARGET_SERVER_ID`
  and the whole prepare-vars block; keep only:
  ```bash
    SOURCE_POOL="$(Resolve_pool "${SOURCE_SERVER_ID}" "fast")"
    TARGET_POOL="$(Resolve_pool "${TARGET_SERVER_ID}" "fast")"
  ```
  ⚠️ `SOURCE_LOCATION`, `SOURCE_POOL`, etc. are consumed by
  `Backup_immich_DB`/`Restore_immich_DB` through bash dynamic scoping — keep
  `SOURCE_POOL`/`TARGET_POOL` as (now-global-visible or still-local) variables
  with these exact names.
- `rep_filesystems.bash:106-114`: the `if/elif` collapses to:
  ```bash
    if [[ "${SOURCE_LOCATION}" == "remote" ]]; then
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-source truenas-${SOURCE_SERVER_ID}"
    else
        SSH_OPTARGS="--ssh-config ${SSH_CONFIG_FILE} --ssh-target truenas-${TARGET_SERVER_ID}"
    fi
    TARGET_POOL="$(Resolve_pool "${TARGET_SERVER_ID}" "${POOL_TYPE}")"
    [[ -n "${TARGET_POOL}" ]] || Background_error "ERROR: Failed to resolve target pool."
    TARGET_PARENT_DATASET="${TARGET_POOL}/encrypted-ds"
  ```
  Also `l_Toggle_mounts` line 48 (`TARGET_SERVER_ID=$(...)`) becomes redundant
  — delete the local override.
- `rep_vms.bash` `Rsync_vm_file_disks:508-519`: replace the `case
  "${TASK}:${LOCAL_SERVER_ID}"` with:
  ```bash
    [[ "${SOURCE_LOCATION}" == "remote" ]] && RSYNC_SOURCE="truenas-${SOURCE_SERVER_ID}:${SOURCE_PATH}"
    [[ "${TARGET_LOCATION}" == "remote" ]] && RSYNC_TARGET="truenas-${TARGET_SERVER_ID}:${TARGET_PATH}"
  ```
- `snap_rollup.bash:15-21`: `EXEC_MODE` becomes
  `EXEC_MODE="${TARGET_LOCATION}_verbose"`.

## 2. Replace the snap_rollup path-substitution trick with an explicit path — [lib/snap_rollup.bash:13](../lib/snap_rollup.bash#L13)

`${SCRIPT_DIR/${LOCAL_SOURCE}${REMOTE_SOURCE}/${LOCAL_TARGET}${REMOTE_TARGET}}`
rewrites e.g. `backup` → `master` *inside the deploy path* to guess where the
sibling `zfs-rollup` checkout lives **on the other server**. It works only
because the deploy path contains the pool name exactly once and both hosts
mirror the layout — maximally cryptic and fragile.

**Change:** state the assumption explicitly (after item 1's vocabulary):

```bash
    # zfs-rollup lives as a sibling of the deploy dir on BOTH hosts; the deploy
    # path differs between hosts only by the (normal) pool name.
    local ROLLUP_SCRIPT_DIR="${SCRIPT_DIR/$(Resolve_pool "${LOCAL_SERVER_ID}")/$(Resolve_pool "${TARGET_SERVER_ID}")}"
```

…or, better long-term, add `declare ROLLUP_SCRIPT_MASTER=...` /
`ROLLUP_SCRIPT_BACKUP=...` to `config.local.bash` and pick by
`TARGET_SERVER_ID`. Either way, build `ROLLUP_CMD` (make it `local`) from that
variable, and note that when `TARGET_LOCATION` is `local` the substitution is
a no-op (source pool ≠ target pool never both appear).

## 3. Validate `--app` at parse time and generate Help's app list — [lib/cli.bash](../lib/cli.bash)

- In `Help`, replace the hardcoded `immich`/`plex` lines (35-36) with:
  ```bash
    jq -r '.apps[].name' "${SCRIPT_DIR}/../config/apps.json" \
        | while read -r APP; do echo -e "${APP}\t\t\t\t\t\t\t\tLimit the Application Replication subtask to only copy ${APP^}."; done
  ```
- In `Process_command_line_options`, after the parse loop, validate every
  entry of `APP_LIST` against `jq -r '.apps[].name'` output and `exit 1` with
  a clear message on unknown names (bare `exit` is correct here — parse time,
  before backgrounding).
- Then delete the now-redundant `FOUND` search in
  [lib/rep_apps.bash:128-145](../lib/rep_apps.bash#L128-L145): the filter loop
  can keep only the `SELECTED_APP_JSON_LIST+=` matching, without the
  error branch.

## 4. Remove the dead `"fast"` guard in `Resolve_pool` — ✅ DONE (2026-07-07) — [lib/common.bash:43](../lib/common.bash#L43)

No caller passes `"fast"` as the *first* argument (all pass a server id, or
the `${LOCAL_TARGET}${REMOTE_TARGET}` concatenation which is always
master/backup). The `|| "${SERVER_TYPE}" == "fast"` branch is dead defensive
code that silently returns an empty pool. Reduce to:

```bash
    [[ -z "${SERVER_TYPE}" ]] && return 1
```

(Return 1, not 0 — combined with plan 01 item 6's call-site guards, an empty
input now fails loudly instead of propagating an empty pool.)

## 5. `eval` → nameref in `Control_app_with_checks` — [lib/rep_apps.bash:43-45,75](../lib/rep_apps.bash#L43-L45)

Replace the indirect read (`${!STOPPED_LIST_VAR}`) and the `eval` append with
one nameref per loop iteration:

```bash
        local -n STOPPED_LIST_REF="${LOCATION^^}_STOPPED_LIST"
        ...
        if [[ ! "${APP_STATE}" =~ ... ]]; then ...
        elif [[ " ${STOPPED_LIST_REF[*]} " =~ " ${APP_NAME} " ]]; then ...
        ...
        [[ "${ACTION}" == "stop" ]] && STOPPED_LIST_REF+=( "${APP_NAME}" )
```

Note: `local -n` inside a `for` loop errors on re-declaration in some bash
versions — use `unset -n STOPPED_LIST_REF` at the end of each iteration, or
declare once before the loop and re-point with `STOPPED_LIST_REF=…`? Namerefs
can't be re-pointed by assignment — **safest pattern:** `unset -n
STOPPED_LIST_REF` as the first statement of the loop body, then `local -n`.
Verify with a two-app `--test` run that stop/start bookkeeping still works.

## 6. OPTIONAL: `midclt call -job` instead of the poll loop — [lib/rep_apps.bash:14-21](../lib/rep_apps.bash#L14-L21)

`midclt call -job app.stop <name>` blocks until the job finishes and exits
non-zero on failure, which would delete the whole JOBID/poll machinery.
**Verify on a host first** (`midclt call -job app.stop immich-<id>` /
`app.start`) — check exit codes and that output doesn't pollute the log
format. If output is noisy, keep the poll loop (plan 01 item 3 already fixed
its real defect). Do not attempt this without host access.

## 7. Small-stuff bucket (all cosmetic, zero behavior change) — ✅ DONE (2026-07-07)

- [lib/snap_rollup.bash:2](../lib/snap_rollup.bash#L2): header comment says
  `lib/rollup.bash` — stale filename.
- [lib/snap_rollup.bash:13](../lib/snap_rollup.bash#L13): `ROLLUP_CMD` is not
  declared `local`.
- [lib/docker.bash:41-47](../lib/docker.bash#L41-L47): body of the `if` is
  not indented.
- [lib/cli.bash:127](../lib/cli.bash#L127): missing `--task` falls into
  `Help`, which `exit 0`s — misuse should exit non-zero. Give `Help` an
  optional exit-code argument (`exit "${1:-0}"`) and call `Help 1` there.
- [lib/rep_vms.bash:718](../lib/rep_vms.bash#L718): `TARGET_VM_JSON_FILE` is
  assigned but never read — delete the assignment (the extraction step still
  *writes* the `.target.json` files; those are debug artifacts, keep them).
- [lib/rep_vms.bash:728-731](../lib/rep_vms.bash#L728-L731): a source-VM-not-
  stopped skip is counted as FAILED but printed as "skipped" — add a `SKIPPED`
  counter and include it in the summary line.
- [lib/immich_db.bash:60-63](../lib/immich_db.bash#L60-L63): `break` after
  `Background_error` is unreachable — delete.
- [lib/common.bash:32](../lib/common.bash#L32): `return 1` after
  `Background_error` is unreachable — delete.
- [lib/rep_filesystems.bash:13,22,29](../lib/rep_filesystems.bash#L13):
  `( IFS=", "; echo "... ${IMPACTED_DATASETS[*]}" )` joins with `,` only (bash
  uses the first IFS char) — either accept and change IFS to `','`, or use
  `printf` if you want `", "`.

## Explicitly NOT recommended (in this plan)

- **Renaming apps to drop the `-master`/`-backup` suffix as a manual,
  big-bang reinstall** — the end-state of same names/same ports on both hosts
  *is* endorsed, but only via the phased pipeline in
  [plan 06](06-app-definition-replication.md) (phase 3 adds a per-app
  `per_server_name_suffix` transition flag). Until then, this plan leaves all
  suffix-composition logic untouched.
- **`set -u` / `set -o pipefail` globally** — the codebase deliberately relies
  on unset-is-empty (`LOCAL_SOURCE` vs `REMOTE_SOURCE`, `${TEST_MODE:+...}`)
  and long pipelines whose early stages may non-fatally fail. Retrofitting
  would touch nearly every line for little gain over plan 01 item 6's targeted
  guards.
- **Restructuring `Execute_command`'s eval/string design** — it's the
  load-bearing convention of the whole codebase; changing it is a rewrite in
  disguise (see plan 05 / README Q5 instead).

---

## Verification checklist

1. Baseline: capture a full `--test --task=master_to_backup` log **before**
   starting.
2. After each item: `bash -n` all touched files; re-run the same `--test`
   command; `diff` the logs — only timestamps and this plan's intended
   cosmetic lines may differ.
3. Item 1 especially: verify an app replication `--test` run still shows
   correct source/target pools in every rsync/echo line, and a VM `--test`
   run still resolves rsync remote prefixes correctly in both directions
   (run once with `--task=master_to_backup` and once with
   `--task=backup_to_master`).
