# Plan 07 — Shellcheck findings triage

**Status: done (2026-07-03).** Every tracked bash file was linted with
`shellcheck -x` (installed via `scoop install shellcheck`, matching the
VSCode extension's engine) and every finding triaged. `bash -n` and
`shellcheck -x` are clean on all touched files, confirmed via a real
`--test` run and a real production run.

Environment note kept for future reference: the VSCode shellcheck extension
needs **both** a `# shellcheck source=...` directive above each dynamic
`source` line **and** `"shellcheck.customArgs": ["-x"]` in
`.vscode/settings.json` — the directive alone isn't enough for the extension
to actually follow and lint sourced files.

## Real bugs fixed (10)

| Where | Issue | Fix |
|---|---|---|
| `rep_apps.bash` (SC2199) | `[[ " ${STOPPED_LIST[@]} " =~ ... ]]` — inconsistent array-join idiom vs. rest of codebase | `[*]` instead of `[@]` |
| `bin/sync_truenas_servers` (SC2086) | Unquoted `$0` in `nohup` re-exec | Quoted |
| `bin/sync_truenas_servers` (SC2155, elevated) | Silent `SCRIPT_DIR` resolution failure — every path in the script derives from it | Explicit `\|\| { echo ...; exit 1; }` check |
| `immich_db.bash` (SC2181) ×3 | `$?`-based checks on the DB backup/restore path — fragile if a line is ever inserted between command and check | `cmd \|\| Background_error` |
| `immich_db.bash` (SC2207) ×4 | Word-splitting into arrays, inconsistent with the `mapfile -t` idiom used elsewhere | `mapfile -t` |
| `immich_db.bash` (SC2027/SC2086) | Malformed (but accidentally-working) quoting in a log-preview string | Unambiguous quoting |
| `rep_filesystems.bash` (SC2164) | Unchecked `cd -` after the zfs_autobackup run | `\|\| Background_error` |
| `rep_vms.bash` (SC2295) ×2 | Unquoted expansion inside `${VAR#pattern}` — latent glob-injection if pool names ever change | Quoted the inner expansion |
| `rep_vms.bash` (SC2015) | `A && B \|\| C` where a failing B would wrongly trigger C | Restructured as `if/then/else` |
| `rep_filesystems.bash` (SC2046) | Unquoted command substitution (always yields one safe word) | Quoted, cosmetic |

## False positives — silenced with `# shellcheck disable=`

- **SC1091 + SC2034 cascade** (bin/sync_truenas_servers, cli.bash,
  config.local.bash): shellcheck can't follow the runtime-built `source`
  paths, so it can't see variables set in one file and used in another
  (`REMOTE_CMD`, `TASK`, `TEST_MODE`, `EMAIL_TO`, `LOCAL_SOURCE`/etc.). Fixed
  with `source=` directives + targeted `disable=SC2034` comments at each
  genuinely-cross-file assignment.
- **SC2076 ×4** (`rep_apps.bash`, `cli.bash` ×2, `rep_vms.bash`): shellcheck
  suggests removing quotes from the RHS of `=~`, which would turn a
  deliberate **literal substring match** into a regex match — wrong, since
  `APP_NAME`/VM names can contain regex metacharacters.
- **SC2086 on the `zfs_autobackup` invocation** (`rep_filesystems.bash`):
  **do not "fix" this one.** `SSH_OPTARGS`/`SNAPSHOT_OPTARGS`/etc. are each a
  string of multiple space-separated CLI flags, deliberately left unquoted so
  the shell word-splits them into separate arguments. Quoting them would pass
  the whole string as one argument and break the command. This is the one
  place in the whole codebase where the "obvious" shellcheck fix is wrong —
  verify after any future edit to this line that the logged `zfs_autobackup`
  command still shows multiple separate flags, not one quoted blob.
- **SC2034 nameref** (`rep_vms.bash`): `SOURCE_OR_TARGET_ALL_VM_JSON_REF` is a
  `local -n` bound to a dynamically-constructed target name — shellcheck
  can't resolve the write-through, but it's genuinely used (grepped, read in
  6+ places).

## Valid but deliberately not fixed

- **SC2009** (`ps -ef | grep` instead of `pgrep`, bin/sync_truenas_servers):
  correct suggestion, no bug (the `grep -v grep` guard already handles the
  self-match pitfall) — skip unless already touching that line.
- **SC2094** (`bin/sync_truenas_servers`, same file read+written in one
  pipeline): false positive — it's the intended live-log-streaming design
  (append + `tail -f`), not a clobber. Left undisabled on purpose since plan
  02 rewrites this exact line (adding `flock`) — add the disable then.
- **`REMOTE_STOPPED_LIST`/`LOCAL_STOPPED_LIST`** dynamic-scoping (SC2034
  shape): real usage via `eval`/indirect expansion, which plan 04 item 5
  replaces with namerefs — that refactor makes shellcheck understand it
  natively, so no disable comment was added (would just need removing later).

## Cross-referenced, not duplicated here

A few findings are already covered by other plans' real fixes rather than a
disable comment: `lib/common.bash` unreachable `return 1` (plan 04 item 7),
`rep_vms.bash` dead `TARGET_VM_JSON_FILE` assignment (plan 04 item 7),
`rep_filesystems.bash` `TARGET_SERVER_ID` SC2155 (superseded by plan 04 item
1's `Resolve_direction` helper). No action needed here.
