# Plan 05 — OPTIONAL: port the VM-definition transform to Python

**Status: optional.** Do plans 01–04 first. See `plans/README.md` Q5 for why a
full Python rewrite is not recommended but this one component is the right
candidate: `Transform_vm_definition` ([lib/rep_vms.bash:114-282](../lib/rep_vms.bash#L114-L282))
is ~170 lines containing three near-duplicate jq programs (decision log,
preview print, apply) plus a validation pass. It is **pure data-in/data-out**
(JSON file + mapping file → JSON file), which makes it:

- testable **offline on the Windows desktop** — no TrueNAS, no root, no
  destructive commands;
- verifiable by **byte/structural diff** against the existing jq output before
  the switch — the ideal shape of task for any model.

## Scope

Replace only the *three jq programs* inside `Transform_vm_definition` with one
script: `bin/transform_vm_definition.py`. Everything else in `rep_vms.bash`
stays. No new dependencies — Python 3 stdlib (`json`, `argparse`) only.

## Interface (fixed — do not deviate)

```
python3 bin/transform_vm_definition.py \
    --source-json <SOURCE_VM_JSON_FILE> \
    --map-file    <config/vm_device_mappings.json> \
    --source-id   master|backup \
    --target-id   master|backup \
    --output      <TRANSFORMED_VM_JSON_FILE>
```

- stdout: the human-readable mapping preview (same information as today's
  step 2 table: `DTYPE  attribute  old → new`, plus the `WARNING path … (no
  mapping rule)` lines for unmapped DISK paths).
- exit 0 on success; exit 1 with a clear message on stderr on any failure
  (missing file, unparsable JSON, post-transform validation failure).
- Validation from today's step 5 moves inside the script: every device must
  be an object with a non-null `attributes.dtype`.

## Transformation rules (replicate exactly — from the jq)

For the VM object (accept both a single object with `.devices` and a
one-element array `[ {..devices..} ]`, as the jq's `(.devices // .[0].devices)`
does), for each device:

1. `dtype = device.attributes.dtype`; look up `map_file[dtype]` — if absent,
   device passes through unchanged (but for `DISK` devices emit the WARNING if
   `attributes.path` matches no `scope` of any `DISK.path` rule).
2. For each attribute key in the map entry except `_doc`: take
   `old = str(device.attributes[key] or "")`. Find the **first** rule in the
   list where:
   - `scope` is null **or** `scope in old` (substring), **and**
   - `old == str(rule[source_id])` **or** `str(rule[source_id]) in old`.
3. If matched: when `old == str(rule[source_id])`, set the attribute to
   `rule[target_id]` **preserving its JSON type** (e.g. DISPLAY ports stay
   numbers); when it's a substring match, replace the **first occurrence** of
   `str(rule[source_id])` in `old` with `str(rule[target_id])` (plain string
   replace, count=1 — the jq used regex `sub`, but all current rule values are
   regex-safe; plain replace is the intended semantics) and store the
   resulting **string**.
4. No match → attribute unchanged.

## Call-site change in `Transform_vm_definition`

Keep the function; its body becomes:

```bash
    local MAP_FILE="${SCRIPT_DIR}/../config/vm_device_mappings.json"
    [[ -f "${MAP_FILE}" ]] || Background_error "ERROR: mapping file not found: ${MAP_FILE}"

    echo "Transforming source VM definition into destination VM definition..."
    if ! python3 "${SCRIPT_DIR}/transform_vm_definition.py" \
            --source-json "${SOURCE_VM_JSON_FILE}" \
            --map-file "${MAP_FILE}" \
            --source-id "${SOURCE_SERVER_ID}" \
            --target-id "${TARGET_SERVER_ID}" \
            --output "${TRANSFORMED_VM_JSON_FILE}" | sed 's/^/  /'; then
        Background_error "ERROR transforming ${SOURCE_VM_JSON_FILE}"
    fi
    echo "  truenas-${LOCAL_SERVER_ID} - Transformed source json into ${TRANSFORMED_VM_JSON_FILE}"
    echo
```

⚠️ `pipefail` is not set, so `if ! python3 ... | sed` tests **sed's** status,
not python's. Either use `PIPESTATUS`:

```bash
    python3 ... | sed 's/^/  /'
    [[ "${PIPESTATUS[0]}" -eq 0 ]] || Background_error "ERROR transforming ${SOURCE_VM_JSON_FILE}"
```

or have the script indent its own output and drop the sed. Use `PIPESTATUS`.

Note `python3` on TrueNAS SCALE is the system interpreter — verify once with
`which python3` on the host; stdlib-only means no venv is needed.

## Acceptance harness (the important part)

Before switching the call site, prove equivalence on real data:

1. Copy a set of real inputs from the host: `tmp/vms/json/per_vm/*.source.json`
   (they exist after any VM replication run, including `--test`).
2. Write a throwaway compare script (can live in the scratchpad, not the repo)
   that, for each sample and for **both directions**
   (`master→backup`, `backup→master`):
   - runs the *existing* jq step-3 program (extract it verbatim into a file)
     → `expected.json`;
   - runs `transform_vm_definition.py` → `actual.json`;
   - compares structurally: `jq -S . expected.json` vs `jq -S . actual.json`
     must be byte-identical.
3. Only when every sample passes in both directions, change the call site.
4. Then on the host: `./bin/sync_truenas_servers --test --task=master_to_backup
   --subtask=vm_replication` and diff the printed mapping table against a
   pre-change run.

## Rollback

Keep the old function body in git history; the call-site change is one commit
— revert it to fall back to jq instantly.
