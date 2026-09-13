# Plan 06 — Phase 0 findings (Goldeye `midclt app.*` shapes)

**Status: NOT YET RUN ON A HOST.** Phase 2 code was written offline against the
assumptions below. Run each command on `truenas-master` as root, record the
answer here, and fix the code wherever an assumption is wrong — every
assumption site is marked `VERIFY(phase0)` (grep for it).

## 1. Shape of an app object (`app.query`)

```bash
midclt call app.query | jq '.[0] | keys'
midclt call app.query | jq '.[] | {name, state, custom_app, version, train: .metadata?.train?}'
```

Answer: _(fill in)_

Assumed by the code (`lib/rep_app_definitions.bash`):
- `.name`, `.state` (values `RUNNING`/`STOPPED`/`CRASHED`/`DEPLOYING`), `.custom_app`
- `.version` = installed version
- `.metadata.train` = catalog train, `.metadata.name` = catalog app id

## 2. Full config of one app (`app.config`)

```bash
midclt call app.config immich-master | jq . > /root/immich-master.config.json
```

Answer: _(fill in)_

Assumed: the `app.config` output is the `values` object that `app.create` /
`app.update` accept round-trip. If the server adds read-only keys that
`app.*` rejects (e.g. `ix_volumes` internals, `ix_certificates`), add a
`del(...)` strip at the top of the jq program in `Transform_app_definition`.

## 3. `app.create` / `app.update` signatures

```bash
midclt call core.get_methods | jq '."app.create", ."app.update"'
```

Answer: _(fill in)_

Assumed:
- `app.create` accepts `{app_name, train, catalog_app, version, values}` and
  returns a **job id**;
- `app.update` accepts `<name> {values: ...}` and returns a **job id**;
- both jobs are pollable via `core.get_jobs` (same as `app.start`/`app.stop`).

## 4. Version availability on the target

```bash
midclt call app.available | jq '.[] | select(.name=="immich") | {name, train, latest_version}'
# check both hosts show the same version for one app
```

Answer: _(fill in — note where the *full* installable-versions list comes from)_

The code has **no pre-check**: `app.create` is attempted with the source's
exact version and the submit/job failure message says to sync catalogs (it
never falls back to "latest"). If a reliable availability query exists,
add a pre-check in `Apply_app_definition`.

## 5. Same `app.config` on `truenas-backup`

Repeat step 2 on backup for the same app — feeds the phase 1 diff
(`config/app_divergences.md`).

## Other assumptions baked into the code

- **custom apps** (`custom_app: true`) are skipped with a warning — the
  compose-config payload shape is unverified.
- App create/update job wait: `core.get_jobs` polling, 600 s timeout (image
  pulls are slow; deliberately longer than `Control_app`'s 60 s).
- Post-apply check: target `app.config` is diffed against the transformed
  JSON; differences only WARN (server-added defaults are expected noise —
  tighten after the first real run).
- The jq transform uses `walk()` (jq ≥ 1.6) — verify with the first `--test`
  run; there is no jq on the Windows side to pre-check the program.
