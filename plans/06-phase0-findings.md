# Plan 06 — Phase 0 findings (Goldeye `midclt app.*` shapes)

**Status: done 2026-10-21** on both hosts (read-only `midclt` queries; configs
captured with secret values redacted). All assumed payload shapes hold; one
code change followed (strip server-managed keys, item 2).

## 1. Shape of an app object (`app.query`) — ✅ as assumed

```bash
midclt call app.query | jq '.[0] | keys'
midclt call app.query | jq '.[] | {name, state, custom_app, version, train: .metadata?.train?}'
```

Keys: `active_workloads custom_app human_version id image_updates_available
latest_version metadata migrated name notes portals state upgrade_available
version`. `.version` = installed catalog version, `.metadata.train` = train
(`stable`/`community`), `.metadata.name` = catalog app id (`immich`, `plex`).

Custom apps: `hermes-agent` (`custom_app: true`) — not in `apps.json`. The
`ix-app` apps (`loki`, `promtail`, `syslog-ng`) report `custom_app: false`
with catalog app `ix-app`; also not in `apps.json`.

## 2. Full config of one app (`app.config`) — ⚠️ includes server-managed keys

```bash
midclt call app.config immich-master | jq .
```

Top level = the app's user values **plus** keys the middleware generates:
`ix_context` (app name, operation, version/upgrade metadata), `ix_certificates`,
`ix_certificate_authorities`, `ix_volumes`, `release_name`. These are rebuilt on
every create/update, so `Extract_app_definitions` strips them
(`APP_DEF_SERVER_KEYS`); the post-apply check strips them too. Everything else
round-trips.

Secrets in values: Immich `immich.db_password`, `immich.redis_password`; Plex
`plex.claim_token`. They replicate like any other value — see
`config/app_divergences.md` for the Plex claim-token caveat.

## 3. `app.create` / `app.update` signatures — ✅ as assumed

```bash
midclt call core.get_methods | jq '."app.create", ."app.update"'
```

Identical on both hosts. Both are jobs (`job: true`) → `midclt call` returns a
job id, pollable via `core.get_jobs`.
- `app.create {app_name (required), train="stable", catalog_app, version="latest", values, custom_app=false, custom_compose_config, custom_compose_config_string}`
- `app.update <app_name> {values, custom_compose_config, custom_compose_config_string}` — no version: an update keeps the target's installed version.

## 4. Version availability on the target — ⚠️ catalogs differ per host

```bash
midclt call app.available | jq '.[] | select(.name=="immich") | {name, train, latest_version}'
```

`app.available` gives only `latest_version`; no full installable-versions list
found yet, so `Apply_app_definition` still has no pre-check (open). Each host's
catalog reports its own installed version as latest:

| App | master | backup |
|---|---|---|
| immich | 1.14.56 (Immich v3.3.4) | 1.14.58 (v3.3.5) |
| plex | 1.3.19 | 1.3.19 |

An `app.create` on a host whose catalog lacks the source's version fails with
the "sync catalogs" error; `app.update` is unaffected (item 3).

## 5. Same `app.config` on `truenas-backup` — ✅ captured

Master-vs-backup diff classified in `config/app_divergences.md`.

## Other assumptions baked into the code

- **custom apps** (`custom_app: true`) are skipped with a warning — the
  compose-config payload shape is unverified.
- App create/update job wait: `core.get_jobs` polling, 600 s timeout.
- Post-apply check: target `app.config` (server keys stripped) is diffed
  against the transformed JSON; differences only WARN.
- ✅ The jq transform (`walk()`, lookaround regex) runs on the hosts' jq 1.6.
