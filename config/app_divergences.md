# App config divergences (plan 06 phase 1 — spec for `app_config_mappings.json`)

**Status: diffed 2026-10-21; bucket (b) not yet converged, two rows await a
decision.** Server-managed keys (`ix_*`, `release_name`) are stripped before
diffing and never synced. Per app in `apps.json`:

```bash
diff <(jq -S . /root/<app>-master.config.json) <(jq -S . /root/<app>-backup.config.json)
```

Classify every difference into exactly one bucket:
- **(a) must diverge** — external identity, per-host certificate ID, host's own
  IP/hostname (Root URL). Keep; becomes a transform rule.
- **(b) artificial drift** — ports, resource limits, drifted toggles.
  **Converge on the live hosts now** (UI or `midclt call app.update`) — same
  host ports on both servers is required for transparent `data`-alias failover.
- **(c) pool-path differences** — `ssdmaster-pool` vs `backup-pool`. Expected;
  mechanical rewrite rule.

Bucket (a) and (c) rows become rules in `config/app_config_mappings.json`;
bucket (b) rows are eliminated and listed here only until converged.

## Known before any diff (pre-filled)

| Field | Bucket | master | backup | Rule status |
|---|---|---|---|---|
| pool in host paths | c | `/mnt/ssdmaster-pool/...` | `/mnt/backup-pool/...` | in `string_rewrites` |
| host IP (Root URLs etc.) | a | `192.168.0.10` | `192.168.0.60` | in `string_rewrites` |
| hostname | a | `truenas-master` | `truenas-backup` | in `string_rewrites` |
| app name suffix | a (until phase 3) | `<app>-master` | `<app>-backup` | built into the transform (naming convention, not config) |
| GUI certificate ID | a | _(fill in: id of `truenas-master` cert)_ | _(fill in: id of `truenas-backup` cert)_ | placeholder in `value_overrides` — fill both ids |

Certificate ids: `midclt call certificate.query | jq '.[] | {id, name}'` on each host.

## plex

| Field | Bucket | master | backup | Action |
|---|---|---|---|---|
| `network.web_port.port_number` | b | `32400` | `33400` | converge on `32400` (Plex default) |
| `plex.allowed_networks` | b | `192.168.0.0/16` | `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` | converge |
| `resources.limits.cpus` | a or b — decide | `4` | `2` | hardware-dependent → `value_overrides` rule; otherwise converge |
| `storage.transcode` | a or b — decide | `type: temporary` | `type: tmpfs`, `tmpfs_config.size: 2048` | RAM-dependent → keep per host (needs a rule type for objects); otherwise converge |
| `plex.claim_token` | a | _(redacted)_ | _(redacted)_ | plex.tv identity; a claim token expires minutes after issue, so a stored one is stale — clear it on both hosts |
| `storage.config`, `storage.logs` host paths | c | `ssdmaster-pool/…/app-ds/plex-ds` | `backup-pool/…/app-ds/plex-ds` | in `string_rewrites` |
| `storage.data.host_path_config.acl.path` | c | `master-pool/encrypted-ds/media-ds` | `backup-pool/encrypted-ds/media-ds` | in `string_rewrites` |

## immich

| Field | Bucket | master | backup | Action |
|---|---|---|---|---|
| `network.web_port.port_number` | b | `30041` | `30042` | converge (pick one) |
| installed catalog version | b | `1.14.56` (Immich v3.3.4) | `1.14.58` (v3.3.5) | refresh master's catalog, upgrade `immich-master` |
| `immich.db_password`, `immich.redis_password` | ? | _(redacted)_ | _(redacted)_ | compare by hash; differing values make an update change the target's |
| `storage.data`, `storage.postgres_data` host paths | c | `ssdmaster-pool/…/immich-ds/…` | `backup-pool/…/immich-ds/…` | in `string_rewrites` |
