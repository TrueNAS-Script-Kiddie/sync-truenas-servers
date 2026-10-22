# App config divergences (plan 06 phase 1 — spec for `app_config_mappings.json`)

**Status: bucket (b) converged on both hosts 2026-10-22.** Server-managed keys (`ix_*`, `release_name`) are stripped before
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
| GUI certificate ID | a | `2` (`truenas-master`) | `3` (`truenas-backup`) | in `value_overrides` |

Certificate ids: `midclt call certificate.query | jq '.[] | {id, name}'` on each host.

## plex

| Field | Bucket | master | backup | Action |
|---|---|---|---|---|
| `network.web_port.port_number` | b | `32400` | `33400` | ✅ `32400` on both (catalog default); router forward `Plex-Backup` external = internal `32400`, matching Remote Access' manual public port |
| `plex.allowed_networks` | b | `192.168.0.0/16` | `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` | ✅ **empty** on both (TrueNAS field and Plex Web): it becomes Plex's `allowedNetworks` — clients from these IPs need no sign-in and are treated as the server owner (all libraries, server settings) — not bandwidth "LAN Networks", whatever the TrueNAS field help says. The image applies it only when the container is created and skips an empty value, so also clear it in Plex Web (Settings → Network → "List of IP addresses and networks that are allowed without auth") |
| `resources.limits.cpus` | b | `4` | `2` | ✅ `4` on both (same 12-thread CPU; a cap, not a reservation) |
| `storage.transcode` | b | `type: temporary` | `type: tmpfs`, `tmpfs_config.size: 2048` | ✅ `temporary` on both (catalog default): can't fill up mid-stream, uses no RAM, SSD wear negligible |
| `plex.claim_token` | — | _(redacted)_ | _(redacted)_ | none on the hosts: used only to link an unlinked server to plex.tv, ignored once linked (the link lives in `Preferences.xml`, never synced). Never synced: in `keep_target` (target keeps its own; `""` on create) |
| `storage.config`, `storage.logs` host paths | c | `ssdmaster-pool/…/app-ds/plex-ds` | `backup-pool/…/app-ds/plex-ds` | in `string_rewrites` |
| `storage.data.host_path_config.acl.path` | c | `master-pool/encrypted-ds/media-ds` | `backup-pool/encrypted-ds/media-ds` | in `string_rewrites` |

## immich

| Field | Bucket | master | backup | Action |
|---|---|---|---|---|
| `network.web_port.port_number` | b | `30041` | `30042` | ✅ `30041` on both (catalog default) |
| installed catalog version | b | `1.14.56` (Immich v3.3.4) | `1.14.58` (v3.3.5) | ✅ converged (master upgraded) |
| `immich.db_password`, `immich.redis_password` | — | _(redacted)_ | _(redacted)_ | equal on both hosts (hash-compared) |
| `storage.data`, `storage.postgres_data` host paths | c | `ssdmaster-pool/…/immich-ds/…` | `backup-pool/…/immich-ds/…` | in `string_rewrites` |
