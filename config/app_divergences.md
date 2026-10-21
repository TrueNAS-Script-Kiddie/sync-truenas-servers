# App config divergences (plan 06 phase 1 — spec for `app_config_mappings.json`)

**Status: TEMPLATE — fill from live diffs.** Per app in `apps.json`, after
phase 0 saved both configs:

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

_(fill in from diff)_

| Field | Bucket | master | backup | Action |
|---|---|---|---|---|

## immich

_(fill in from diff)_

| Field | Bucket | master | backup | Action |
|---|---|---|---|---|
