# grafana

Evidence: `evidence-master-20261115-0116`, `evidence-backup-20261115-0104`, app running on both. Template and vocabulary:
[README.md](README.md).

## 1. Identity

| | truenas-master | truenas-backup | Evidence |
|---|---|---|---|
| App name | `grafana-master` | `grafana-backup` | `E:platform/inventory-apps.json` |
| Kind | catalog app `grafana`, train `community`, not a compose app | same | same |
| Catalog version | `1.4.28` (= the catalog's latest on both) | `1.4.28` | `E:apps/grafana/app.available.json` |
| Upstream version | `13.3.1` | `13.3.1` | `E:platform/inventory-apps.json` |
| Image | `grafana/grafana:13.3.1@sha256:0615ec4f…`, id `sha256:bc5ca661…` | identical | `E:apps/grafana/docker/image-*.json` |
| Containers | one: `ix-grafana-<server>-grafana-1`, runs as `568:568` | same | `E:apps/grafana/docker/inspect-*.json` |
| State | RUNNING | RUNNING | `E:platform/inventory-apps.json` |

## 2. Definition

15 values (`app.config` without the platform's generated keys); 4 differ between the hosts.
Evidence for the whole table: `E:apps/grafana/app.config.json`,
`E:apps/grafana/token-hits-definition.json`.

| Value | Class | master | backup | Reason |
|---|---|---|---|---|
| `network.certificate_id` | rewrite (lookup, not a token) | `2` | `3` | Id of the host's own GUI certificate (`truenas-master` / `truenas-backup`) in that host's certificate list — `E:platform/certificates.json`. truenas-backup also has id `1` (`truenas_default`) |
| `network.root_url` | rewrite | `https://192.168.0.10:30037` | `https://192.168.0.60:30037` | Host IP; becomes the container's `GF_SERVER_ROOT_URL` |
| `storage.data.host_path_config.path` | rewrite | `/mnt/ssdmaster-pool/encrypted-ds/app-ds/grafana-ds/data` | `/mnt/backup-pool/encrypted-ds/app-ds/grafana-ds/data` | Pool |
| `storage.plugins.host_path_config.path` | rewrite | `/mnt/ssdmaster-pool/…/grafana-ds/plugins` | `/mnt/backup-pool/…/grafana-ds/plugins` | Pool |
| `network.web_port` (`published`, port `30037`, `host_ips: []`), `network.host_network: false`, `network.networks: []` | copy | identical | | |
| `resources.limits` (`cpus: 2`, `memory: 4096`) | copy | identical | | |
| `run_as` (`user: 568`, `group: 568`) | copy | identical | | uid/gid 568 is `apps` on both hosts — `E:apps/grafana/data/0-data/owners.txt` |
| `storage.*.type: host_path`, `acl_enable: false`, `additional_storage: []` | copy | identical | | |
| `grafana.additional_envs: []`, `grafana.plugins: []`, `labels: []` | copy | identical | | No secrets and no user-selected plugins in the definition |
| `ix_context`, `ix_certificates`, `ix_certificate_authorities`, `ix_volumes` (`{}`), `release_name` | generated | | | Rebuilt by the platform on every create/update; `ix_certificates` carries the host certificate's private key |

Generated from the definition, never handled:
- the rendered compose file and `user_config.yaml` under
  `/mnt/.ix-apps/app_configs/<app name>/versions/<catalog version>/` — `E:apps/grafana/app_configs/`;
- the container's environment (`GF_SERVER_ROOT_URL`, `GF_SERVER_HTTP_PORT`, certificate paths),
  its network (`ix-grafana-<server>_default`) and port binding `30037` —
  `E:apps/grafana/docker/inspect-*.json`.

The definition's schema (types, defaults, allowed values) is `E:apps/grafana/app_configs/versions/1.4.28/questions.yaml`.

## 3. Storage

Both mounts are plain directories in one dataset, `<fast pool>/encrypted-ds/app-ds/grafana-ds`
(`acltype nfsv4`, `aclmode passthrough`, encrypted) — `E:apps/grafana/data/0-data/dataset.txt`,
`E:apps/grafana/mounts.json`. No ix-volumes.

| Mount (in the container) | Content | Size | Owner, mode | Class |
|---|---|---|---|---|
| `…/grafana-ds/data` (`/var/lib/grafana/data`) | `grafana.db` and four empty directories | 1.9 MB | `568:568`, files and directories `0770` | see §4 |
| `…/grafana-ds/plugins` (`/var/lib/grafana/plugins`) | data-source and app plugins, including 74 MB executables | master 631 MB / 1309 entries, backup 508 MB / 1066 entries | `568:568`, `0770` | see §4 |

Evidence: `E:apps/grafana/data/0-data/tree.tsv`, `E:apps/grafana/data/1-plugins/tree.tsv`,
`…/tree-summary.txt`.

Numeric owners are the same on both hosts, so a copy that preserves numeric ids is correct here.
Access control lists are inherited from each host's dataset, not part of the content
(`E:apps/grafana/data/0-data/acl.txt`; they differ — [drift.md](drift.md)).

## 4. Data items

| Item | Class | Reason | Evidence |
|---|---|---|---|
| `data/grafana.db` — SQLite, one file, no `-wal`/`-shm` beside it while running | copy, with the one host reference in §5 | All of Grafana's state: users, dashboards, data sources, alert rules, API keys, encryption keys. 96 tables, 740 schema migrations on both hosts | `E:apps/grafana/data/0-data/sqlite/grafana.db/schema.sql`, `tables.tsv` |
| — tables `data_keys`, `secrets`, `signing_key`, `api_key`, `user`, `user_auth_token` | copy (part of the file) | Secrets, encrypted with keys that are also in the database. The container has no `GF_SECURITY_*` variable on either host, so nothing outside the file is needed to read them: the file is portable as a whole | `…/rows/data_keys.jsonl`, `E:apps/grafana/docker/inspect-*.json` (`Config.Env`) |
| — tables `server_lock`, `alert_rule_state`, `kv_store` timestamps, `user.last_seen_at`, `resource_version`, `sqlite_sequence`; timestamps in `role` and the `*_migration_log` tables; one `resource` row (`guid`, rewritten by a migration at each start) | runtime | The only rows that differ between the hosts; all are clocks, counters and start-up bookkeeping of the running instance | `…/rows/*.jsonl` |
| — `kv_store` row `infra.usagestats / anonymous_id` | copy (part of the file) | Grafana's usage-statistics id; identical on both hosts today, so both report as one instance. No effect on operation | `…/rows/kv_store.jsonl` |
| `data/csv`, `data/pdf`, `data/png`, `data/unified-search/bleve` | regenerable | Empty on both hosts; render and search-index scratch space | `E:apps/grafana/data/0-data/tree.tsv` |
| `plugins/*` | regenerable — **unconfirmed** | The definition selects no plugins, yet 17–19 plugin directories exist, most rewritten at the container start of 2026-11-15 on both hosts: the image installs them itself. The two hosts hold different sets (`cloudwatch`, `opentsdb` only on truenas-master) while running the identical image | `E:apps/grafana/data/1-plugins/tree.tsv` |

No per-instance identity was found: every value that differs between the two installs is either
in §2, runtime, or history.

## 5. Host references

Token scan over the definition, every file of both mounts, and every row of `grafana.db`.
Files: no hit on either host (`E:apps/grafana/data/*/token-hits-files.json`); one file was not
scanned on truenas-master (`plugins/cloudwatch/gpx_grafana_cloudwatch_…_linux_amd64`, 74 MB, an
executable — `E(master):apps/grafana/data/1-plugins/skipped.txt`).

| Location | Value | On truenas-backup today | Class |
|---|---|---|---|
| definition, 3 paths + `root_url` | §2 | own tokens only | rewrite |
| `grafana.db`, table `data_source`, column `url`, 1 row (`name: loki`, `type: loki`) | `http://192.168.0.10:3100` on **both** hosts | **a reference to truenas-master** — the independence goal is not met | rewrite, or host-neutral (below) |

Evidence: `E:apps/grafana/data/0-data/sqlite/grafana.db/token-hits.json`, `…/rows/data_source.jsonl`.

Host-neutral options for the Loki address, none confirmed:
- the container's gateway address — `172.16.3.1` on both hosts today
  (`E:apps/grafana/docker/inspect-*.json`, `NetworkSettings`), but Docker assigns that subnet;
  nothing pins it, and whether Loki's port answers there is not in evidence;
- a name that resolves to the own host from inside the container — none exists today.

## 6. Constraints

- **Same image on both hosts** before the database is copied: the file carries its schema
  version, and an older Grafana cannot open a newer schema. Same catalog version gives the same
  image (§1).
- **Stopped on both hosts while `grafana.db` is copied**: a single SQLite file, written
  by the running app without being asked (alert state, minutes before each collection —
  `…/rows/alert_rule_state.jsonl`).
- **Definition before data**: the target's host paths and container come from the definition.
- **The data-source address is changed on the target while the app is stopped**, after the copy
  and before the start — unless it is made host-neutral.
- **The logging chain**: Grafana's only data source is Loki; on truenas-backup it must be
  truenas-backup's own Loki, which is not installed there yet
  (`E(backup):platform/inventory-apps.json`).

## 7. Operations needed

From [operations.md](operations.md): definition replication; stop/start an app; directory copy;
value rewrite inside a SQLite database (only if the address is not made host-neutral);
token scan on the target.

## 8. Open

| Question | What would settle it |
|---|---|
| Are `plugins/*` fully rebuilt by the image at start? If so the 500–600 MB need no copy | Start Grafana on truenas-backup with an empty `plugins` directory and compare — a change on a host, so the user's decision |
| Why do the plugin sets differ between identical images? | The same test; or Grafana's log at start |
| Can the Loki address be host-neutral? | Loki's catalogue entry (its bind address and port); a decision whether to pin the Docker subnet |
| Does the 74 MB unscanned executable matter? | No design value expected; raise `--max-scan-bytes` on the next collection to close it |
