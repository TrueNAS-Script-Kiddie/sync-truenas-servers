# App divergences between truenas-master and truenas-backup

The spec for the app transforms: every value that legitimately differs per host, where it lives,
and how it is handled. **Everything not listed here is identical on both hosts** — ports, resource
limits and toggles included (identical ports keep a `data` repoint transparent). A difference that
isn't a per-host value is drift: converge it on the hosts, don't add a rule.

Kinds of per-host value:
- **host identity** — IP, hostname, certificate id, app name suffix;
- **pool path** — `ssdmaster-pool` / `master-pool` vs `backup-pool`;
- **external identity** — registrations with third parties; never copied, each host keeps its own.

Compare a live app (server-managed keys stripped, as root on each host):
`midclt call app.config <app>-<server> | jq -S 'del(.ix_context, .ix_certificates, .ix_certificate_authorities, .ix_volumes, .release_name)'`

## Common to all apps

| Value | master | backup | Handled by |
|---|---|---|---|
| pool in host paths | `/mnt/ssdmaster-pool/encrypted-ds/{app-ds,vm-ds}`, `/mnt/master-pool/encrypted-ds` | `/mnt/backup-pool/encrypted-ds/…` | `string_rewrites` |
| host IP | `192.168.0.10` | `192.168.0.60` | `string_rewrites` |
| hostname | `truenas-master` | `truenas-backup` | `string_rewrites` |
| app name suffix (also inside values, e.g. `hostname: loki-master`) | `<app>-master` | `<app>-backup` | built into the transform |
| GUI certificate id (`certificate_id`) | `2` | `3` | `value_overrides` |

## In app definitions (`app.config`)

| App | Field | Kind | Handled by |
|---|---|---|---|
| plex | `storage.config`, `storage.logs`, `storage.data` host paths | pool path | `string_rewrites` |
| plex | `plex.claim_token` | external identity | `keep_target` (target keeps its own; `""` on create). Used only to link an unlinked server to plex.tv; the link itself lives in `Preferences.xml`, never copied |
| immich | `storage.data`, `storage.postgres_data` host paths | pool path | `string_rewrites` |
| grafana | `network.certificate_id`; `network.root_url` (`https://<ip>:30037`); `storage.*` host paths | host identity, pool path | `value_overrides`, `string_rewrites` |
| forgejo | `network.certificate_id`; `forgejo.root_url` (`https://<ip>:30142/`); `storage.*` host paths | host identity, pool path | `value_overrides`, `string_rewrites` |
| firefly-iii | `firefly_iii.app_url` (`http://<ip>:31069/`); `network.web_port.host_ips` (the app binds to it — a wrong IP and it can't start); `storage.*` host paths | host identity, pool path | `string_rewrites` |
| syslog-ng, promtail, loki | `hostname` (`<app>-master`); `storage.N` host paths | host identity, pool path | name suffix, `string_rewrites` |
| hermes-agent (custom app) | compose `services.hermes.volumes` | pool path | `string_rewrites` — custom apps not yet supported |

Same on both hosts on purpose (not divergences): Immich `db_password` / `redis_password`;
firefly-iii `APP_KEY` (must match for its data to decrypt; copied with the definition); Plex `allowed_networks` **empty** on both (it becomes Plex's
`allowedNetworks`: clients from those IPs skip sign-in and get owner rights — keep it empty, also in
Plex Web → Settings → Network); Plex transcode storage `temporary`.

## Inside the app's own data (not in `app.config`)

Not handled yet — [plans/app-replication.md](../plans/app-replication.md).

| App | Location | Value on master |
|---|---|---|
| grafana | `data/grafana.db` (SQLite), table `data_source`, column `url` | `http://192.168.0.10:3100` (Loki) |
| forgejo | `data/custom/conf/app.ini`: `DOMAIN`, `SSH_DOMAIN`, `ROOT_URL` | `192.168.0.10` |
| syslog-ng | `syslog-ng.conf`: `syslog("192.168.0.10" …)` | `192.168.0.10` |
| promtail | `config.yml`: `url: http://192.168.0.10:3100/loki/api/v1/push` | `192.168.0.10` |

Checked and copied as-is: the Postgres DBs of immich and firefly-iii (no host references);
forgejo's Postgres DB and hermes-agent's memories/sessions (only history and labels); Plex's library
DB and loki's `loki-config.yaml` (none).
