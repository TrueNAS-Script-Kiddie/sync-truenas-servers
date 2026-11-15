# Drift

Differences between the two hosts that are not per-host by nature. For the user to converge on
the hosts (GUI); the spec never models them. Evidence references: [README.md](README.md).

## grafana

| Where | truenas-master | truenas-backup | Evidence |
|---|---|---|---|
| Dataset `grafana-ds`, access control list of `data`, `plugins` and everything below | has an inherited entry `group:builtin_users` (full access except ownership) | no such entry | `E:apps/grafana/data/0-data/acl.txt`, `E:apps/grafana/data/1-plugins/acl.txt` |
| Dataset `grafana-ds`, property `snapdir` | `hidden` (default) | `hidden` (set locally) | `E:apps/grafana/data/0-data/dataset.txt` |
| Dataset `app-ds`, user property `org.truenas:managedby`, inherited by `grafana-ds` | absent | `192.168.0.15` | same |
| Plugin directories | `cloudwatch` and `opentsdb` present | absent | `E:apps/grafana/data/1-plugins/tree.tsv` — may resolve itself, see [grafana.md](grafana.md) §8 |

## Platform

| Where | truenas-master | truenas-backup | Evidence |
|---|---|---|---|
| Installed apps | 9 (grafana, immich, plex, forgejo, firefly-iii, syslog-ng, promtail, loki, hermes-agent) | 3 (grafana, immich, plex) | `E:platform/inventory-apps.json` |
| App name of hermes-agent | `hermes-agent` — no `-master` suffix | not installed | `E(master):platform/inventory-apps.json` |
