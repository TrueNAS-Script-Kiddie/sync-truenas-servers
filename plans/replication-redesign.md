# Plan: replication redesign — every workload, fully independent of the other host

**Goal:** after a sync, every workload on truenas-master — apps, VMs and, once used, TrueNAS
containers; current and future — runs on truenas-backup with the same content and **no reference
to truenas-master** (README "Purpose"). Adding a workload is specification, not code.

**Why a redesign:** today's implementation was built app by app and never saw the full scope.
A new design derived from it inherits its blind spots, even with correct requirements. So the
requirements are captured first, the design is made from those alone, and only then is the old
implementation mined for ideas worth keeping.

## Way of working

- **A master chat orchestrates; each phase runs in its own fresh sub-chat** (Claude Code `Agent`;
  `SendMessage` continues a sub-chat with its context). The master chat:
  - writes each phase's prompt when the phase starts: goal, inputs, what not to read, exit
    criteria. Short, so it doesn't steer the design;
  - passes file paths, never its own summaries, so its knowledge of the old implementation
    doesn't leak into phase 2;
  - relays a sub-chat's questions and root commands to the user (sub-chats can't reach the user);
  - checks the output against the exit criteria and keeps the status table below current.
- **Every phase ends with the user's review and a commit.** The handoff between phases is files
  in `plans/redesign/`, never chat history.
- **Ask only what the material doesn't answer;** quote what was checked.
- **Each phase is a safe stopping point.** Phase 1 alone already answers "what differs between my
  two installs, and why".

| Phase | Output | Status |
|---|---|---|
| 1. Requirements | `plans/redesign/requirements/`, `plans/redesign/evidence/` | not started |
| 2. Design | `plans/redesign/design/` | — |
| 3. Compare with the old implementation | `plans/redesign/comparison.md`, design updated | — |
| 4. Implement | code, config, docs; `plans/redesign/` removed | — |

### Phase 1 — Requirements

Research and record everything replication could touch, read, handle or transform, so the design
chat never has to extract anything from TrueNAS itself.

- **Scope:** every app (grafana, immich, plex, forgejo, firefly-iii, syslog-ng, promtail, loki,
  hermes-agent), every VM (FedoraBC, FedoraF), TrueNAS containers (none in use: their definition
  format from TrueNAS docs/API, marked hypothetical), and the host level (pools and dataset
  layout, certificates, NICs, network).
- **Evidence** (`plans/redesign/evidence/`, gitignored): from **both** hosts — the master/backup
  difference is the strongest evidence of what is per-host. Definitions (`app.config`,
  `app.query`, `vm.query`, compose files), image tags and digests, every host-path mount and
  ix-volume, file trees, config files and databases **in full** (SQLite files, Postgres dumps),
  the API method signatures (`core.get_methods` for the app, VM and container methods) and each
  host's catalog versions (`app.available`). Two exceptions:
  - bulk user content (photos, media, Plex `Metadata`, Loki chunks, git repos): tree, sizes and
    samples only — hundreds of GB with no design value beyond kind and size;
  - secret values masked, key and location kept: the design needs *that* and *where*, never the
    value, and every chat reading the evidence would otherwise carry it.
- **Collection:** one read-only script, run by the user as root on each host (Claude's SSH users
  can't call `midclt`), writing one archive. Re-runnable for a new workload later.
- **Requirements catalogue** (`plans/redesign/requirements/`, committed), in neutral language —
  none of the old config's vocabulary (`app_dir_list`, `string_rewrites`, `pre_action`):
  - **per workload:** every item classified (copy as-is / keep per host / rewrite / regenerable
    cache / runtime state / never touch), with the reason and the evidence behind it; its
    constraints (order, state, versions);
  - **drift:** a master/backup difference that isn't per-host by nature (a port, a limit, a
    toggle). Listed for the user to converge on the hosts — never specified, or the design
    models noise;
  - **general:** the run-level requirements, restated from README, AGENTS.md and
    `.claude/rules/architectural_patterns.md` without their implementation: failover and
    failback, dual-active hosts, VMs as twins (one copy running at a time), manual runs only,
    `--test` changes nothing but shows everything, skip an item vs abort the run, restore the
    previous state on failure, the result email, secrets never in logs or email; a running
    target may be a test in progress, so it is overwritten only on an explicit flag, separate
    from starting a stopped one;
  - **platform facts:** verified TrueNAS behaviour ([docs/truenas-api.md](../docs/truenas-api.md))
    as-is.
- **Every fact cites its evidence.** Docs, this plan and any model's memory are hypotheses until
  confirmed on the hosts — including API payload shapes.
- **Known requirements to verify and complete** — carried over, not to be trusted blindly:
  - The target gets the source's exact catalog version, never silently `latest`. Catalogs can
    differ per host; a version the target lacks skips the workload with the GUI fix.
  - After an apply, the target's actual state is compared with what the transform predicted; a
    difference is reported.
  - Data replication needs the app installed on both hosts, so its definition goes first.
  - Host references (2026-10-23 inventory, [config/app_divergences.md](../config/app_divergences.md)):
    in definitions — host paths, certificate id, URLs (grafana, forgejo, firefly-iii),
    firefly-iii's bind IP, `hostname` (syslog-ng, promtail, loki), hermes-agent's compose volume;
    in data files — forgejo `custom/conf/app.ini`, syslog-ng `syslog-ng.conf`, promtail
    `config.yml`; in databases — grafana `grafana.db` (Loki data-source URL).
  - Rewrites inside data happen on the target while the app is stopped: after the copy, before
    the start.
  - No host references, or only history: the Postgres DBs of immich, firefly-iii and forgejo;
    Plex's library DB; hermes-agent's memories and sessions.
  - An app's data = its host-path mounts in `app.config`. Per-instance identity inside them is
    known only for Plex (`Preferences.xml` with server GUID and claim, `Cache`, `Logs`; Plex
    `claim_token` in the definition); **hermes-agent's dataset is unchecked**.
  - VMs: UUID and MAC copied (twin), NIC and paths rewritten, display port 5900/5901 → 5950/5951
    on truenas-backup (reason unrecorded; no conflict on 5900 there). Backlog: per-VM twin vs own
    MAC ([backlog.md](backlog.md)).
  - Logging chain (router → syslog-ng → promtail → loki → grafana): on truenas-backup every link
    points at truenas-backup's own copy.
  - `:latest` images (syslog-ng, promtail, loki, hermes-agent) stay `:latest`; data and
    definitions are only replicated between identical running image builds — also for every app
    whose database files are copied directly. The upstream app version can match while images
    differ (the `ix-app` wrapper version is identical everywhere).
  - Forgejo's and firefly-iii's Postgres: copy the data directory with the app stopped on both
    hosts, no dump — only with the identical Postgres image (same major and glibc, so no
    collation mismatch).
  - Immich: DB via dump/restore, proven; the container order (stop writers → start pgvecto →
    dump; on the target stop all → wipe pgdata → start pgvecto → wait (TCP) → restore → full app
    stop → start) is the only one that works.
  - Forgejo on truenas-backup is only used on failover; push mirrors / webhooks (none today) must
    be disabled there if ever added.
  - `app.delete` is acceptable where recreate is cleaner than update — never with ix-volume
    removal, always shown by `--test`.
  - Never create datasets from the script: a missing host path skips the workload with the exact
    GUI steps ([docs/operations.md](../docs/operations.md#preparing-truenas-backup-for-a-new-app)).
  - Custom (compose) and `ix-app` apps are in scope; hermes-agent is the custom-app case until it
    is replaced by the official TrueNAS app.
  - Same bare app name on both hosts (`immich`, no `-master`/`-backup`): the name rewrite and its
    code disappear. TrueNAS can't rename an app, so it needs a migration.
- **Exit:** every workload in scope has a catalogue entry with evidence from both hosts (or a
  stated reason it has none); every item above is confirmed, corrected or marked open.

### Phase 2 — Design

A fresh chat designs how replication is specified, from `plans/redesign/requirements/` and
`evidence/` only — **not** `lib/`, `bin/`, `config/`, `tests/` or the old implementation's docs.

- **Requirements:** (1) easy for the user to understand and modify; (2) unambiguous and
  deterministic for the sync app; (3) flexible enough for every current workload and, as far as
  foreseeable, future ones.
- **Free:** the form (JSON or something better), one spec or several (per workload kind, per kind
  of app), and the operations it offers (copy files/folders/datasets; transform files, configs,
  databases; export/import; …).
- **Deliverables:**
  - the spec format with its exact semantics: order, state handling, failure behaviour, `--test`;
  - the complete spec for every current workload, written in it;
  - a walk-through of adding a new app;
  - if feasible, tests from the evidence: the spec applied to a captured source yields the
    captured target.

### Phase 3 — Compare with the old implementation

A fresh chat reads the new design and the old implementation (`lib/`, `bin/`, `config/`,
`tests/`, docs) and lists what the old one does that the design lacks or does better — proven
mechanisms, edge cases, platform workarounds — each with a recommendation. The user decides;
the design is updated.

### Phase 4 — Implement

- **Per workload, `--test` first, in both directions.** Old and new paths coexist until a
  workload has passed a real run; then its old path goes. The first real run of each new kind of
  operation is on a throwaway app (as it-tools was for `app.create`/`app.update`).
- **Rollout order** (from the old plan, open to the design): loki/promtail/syslog-ng, forgejo,
  firefly-iii, hermes-agent; plex's first real definition run. Before each workload's first real
  run, check its `--test` output for unmasked secrets.
- **App definitions join `--subtask=all`** once they have survived supervised runs in both
  directions.
- **Name convergence**, scheduled, not drifted into; one app at a time, with a per-app switch so
  the tool keeps working throughout. On truenas-backup first: delete the suffixed app (check
  the delete dialog leaves host-path data and ix-volumes alone), recreate it under the bare name,
  replicate data, test end to end (doubles as a failover drill). Then truenas-master, recreated
  from truenas-backup's definition (`backup_to_master`). Finally remove the switch and the suffix
  handling.
- **End:** durable content (requirements catalogue, spec reference) moves to `docs/`;
  `config/app_divergences.md` is replaced; `plans/redesign/` and this plan are deleted.

## Current implementation

The baseline that phase 4 replaces; phase 3 reviews it.

- **App data** (`app_replication`): works for plex, immich (DB dump/restore), grafana.
- **App definitions** (`app_definition_replication`, manual-only): create and update work for
  catalog apps (immich-backup updated, grafana-backup created; plex `--test` clean, no real run
  yet). Custom apps are skipped; `ix-app` apps are untested.
- **Host references inside app data** are not rewritten.
- **VMs** (`vm_replication`): works for both VMs.
