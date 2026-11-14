# Plan: replication redesign — every workload, fully independent of the other host

**Goal:** after a sync, every workload on truenas-master — apps, VMs and, once used, TrueNAS
containers; current and future — runs on truenas-backup with the same content and **no reference
to truenas-master** (README "Purpose"). Adding a workload is specification, not code — unless it
needs a new kind of operation.

**Scope boundary:** the redesign is about the **spec** — per workload, *what* is copied, kept per
host, rewritten or ignored, and which operation handles it. The **mechanisms** — *how* an
operation runs (Immich dump/restore, Postgres data-directory copy, container order, app and VM
control, `zfs_autobackup`) — are reasonably sound and stay in code; phase 4 adapts them to the
spec with full access to the implementation.

**Why a redesign:** today's config was built app by app and never saw the full scope. A spec
derived from it inherits its blind spots, even with correct requirements. So the requirements are
captured first, the spec is designed from those alone, and only then is the old implementation
mined for ideas worth keeping.

## Way of working

- **A master chat orchestrates; phases 1, 3 and 4 run in fresh sub-chats** (Claude Code `Agent`;
  `SendMessage` continues a sub-chat with its context). The master chat:
  - writes each phase's prompt when the phase starts: goal, inputs, what not to read, exit
    criteria. Short, so it doesn't steer the design;
  - passes file paths, never its own summaries, so its knowledge of the old implementation
    doesn't leak into phase 2;
  - relays a sub-chat's questions and root commands to the user (sub-chats can't reach the user);
  - checks the output against the exit criteria and keeps the status table below current.
- **Phase 2 runs as a separate session outside this repo**, not as a sub-chat: every chat in this
  repo, sub-chats included, auto-loads `CLAUDE.md` → `AGENTS.md` → `architectural_patterns.md`
  and this project's memory, which describe the old config in detail. The master chat prepares a
  private Forgejo repo holding only copies of `plans/redesign/requirements/` and `evidence/`
  (gitignored there too) and writes the prompt; the user starts the session in that folder.
- **Every phase ends with the user's review and a commit.** The handoff between phases is files,
  never chat history.
- **Ask only what the material doesn't answer;** quote what was checked.
- **Each phase is a safe stopping point.** Phase 1 alone already answers "what differs between my
  two installs, and why".
- **Starting or resuming:** a new chat in this repo with *"Act as the master chat for
  plans/replication-redesign.md; continue at the first phase that isn't done."*

| Phase | Output | Status |
|---|---|---|
| 1. Requirements | `plans/redesign/requirements/`, `plans/redesign/evidence/` | not started |
| 2. Spec design | the phase 2 Forgejo repo, copied to `plans/redesign/design/` | — |
| 3. Compare with the old implementation | `plans/redesign/comparison.md`, design updated | — |
| 4. Implement | code, config, docs; `plans/redesign/` removed | — |

### Phase 1 — Requirements

Research and record everything replication could touch, read, handle or transform, so the design
chat never has to extract anything from TrueNAS itself.

- **Scope:** every workload on the hosts at collection time — at least the apps grafana, immich,
  plex, forgejo, firefly-iii, syslog-ng, promtail, loki, hermes-agent and the VMs FedoraBC,
  FedoraF (inventory 2026-10-23). TrueNAS containers (none in use: their definition format from
  TrueNAS docs/API, marked hypothetical), and the host level (pools and dataset layout,
  certificates, NICs, network). Plus dataset replication: every dataset with its `autobackup:*`
  properties (where set, where inherited), and which workload depends on which dataset (Plex on
  `media-ds`, VMs on their zvols).
- **Evidence** (`plans/redesign/evidence/`, gitignored): from **both** hosts — the master/backup
  difference is the strongest evidence of what is per-host. Definitions (`app.config`,
  `app.query`, `vm.query`, compose files), image tags and digests, every host-path mount and
  ix-volume, file trees and config files in full, the API method signatures (`core.get_methods`
  for the app, VM and container methods) and each host's catalog versions (`app.available`).
  Limits:
  - databases (SQLite, Postgres): full schema; rows only from tables holding configuration or
    host references (e.g. grafana's data sources) — user data such as firefly-iii's finances has
    no design value;
  - bulk user content (photos, media, Plex `Metadata`, Loki chunks, git repos): tree, sizes and
    samples only — hundreds of GB with no design value beyond kind and size;
  - secret values masked, key and location kept: the design needs *that* and *where*, never the
    value, and every chat reading the evidence sends it to the model and keeps it in its
    transcript.
- **Collection:** `tools/collect-evidence.sh` (deployed by the SFTP watcher, like
  `tools/mountwatch.sh`; `plans/` is never uploaded), read-only, run by the user as root on each
  host (Claude's SSH users can't call `midclt`). Writes one archive readable by the non-root SSH
  user, so it can be copied to the desktop. Re-runnable for a new workload later.
- **Requirements catalogue** (`plans/redesign/requirements/`, committed), in neutral language —
  none of the old config's vocabulary (`app_dir_list`, `string_rewrites`, `pre_action`):
  - **per workload:** every item classified (copy as-is / keep per host / rewrite / regenerable
    cache / runtime state / never touch), with the reason and the evidence behind it; its
    constraints (order, state, versions) and the operations it needs;
  - **operations:** the existing mechanisms, named and described at a high level — what each
    does, what it needs, its constraints (e.g. "database dump/restore: identical image, app
    stopped on the target") — never how the code does it. The spec refers to these;
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
- **Exit:** every workload on the hosts has a catalogue entry with evidence from both hosts (or a
  stated reason it has none); every item above is confirmed, corrected or marked open; every
  operation a workload needs is in the operations list.

### Phase 2 — Spec design

A separate session (see Way of working) designs how replication is specified, from the
requirements catalogue and evidence only — **not** `lib/`, `bin/`, `config/`, `tests/` or the old
implementation's docs.

- **Requirements:** (1) easy for the user to understand and modify; (2) unambiguous and
  deterministic for the sync app; (3) flexible enough for every current workload and, as far as
  foreseeable, future ones.
- **Fixed:** `zfs_autobackup` and how it is run stay. Workload → dataset dependencies are
  specified. Workloads select operations from the catalogue's list; a needed operation that isn't
  there is proposed as a new one, described like the others.
- **To decide:** whether dataset selection moves into the spec. If so, the spec classifies every
  dataset (replicated or deliberately not), and a property that doesn't match it or a dataset it
  doesn't classify fails the run before anything changes.
- **Free:** the form (JSON or something better), one spec or several (per workload kind, per kind
  of app).
- **Deliverables:**
  - the spec format with the exact meaning of every field, and how a workload selects and
    parametrizes operations;
  - the complete spec for every current workload, written in it;
  - a walk-through of adding a new app;
  - if feasible, tests from the evidence: the spec applied to a captured source yields the
    captured target.
- **Exit:** every catalogue item maps to a place in the spec (or a stated reason it needs none);
  the walk-through is done. The design is copied to `plans/redesign/design/`.

### Phase 3 — Compare with the old implementation

A fresh sub-chat reads the new design and the old implementation (`lib/`, `bin/`, `config/`,
`tests/`, docs) and lists what the old one does that the design lacks or does better — proven
mechanisms, edge cases, platform workarounds — each with a recommendation.

- **Exit:** every finding has the user's decision. The phase 2 session (not the comparing chat)
  applies them to the design, which is copied back to `plans/redesign/design/`.

### Phase 4 — Implement

- **The existing mechanisms are adapted to read the new spec,** not rewritten; a new operation
  from phase 2 is added as code.
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

The baseline that phase 4 adapts; phase 3 reviews it.

- **App data** (`app_replication`): works for plex, immich (DB dump/restore), grafana.
- **App definitions** (`app_definition_replication`, manual-only): create and update work for
  catalog apps (immich-backup updated, grafana-backup created; plex `--test` clean, no real run
  yet). Custom apps are skipped; `ix-app` apps are untested.
- **Host references inside app data** are not rewritten.
- **VMs** (`vm_replication`): works for both VMs.
