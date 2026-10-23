# Operations

Procedures for running the pair by hand. Everything here is done by a person, not by the script.

## Failover — truenas-backup takes over

1. Power on truenas-backup and unlock its encrypted datasets (GUI).
2. **Start the SMB service** on truenas-backup — it is stopped there on purpose (part of the
   [dataset-busy investigation](dataset-busy-investigation.md)). All shares are enabled, but a
   stopped service does not announce itself: the shares just look absent.
3. Repoint `data` to `192.168.0.60` in each client's hosts file. Forgejo clients follow
   automatically (their `forgejo` SSH alias uses `HostName data`).
4. Repoint the router's syslog target to `192.168.0.60`.
5. Check the apps on truenas-backup are running.

Failback: run `--task=backup_to_master`, then reverse steps 2–4.

## Preparing truenas-backup for a new app

`app.create` needs the app's host paths to exist. Create them the way TrueNAS expects — never
with `zfs create`, which skips the middleware (presets, ACL type, its own bookkeeping):

1. **Datasets** → select `backup-pool/encrypted-ds/app-ds` → **Add Dataset**: same name as on
   truenas-master (`<app>-ds`), preset **Apps**. Under Advanced Options match truenas-master's
   dataset explicitly: ACL Type, ACL Mode, Enable Atime, Snapshot Directory.
2. **Permissions** → Edit: owner and group `apps` (568), *Apply Owner/Group* ticked; remove
   entries truenas-master's dataset doesn't have (e.g. `builtin_users`). Not recursive — empty.
3. Plain directories inside the dataset (not datasets) are fine from a root shell:
   `mkdir` + `chown 568:568`. `mkdir` as root makes root the owner, hence the `chown`.

Compare with truenas-master: `ls -ln`, `zfs get acltype,aclmode,atime,snapdir`, `nfs4xdr_getfacl`.

## Immich Postgres collation-version mismatch

**Symptom:** `Backup_immich_DB` prints, once per database it connects to:

```
WARNING:  database "postgres" has a collation version mismatch
DETAIL:  The database was created using collation version 2.41, but the operating system provides version 2.36.
```

A warning only: `pg_dumpall` exits 0 and the backup completes. **Recurs after every Postgres
*major* upgrade of the Immich app.**

**Cause.** Postgres records per database the glibc version it was created under
(`pg_database.datcollversion`), because glibc defines text sort order; when the running glibc
differs, indexes on text columns may be silently wrong. Immich's app runs the same data files
(`immich-pgdata-ds`) in two containers on different Debian releases:

| Container | Image | glibc | Runs |
|---|---|---|---|
| `ix-immich-<server>-pgvecto-1` | `ghcr.io/immich-app/postgres:18-vectorchord1.1.1` | 2.36 (`pgdg12`) | always — the Postgres server |
| `ix-immich-<server>-pgvecto_upgrade-1` | `ghcr.io/truenas/containers/postgres-upgrade:1.2.24` | 2.41 (`pgdg13`) | at each app start; acts only on a major upgrade |

On a major upgrade the TrueNAS helper `pg_upgrade`s under 2.41 and labels every database 2.41;
`pgvecto` then runs it under 2.36. TrueNAS treats this as expected
([truenas/apps#3882](https://github.com/truenas/apps/issues/3882)).

Only the sync's *source* shows it (only `pg_dumpall` connects there). A *target* clears itself:
`Restore_immich_DB` wipes pgdata and `pgvecto` re-`initdb`s under its own glibc.

**Procedure** — root shell on the host that prints it. Steps 1–2 only read; the app must run.

1. Recorded vs running version per database:

```bash
h=$(hostname -s | sed 's/^truenas-//')
c=$(docker ps --format '{{.Names}}' | grep -E "immich-$h-pgvecto-[0-9]+$"); echo "pgvecto container: $c"
docker exec -i "$c" psql -U immich -d immich -c "SELECT datname, datcollversion AS created_with, pg_database_collation_actual_version(oid) AS running_on FROM pg_database;"
```

2. Rows where `created_with` ≠ `running_on` are affected. For each affected database other than
   `immich`, confirm it holds no user objects (expect `0`):

```bash
for db in postgres template1; do echo -n "$db: "; docker exec -i "$c" psql -U immich -d "$db" -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%';"; done
```

3. Fix per affected database.
   - **Empty (`0` in step 2):** only the label is wrong — refreshing it is the complete fix:

```bash
docker exec -i "$c" psql -U immich -d postgres -c "ALTER DATABASE postgres REFRESH COLLATION VERSION;"
docker exec -i "$c" psql -U immich -d template1 -c "ALTER DATABASE template1 REFRESH COLLATION VERSION;"
```

   - **`immich`, or any non-empty database:** rebuild first, then refresh (refreshing alone would
     hide wrong indexes). Stop the writers during the `REINDEX` (minutes on a large library):

```bash
docker stop "ix-immich-$h-server-1" "ix-immich-$h-machine-learning-1"
docker exec -i "$c" psql -U immich -d immich -c "REINDEX DATABASE immich;"
docker exec -i "$c" psql -U immich -d immich -c "ALTER DATABASE immich REFRESH COLLATION VERSION;"
docker start "ix-immich-$h-machine-learning-1" "ix-immich-$h-server-1"
```

   Re-run step 1: every row except `template0` must match. Each `ALTER` prints the warning once
   more, then `NOTICE: changing version from X to Y` — expected.

On a sync target, the next `app_replication` onto it clears the mismatch without any step.

**When it gets risky:** when Immich moves its Postgres image to Debian 13 (glibc 2.41) the
mismatch flips and hits every database on both hosts, `immich` included (REINDEX path). Check
step 1 after any Immich update that changes the `pgvecto` image's `PG_VERSION` suffix
(`pgdg12` → `pgdg13`). Deliberately not automated: a REINDEX of a live DB is a judgement call,
and an automatic refresh would hide the problem.

## VM orphan libvirt domain

When `vm.create` fails with `domain '<id>_<name>' is already defined with uuid …` (see
[truenas-api.md](truenas-api.md#vms) and the open UUID bug in [plans/backlog.md](../plans/backlog.md)):

1. Force-delete the ERROR-state VM — GUI *Force Delete*, or
   `midclt call vm.delete <id> '{"force": true}'`. Leave *Delete Virtual Machine Data* /
   `zvols` **off** so the replicated disk stays.
2. Undefine the leftover domain:
   `virsh -c 'qemu+unix:///system?socket=/run/truenas_libvirt/libvirt-sock' undefine <id>_<name>`
3. Re-run VM replication for that VM.

## A VM that won't stop

`--stop-running-vms` stops gracefully only. A guest that is still booting, or ignores ACPI,
makes the run abort after 180 s with that message; the source is restarted by the cleanup and a
warning names the target. Don't start a `--stop-running-vms` run in the first minute after
booting a VM.

## Immich DB restore left the target empty

If a run aborts inside `Restore_immich_DB`, the target is left with a fresh, empty pgdata and its
writer containers stopped. The source is intact: re-run `--subtask=app_replication --app=immich`.
