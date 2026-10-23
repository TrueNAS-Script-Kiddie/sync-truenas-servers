# TrueNAS middleware behaviour this project relies on

Verified on Goldeye (25.10), on both hosts, with read-only `midclt` calls. Do not trust memory
(yours or a model's) for these shapes — re-verify on a live host when something changes.

## Apps

**Lifecycle**
- `app.stop` **destroys** the app's containers — they disappear from `docker ps -a`. `app.start`
  recreates them. Containers exist only while the app is running.
- To stop part of an app (e.g. Immich's writers around a DB dump), use `docker stop/start` on
  individual containers of a *running* app — never `midclt` for that.
- `app.stop`, `app.start`, `app.create`, `app.update` are **jobs**: `midclt call` returns a job id;
  poll `core.get_jobs '[["id","=",<id>]]'` until `state` is `SUCCESS` (or `FAILED`/`ABORTED`/`ERROR`).

**Names**
- App: `<app>-<server_id>` (`immich-master`). Containers: `ix-<app>-<server_id>-<component>-<N>`
  (`ix-immich-master-pgvecto-1`) — a grep must not anchor at `^<app>`.

**`app.query`** — keys: `active_workloads custom_app human_version id image_updates_available
latest_version metadata migrated name notes portals state upgrade_available version`.
- `.version` = installed catalog version; `.metadata.train` = train (`stable`/`community`);
  `.metadata.name` = catalog app id (`immich`, `plex`, `ix-app` for a custom-image app);
  `.metadata.app_version` = upstream version (for `ix-app` apps this is the wrapper's version,
  identical everywhere — it says nothing about the image).
- `.active_workloads` lists images, container volumes (`source` → `destination`) and ports.
- `custom_app: true` = a compose-file app (hermes-agent); `ix-app` apps report `false`.

**`app.config <name>`** — the app's values **plus** keys the middleware regenerates on every
create/update: `ix_context`, `ix_certificates` (includes private keys), `ix_certificate_authorities`,
`ix_volumes`, `release_name`. Strip these before comparing or pushing; everything else round-trips
unchanged through `app.create`/`app.update`.
- Large (certificates): passing it as one command-line argument can exceed Linux's 128 KB
  per-argument limit — feed it to `jq` as a file/stdin (`--slurpfile`), never `--argjson "$(…)"`.

**Signatures**
- `app.create {app_name (required), train="stable", catalog_app, version="latest", values,
  custom_app=false, custom_compose_config, custom_compose_config_string}`
- `app.update <app_name> {values, custom_compose_config, custom_compose_config_string}` — no
  version: an update keeps the target's installed version, so the valid values depend on it.
- `app.delete` can also remove the app's ix-volumes (option) — never use that option. Host-path
  data is never touched by `app.delete`. Exact parameters: verify before first use.

**Catalog versions differ per host.** Each host's catalog syncs independently. `app.available`
gives only `latest_version` — no list of installable versions found, so there is no pre-check;
an `app.create` of a version the target's catalog lacks fails (refresh the catalog first). A
catalog version fixes the image tag, so a catalog app created from the source's version runs
the same image. An `ix-app` with `:latest` pulls whatever is newest at that moment.

**ix-volumes** (storage in TrueNAS's hidden `ix-apps` dataset): on these hosts they hold only
caches/temp that rebuild themselves — Redis (immich, firefly-iii), immich's ML cache, Plex
transcodes, forgejo tmp. All real app data lives in host paths.

## VMs

- `vm.stop` is a **job** → wait for `SUCCESS`, then confirm `.status.state == STOPPED`.
  `vm.start` is **synchronous** → poll the state.
- Stops must be **graceful**. A forced poweroff flips `.state` to `STOPPED` while libvirt is
  still tearing the domain down, so a snapshot taken then is not of a cleanly shut-down guest. A
  graceful stop reaches `STOPPED` only once the domain is `SHUTOFF`.
- **`vm.delete` right after a stop can leave an orphan domain.** A VM started by TrueNAS gets a
  post-stop thread (`run_post_stop_actions`, `middlewared/plugins/vm/supervisor/supervisor.py`)
  that polls its state every 5 s. If `vm.delete` undefines the domain before that thread's next
  poll, the thread's health check defines it again (old `<id>_<name>`, same UUID) and the delete
  then removes the VM record. The next `vm.create` with that UUID fails with
  `domain '<id>_<name>' is already defined with uuid …`. `vm.delete` waits 7 s itself only when it
  has to power the VM off. The replication waits 10 s after stopping the target and undefines a
  remaining orphan before `vm.create`; manual recovery: [operations.md](operations.md#vm-orphan-libvirt-domain).
- A still-booting guest ignores ACPI shutdown — `vm.query` shows `RUNNING` either way.
- `virsh` needs TrueNAS's own sockets (the default `libvirtd.socket` is masked):
  `virsh -c 'qemu+unix:///system?socket=/run/truenas_libvirt/libvirt-sock' …` (root; polkit
  refuses others), or read-only for any user:
  `virsh -r -c 'qemu+unix:///system?socket=/run/truenas_libvirt/libvirt-sock-ro' …`.

## Host tools

- `jq` on the hosts is 1.6: `walk()` and lookaround regexes work.
- `/tmp` is tmpfs and `noexec` — never put scripts or logs that must survive there.
- `/root` lives in the boot environment and is replaced by an update; persistent root files live
  in `/mnt/<pool>/homedir-ds/home/root` (unencrypted on purpose, reachable before unlock).
