#!/usr/bin/python3
"""Read-only evidence collector for plans/replication-redesign.md, phase 1.

Started by tools/collect-evidence.sh (root, on either host). Reads definitions, files and
databases of the selected workloads, masks secrets, scans for host tokens and writes one
archive. The only thing it writes is that archive, under <repo>/tmp/evidence/.

Python 3.11, standard library only.
"""
import argparse
import base64
import datetime
import hashlib
import hmac
import io
import json
import os
import re
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.parse

HOSTS = {
    "master": {"ip": "192.168.0.10", "hostname": "truenas-master",
               "pools": ["ssdmaster-pool", "master-pool"], "suffix": "-master"},
    "backup": {"ip": "192.168.0.60", "hostname": "truenas-backup",
               "pools": ["backup-pool"], "suffix": "-backup"},
}

SECRET_NAME = re.compile(
    r"pass|secret|token|private|credential|salt|rands|claim|cookie|secure|"
    r"api_?key|app_?key|[_.\-]key$|^email$|^login$", re.I)
# values worth chasing through every other output: real credentials, not personal data
CHASE_NAME = re.compile(r"pass|secret|token|private|credential|api_?key|app_?key|[_.\-]key$", re.I)
PEM_PRIVATE = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S)
PEM_OPEN = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----(?:[A-Za-z0-9+/=\s]|\\[nr])*")
PEM_CLOSE = re.compile(r"-----END [A-Z ]*PRIVATE KEY-----")
URL_CREDENTIAL =re.compile(r"(://[^\s/:@\"']+:)([^\s/@\"']+)(@)")
# key = value / key: value / KEY=value lines in ini, yaml, conf and env-style text
SECRET_LINE = re.compile(
    r"(?P<key>[\w.\-]*(?:pass|secret|token|private|credential|salt|claim|api_?key|app_?key)"
    r"[\w.\-]*)(?P<sep>\"?\s*[:=]\s*)(?P<val>[^\r\n]+)", re.I)
PLAIN_VALUE = re.compile(r"^(true|false|yes|no|on|off|null|none|~|-?\d+(\.\d+)?[a-z]{0,2})$", re.I)

CONFIG_EXT = {".ini", ".conf", ".cfg", ".cnf", ".yml", ".yaml", ".toml", ".json", ".xml",
              ".env", ".properties", ".config", ".list", ""}
SQLITE_MAGIC = b"SQLite format 3\x00"
TRACK_MIN = 8          # shorter secrets are masked where found, not chased elsewhere
MAX_VALUE_CHARS = 4000


class Collector:
    def __init__(self, key, server_id, args):
        self.key = key.encode()
        self.sid = server_id
        self.args = args
        self.out = {}            # relpath -> text, written (and re-masked) at the end
        self.secrets = set()     # masked values long enough to chase through every output
        self.chase = False       # on for definitions and container config, off for app data:
                                 # a plugin's sample value must not blank a word everywhere
        self.untracked = 0
        self.commands = []
        self.errors = []
        self.token_re, self.token_re_b = build_token_regex()

    # ---- masking -------------------------------------------------------------------------
    def tag(self, value, kind="masked"):
        digest = hmac.new(self.key, value.encode("utf-8", "replace"), hashlib.sha256)
        return f"<{kind}:{digest.hexdigest()[:10]}>"

    def mask(self, value, chase=None):
        """Equality-preserving placeholder: same value + same key -> same tag on both hosts."""
        if not isinstance(value, str):
            value = str(value)
        if value == "":
            return ""
        chase = self.chase if chase is None else chase
        if (chase and len(value) >= TRACK_MIN and not value.startswith("/")
                and not value.isalpha() and not PLAIN_VALUE.match(value)):
            self.secrets.add(value)
        else:
            self.untracked += 1
        return self.tag(value)

    def mask_json(self, node, force=False):
        if isinstance(node, dict):
            named = None
            for name_key in ("name", "key", "env", "variable"):
                if isinstance(node.get(name_key), str) and "value" in node:
                    named = node[name_key]
            result = {}
            for k, v in node.items():
                hit = force or bool(SECRET_NAME.search(str(k)))
                if k == "value" and named is not None and SECRET_NAME.search(named):
                    hit = True
                result[k] = self.mask_json(v, hit)
            return result
        if isinstance(node, list):
            return [self.mask_json(v, force) for v in node]
        if isinstance(node, str):
            if "=" in node and not force:           # docker Env entries: "NAME=value"
                name, _, val = node.partition("=")
                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) and SECRET_NAME.search(name):
                    return f"{name}={self.mask(val)}"
            return self.mask(node) if force else node
        return node

    def mask_text(self, text):
        def line(m):
            val = m.group("val").strip()
            bare = val.strip("\"',")
            if not bare or PLAIN_VALUE.match(bare) or bare.startswith("<masked:"):
                return m.group(0)
            return f"{m.group('key')}{m.group('sep')}{self.mask(bare)}"
        text = SECRET_LINE.sub(line, text)
        return URL_CREDENTIAL.sub(lambda m: m.group(1) + self.mask(m.group(2)) + m.group(3), text)

    def final_mask(self, text):
        text = PEM_PRIVATE.sub(lambda m: self.tag(m.group(0), "private-key"), text)
        # an unpaired marker (placeholder text in a plugin schema, or a truncated key): mask
        # the marker and whatever key material follows it
        text = PEM_OPEN.sub(lambda m: self.tag(m.group(0), "private-key-unterminated"), text)
        text = PEM_CLOSE.sub("<private-key-end-marker>", text)
        for secret in sorted(self.secrets, key=len, reverse=True):
            for variant in {secret, json.dumps(secret)[1:-1]}:
                if variant in text:
                    text = text.replace(variant, self.tag(secret))
        return text

    # ---- output --------------------------------------------------------------------------
    def put(self, relpath, text):
        self.out[relpath] = text

    def put_json(self, relpath, data, masked=True):
        if masked:
            data = self.mask_json(data)
        self.put(relpath, json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")

    def error(self, message):
        self.errors.append(message)
        print(f"  ! {message}", file=sys.stderr)

    # ---- commands (all read-only) --------------------------------------------------------
    def run(self, argv, timeout=120):
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, errors="replace",
                                  timeout=timeout)
            code, out, err = proc.returncode, proc.stdout, proc.stderr
        except (OSError, subprocess.TimeoutExpired) as exc:
            code, out, err = -1, "", str(exc)
        self.commands.append(f"[{code}] {' '.join(argv)}")
        if code != 0:
            self.error(f"exit {code}: {' '.join(argv)}: {err.strip()[:300]}")
            return None
        return out

    def midclt(self, method, *params):
        out = self.run(["midclt", "call", method, *[json.dumps(p) for p in params]])
        if out is None:
            return None
        try:
            return json.loads(out)
        except ValueError:
            return out.strip()      # midclt prints a bare string result unquoted

    # ---- platform and inventory ----------------------------------------------------------
    def collect_platform(self):
        self.put_json("platform/system.version.json", self.midclt("system.version"))
        apps = self.midclt("app.query") or []
        self.put_json("platform/inventory-apps.json", [
            {"name": a.get("name"), "state": a.get("state"), "version": a.get("version"),
             "custom_app": a.get("custom_app"),
             "catalog_app": (a.get("metadata") or {}).get("name"),
             "train": (a.get("metadata") or {}).get("train"),
             "app_version": (a.get("metadata") or {}).get("app_version")} for a in apps])
        vms = self.midclt("vm.query") or []
        self.put_json("platform/inventory-vms.json", [
            {"name": v.get("name"), "id": v.get("id"),
             "state": (v.get("status") or {}).get("state")} for v in vms])
        for service in ("app", "vm", "virt.instance", "catalog"):
            methods = self.midclt("core.get_methods", service)
            if methods is not None:
                self.put_json(f"platform/methods-{service}.json", methods, masked=False)
        certs = self.midclt("certificate.query") or []
        self.put_json("platform/certificates.json", [
            {k: c.get(k) for k in ("id", "name", "common", "san", "issuer", "from", "until",
                                   "cert_type", "fingerprint")} for c in certs])
        return apps

    # ---- apps ----------------------------------------------------------------------------
    def collect_app(self, bare, apps):
        base = f"apps/{bare}"
        names = [a["name"] for a in apps if a.get("name") in (f"{bare}-{self.sid}", bare)]
        if not names:
            self.put(f"{base}/NOT-INSTALLED.txt",
                     f"No app named {bare}-{self.sid} or {bare} on truenas-{self.sid}.\n")
            print(f"  {bare}: not installed on this host")
            return
        name = names[0]
        query = next(a for a in apps if a["name"] == name)
        print(f"  {bare}: {name} ({query.get('state')}, catalog version {query.get('version')})")
        self.chase = True
        self.put_json(f"{base}/app.query.json", query)
        config = self.midclt("app.config", name) or {}
        self.put_json(f"{base}/app.config.json", config)
        catalog_app = (query.get("metadata") or {}).get("name")
        available = self.midclt("app.available", [["name", "=", catalog_app]]) or []
        self.put_json(f"{base}/app.available.json", truncate_strings(available, 2000))

        self.put_json(f"{base}/token-hits-definition.json",
                      self.scan_json(strip_generated(config)), masked=False)

        self.collect_app_config_dir(base, name, query.get("version"))
        mounts = self.collect_containers(base, name)
        for path in host_paths(config):
            mounts.setdefault(path, {"source": "app.config"})
        roots = sorted(p for p in mounts if not p.startswith("/mnt/.ix-apps/"))
        roots = [p for p in roots if not any(p != q and p.startswith(q.rstrip("/") + "/")
                                             for q in roots)]
        self.put_json(f"{base}/mounts.json", {"all": mounts, "collected_roots": roots},
                      masked=False)
        self.chase = False
        for index, root in enumerate(roots):
            self.collect_tree(f"{base}/data/{index}-{os.path.basename(root.rstrip('/'))}", root)

    def collect_app_config_dir(self, base, name, version):
        root = f"/mnt/.ix-apps/app_configs/{name}"
        if not os.path.isdir(root):
            self.error(f"{root}: missing")
            return
        listing = []
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames.sort()
            for filename in sorted(filenames):
                listing.append(os.path.relpath(os.path.join(dirpath, filename), root))
        self.put(f"{base}/app_configs/tree.txt", "\n".join(listing) + "\n")
        wanted = [f for f in listing if f == "metadata.yaml" or (
            f.startswith(f"versions/{version}/") and "/templates/library/" not in f
            and "/test_values/" not in f and "/migrations/" not in f
            and f.endswith((".yaml", ".yml", ".json")))]
        for rel in wanted:
            text = read_text(os.path.join(root, rel), self.args.max_file_bytes)
            if text is not None:
                self.put(f"{base}/app_configs/{rel}", self.mask_content(rel, text))

    def collect_containers(self, base, name):
        mounts = {}
        out = self.run(["docker", "ps", "-a", "--no-trunc", "--format", "{{json .}}"]) or ""
        rows = [json.loads(line) for line in out.splitlines() if line.strip()]
        rows = [r for r in rows if re.search(rf"(^|-){re.escape(name)}-", r.get("Names", ""))]
        self.put_json(f"{base}/docker/ps.json", rows)
        images = set()
        for row in rows:
            cname = row["Names"]
            inspect = self.run(["docker", "inspect", cname])
            if inspect is None:
                continue
            data = json.loads(inspect)
            self.put_json(f"{base}/docker/inspect-{cname}.json", data)
            for item in data:
                images.add(item.get("Image", ""))
                for mount in item.get("Mounts") or []:
                    source = mount.get("Source", "")
                    if mount.get("Type") == "bind" and source.startswith("/mnt/"):
                        mounts[source] = {"source": "container", "container": cname,
                                          "destination": mount.get("Destination"),
                                          "rw": mount.get("RW")}
        for image in sorted(i for i in images if i):
            inspect = self.run(["docker", "image", "inspect", image])
            if inspect is not None:
                self.put_json(f"{base}/docker/image-{image.replace(':', '_')[:40]}.json", [
                    {k: i.get(k) for k in ("Id", "RepoTags", "RepoDigests", "Created",
                                           "Architecture", "Os")} for i in json.loads(inspect)])
        if not rows:
            self.put(f"{base}/docker/NO-CONTAINERS.txt",
                     "App not running: containers only exist while it runs.\n")
        return mounts

    # ---- file trees ----------------------------------------------------------------------
    def collect_tree(self, base, root):
        if not os.path.exists(root):
            self.put(f"{base}/MISSING.txt", f"{root} does not exist on this host.\n")
            return
        print(f"    tree {root}")
        self.collect_dataset(base, root)
        entries, sqlite_files, hits, skipped, uids, gids = [], [], [], [], set(), set()
        candidates = []
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames.sort()
            for entry in [None] + sorted(dirnames + filenames):
                path = dirpath if entry is None else os.path.join(dirpath, entry)
                if entry is None and dirpath != root:
                    continue
                try:
                    st = os.lstat(path)
                except OSError as exc:
                    self.error(f"lstat {path}: {exc}")
                    continue
                rel = os.path.relpath(path, root)
                kind = ("d" if stat.S_ISDIR(st.st_mode) else "l" if stat.S_ISLNK(st.st_mode)
                        else "f" if stat.S_ISREG(st.st_mode) else "o")
                target = os.readlink(path) if kind == "l" else ""
                try:
                    xattrs = ",".join(sorted(os.listxattr(path, follow_symlinks=False)))
                except OSError:
                    xattrs = "?"
                uids.add(st.st_uid)
                gids.add(st.st_gid)
                entries.append((rel, kind, oct(stat.S_IMODE(st.st_mode)), st.st_uid, st.st_gid,
                                st.st_size, datetime.date.fromtimestamp(st.st_mtime).isoformat(),
                                xattrs, target))
                if kind == "f":
                    candidates.append((rel, path, st.st_size))
        self.write_tree(base, root, entries)
        self.put(f"{base}/owners.txt", self.owner_names(uids, gids))

        included = 0
        for rel, path, size in sorted(candidates, key=lambda c: (c[0].count(os.sep), c[0])):
            if size > self.args.max_scan_bytes:
                skipped.append(f"{rel}\t{size}\tnot scanned: over --max-scan-bytes")
                continue
            try:
                with open(path, "rb") as handle:
                    blob = handle.read()
            except OSError as exc:
                self.error(f"read {path}: {exc}")
                continue
            if blob.startswith(SQLITE_MAGIC):
                sqlite_files.append((rel, path))
                continue
            if rel.endswith(("-wal", "-shm", "-journal")):
                continue
            text = decode_text(blob)
            found = self.token_re_b.findall(blob)
            if found:
                hits.append(self.file_hits(rel, text, found))
            config_like = os.path.splitext(rel)[1].lower() in CONFIG_EXT
            if text is None or size > self.args.max_file_bytes or not (found or config_like):
                continue
            if not found and included >= self.args.max_files:
                skipped.append(f"{rel}\t{size}\tcontent not included: over --max-files")
                continue
            included += 1
            self.put(f"{base}/files/{rel.replace(os.sep, '/')}", self.mask_content(rel, text))
        self.put_json(f"{base}/token-hits-files.json", hits)
        self.put(f"{base}/skipped.txt", "\n".join(skipped) + "\n")
        for rel, path in sqlite_files:
            self.collect_sqlite(f"{base}/sqlite/{rel.replace(os.sep, '/')}", path)

    def write_tree(self, base, root, entries):
        header = "path\ttype\tmode\tuid\tgid\tsize\tmtime\txattrs\tlink_target\n"
        total = sum(e[5] for e in entries if e[1] == "f")
        summary = f"root: {root}\nentries: {len(entries)}\nfile bytes: {total}\n"
        if len(entries) <= self.args.max_tree_entries:
            self.put(f"{base}/tree.tsv",
                     header + "".join("\t".join(map(str, e)) + "\n" for e in entries))
        else:   # bulk content: per-directory totals and a sample instead of every entry
            per_dir = {}
            for e in entries:
                parts = e[0].split(os.sep)
                key = os.sep.join(parts[:3]) if len(parts) > 3 else os.path.dirname(e[0]) or "."
                count, size, owners = per_dir.get(key, (0, 0, set()))
                per_dir[key] = (count + 1, size + (e[5] if e[1] == "f" else 0),
                                owners | {f"{e[3]}:{e[4]}:{e[2]}"})
            self.put(f"{base}/tree-summary.tsv", "dir(depth<=3)\tentries\tfile_bytes\tuid:gid:mode\n"
                     + "".join(f"{k}\t{c}\t{s}\t{' '.join(sorted(o))}\n"
                               for k, (c, s, o) in sorted(per_dir.items())))
            step = max(1, len(entries) // 2000)
            self.put(f"{base}/tree-sample.tsv",
                     header + "".join("\t".join(map(str, e)) + "\n" for e in entries[::step]))
            summary += f"over --max-tree-entries: summary + every {step}th entry only\n"
        self.put(f"{base}/tree-summary.txt", summary)

    def owner_names(self, uids, gids):
        lines = []
        for kind, ids, database in (("uid", uids, "passwd"), ("gid", gids, "group")):
            for number in sorted(ids):
                out = self.run(["getent", database, str(number)]) if database else None
                name = out.split(":")[0] if out else "(no name on this host)"
                lines.append(f"{kind}\t{number}\t{name}")
        return "\n".join(lines) + "\n"

    def collect_dataset(self, base, root):
        dataset = (self.run(["findmnt", "-n", "-o", "SOURCE", "-T", root]) or "").strip()
        if not dataset:
            return
        props = self.run(["zfs", "get", "-H", "-o", "property,value,source", "all", dataset])
        self.put(f"{base}/dataset.txt", f"dataset: {dataset}\n{props or ''}")
        acltype = ""
        for line in (props or "").splitlines():
            if line.startswith("acltype\t"):
                acltype = line.split("\t")[1]
        tool = ["nfs4xdr_getfacl"] if acltype == "nfsv4" else ["getfacl", "-n", "-p"]
        acls = []
        targets = [root] + sorted(os.path.join(root, e) for e in os.listdir(root))[:50]
        for target in targets:
            acls.append(f"### {target}\n{self.run(tool + [target]) or '(failed)'}")
        self.put(f"{base}/acl.txt", f"acltype: {acltype}\n" + "\n".join(acls))

    def mask_content(self, rel, text):
        if rel.lower().endswith(".json") or text.lstrip()[:1] in ("{", "["):
            try:
                return json.dumps(self.mask_json(json.loads(text)), indent=2) + "\n"
            except ValueError:
                pass
        return self.mask_text(text)

    # ---- token scan ----------------------------------------------------------------------
    def classify(self, token):
        for sid, host in HOSTS.items():
            if token in (host["ip"], host["hostname"], host["suffix"], *host["pools"]):
                return f"{'own' if sid == self.sid else 'peer'}:{token}"
        return token

    def file_hits(self, rel, text, found):
        counts = {}
        for token in found:
            label = self.classify(token.decode())
            counts[label] = counts.get(label, 0) + 1
        result = {"file": rel, "counts": counts, "binary": text is None}
        if text is not None:
            lines = []
            for number, line in enumerate(text.splitlines(), 1):
                tokens = self.token_re.findall(line)
                if tokens and len(lines) < self.args.max_hit_lines:
                    lines.append({"line": number,
                                  "tokens": sorted({self.classify(t) for t in tokens}),
                                  "text": self.mask_text(excerpt(line, self.token_re))})
            result["lines"] = lines
        return result

    def scan_json(self, node, path="$"):
        hits = []
        if isinstance(node, dict):
            for k, v in node.items():
                hits += self.scan_json(v, f"{path}.{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                hits += self.scan_json(v, f"{path}[{i}]")
        elif isinstance(node, str):
            tokens = self.token_re.findall(node)
            if tokens:
                secret = bool(SECRET_NAME.search(path.rsplit(".", 1)[-1]))
                hits.append({"path": path, "tokens": sorted({self.classify(t) for t in tokens}),
                             "value": self.mask(node) if secret else self.mask_text(node[:500])})
        return hits

    # ---- SQLite --------------------------------------------------------------------------
    def collect_sqlite(self, base, path):
        print(f"    sqlite {path}")
        workdir = tempfile.mkdtemp(prefix="evidence-sqlite-")   # root-only copy; source untouched
        try:
            copy = os.path.join(workdir, "db")
            shutil.copyfile(path, copy)
            for suffix in ("-wal", "-shm"):
                if os.path.exists(path + suffix):
                    shutil.copyfile(path + suffix, copy + suffix)
            db = sqlite3.connect(copy)
            db.text_factory = lambda b: b.decode("utf-8", "replace")
            self.dump_sqlite(base, db)
            db.close()
        except (sqlite3.Error, OSError) as exc:
            self.error(f"sqlite {path}: {exc}")
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    def dump_sqlite(self, base, db):
        schema = db.execute("SELECT type, name, tbl_name, sql FROM sqlite_master "
                            "ORDER BY tbl_name, type DESC, name").fetchall()
        self.put(f"{base}/schema.sql", "".join(f"{row[3]};\n" for row in schema if row[3]))
        tables = [row[1] for row in schema if row[0] == "table"]
        counts, hits = [], []
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            columns = [c[1] for c in db.execute(f"PRAGMA table_info({quoted})")]
            total = db.execute(f"SELECT count(*) FROM {quoted}").fetchone()[0]
            counts.append(f"{table}\t{total}")
            secret_table = bool(SECRET_NAME.search(table))
            table_hits, rows = {}, []
            keep_rows = total <= self.args.max_config_rows
            for row in db.execute(f"SELECT * FROM {quoted}"):
                record, row_hit = {}, False
                for column, value in zip(columns, row):
                    if isinstance(value, bytes):
                        text = decode_text(value)
                        if text is None:
                            record[column] = f"<blob {len(value)} bytes>"
                            continue
                        value = text
                    if isinstance(value, str):
                        tokens = self.token_re.findall(value)
                        for token in tokens:
                            row_hit = True
                            key = (column, self.classify(token))
                            entry = table_hits.setdefault(key, {"hits": 0, "samples": []})
                            entry["hits"] += 1
                            sample = self.mask_text(excerpt(value, self.token_re))
                            if len(entry["samples"]) < 5 and sample not in entry["samples"]:
                                entry["samples"].append(sample)
                        secret = (SECRET_NAME.search(column)
                                  or (secret_table and not column.lower().endswith("id")))
                        value = (self.mask(value, bool(CHASE_NAME.search(column))) if secret
                                 else self.mask_cell(value))
                    record[column] = value
                if keep_rows or (row_hit and len(rows) < self.args.max_config_rows):
                    rows.append(record)
            for (column, token), entry in sorted(table_hits.items()):
                hits.append({"table": table, "column": column, "token": token, **entry})
            if rows:
                note = "" if keep_rows else f"# only rows with a token hit, of {total}\n"
                self.put(f"{base}/rows/{table}.jsonl", note + "".join(
                    json.dumps(r, sort_keys=True, default=str) + "\n" for r in rows))
        self.put(f"{base}/tables.tsv", "table\trows\n" + "\n".join(counts) + "\n")
        self.put_json(f"{base}/token-hits.json", hits, masked=False)

    def mask_cell(self, value):
        stripped = value.lstrip()
        if stripped[:1] in "{[":
            try:
                value = json.dumps(self.mask_json(json.loads(value)), sort_keys=True)
            except ValueError:
                value = self.mask_text(value)
        else:
            value = self.mask_text(value)
        if len(value) > MAX_VALUE_CHARS:
            value = value[:MAX_VALUE_CHARS] + f"…<truncated, {len(value)} chars>"
        return value

    # ---- archive -------------------------------------------------------------------------
    def leak_check(self, texts):
        leaks = []
        for secret in self.secrets:
            variants = {secret, json.dumps(secret)[1:-1], urllib.parse.quote(secret, safe=""),
                        base64.b64encode(secret.encode()).decode()}
            for relpath, text in texts.items():
                if any(v in text for v in variants):
                    leaks.append(f"{relpath}: {self.tag(secret)}")
        for relpath, text in texts.items():
            if "PRIVATE KEY-----" in text:
                leaks.append(f"{relpath}: private key block")
        return leaks

    def write_archive(self, out_dir):
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
        self.put("commands.log", "\n".join(self.commands) + "\n")
        self.put("errors.txt", "\n".join(self.errors) + "\n")
        self.put_json("meta.json", {
            "host": socket.gethostname(), "server_id": self.sid, "collected": stamp,
            "workloads": self.args.workload, "tokens": HOSTS,
            "secrets_masked_and_chased": len(self.secrets),
            "short_secrets_masked_in_place_only": self.untracked,
            "limits": {k: v for k, v in vars(self.args).items() if k.startswith("max_")}},
            masked=False)
        texts = {rel: self.final_mask(text) for rel, text in self.out.items()}
        leaks = self.leak_check(texts)
        if leaks:
            print("LEAK CHECK FAILED - no archive written:", file=sys.stderr)
            print("\n".join(f"  {leak}" for leak in leaks), file=sys.stderr)
            return None
        os.makedirs(out_dir, mode=0o755, exist_ok=True)
        archive = os.path.join(out_dir, f"evidence-{self.sid}-{stamp}.tar.gz")
        prefix = f"evidence-{self.sid}"
        with tarfile.open(archive, "w:gz") as tar:
            for rel in sorted(texts):
                payload = texts[rel].encode("utf-8", "replace")
                info = tarfile.TarInfo(f"{prefix}/{rel}")
                info.size, info.mode, info.mtime = len(payload), 0o644, int(
                    datetime.datetime.now().timestamp())
                tar.addfile(info, io.BytesIO(payload))
        os.chmod(archive, 0o644)    # readable by the non-root SSH user that copies it off
        return archive


# ---- helpers -------------------------------------------------------------------------------
def build_token_regex():
    """One alternation, longest token first, so each position is reported once."""
    parts = []
    for host in HOSTS.values():
        parts.append((host["hostname"], re.escape(host["hostname"])))
        for pool in host["pools"]:
            parts.append((pool, r"(?<![A-Za-z0-9])" + re.escape(pool)))
        parts.append((host["ip"], r"(?<![0-9.])" + re.escape(host["ip"]) + r"(?![0-9])"))
        parts.append((host["suffix"], r"(?<=[A-Za-z0-9])" + re.escape(host["suffix"])
                      + r"(?![A-Za-z0-9])"))
    parts.sort(key=lambda p: len(p[0]), reverse=True)
    pattern = "(" + "|".join(p[1] for p in parts) + ")"
    return re.compile(pattern), re.compile(pattern.encode())


def excerpt(text, regex, width=150):
    match = regex.search(text)
    if not match or len(text) <= 2 * width:
        return text[:2 * width]
    start = max(0, match.start() - width)
    return ("…" if start else "") + text[start:match.end() + width] + "…"


def decode_text(blob):
    if b"\x00" in blob[:8192]:
        return None
    try:
        return blob.decode("utf-8")
    except UnicodeDecodeError:
        return None


def read_text(path, limit):
    try:
        if os.path.getsize(path) > limit:
            return None
        with open(path, "rb") as handle:
            return decode_text(handle.read())
    except OSError:
        return None


def strip_generated(config):
    return {k: v for k, v in config.items() if k not in (
        "ix_context", "ix_certificates", "ix_certificate_authorities", "ix_volumes",
        "release_name")}


def host_paths(node):
    paths = set()
    if isinstance(node, dict):
        for value in node.values():
            paths |= host_paths(value)
    elif isinstance(node, list):
        for value in node:
            paths |= host_paths(value)
    elif isinstance(node, str) and node.startswith("/mnt/") and "\n" not in node:
        paths.add(node)
    return paths


def truncate_strings(node, limit):
    if isinstance(node, dict):
        return {k: truncate_strings(v, limit) for k, v in node.items()}
    if isinstance(node, list):
        return [truncate_strings(v, limit) for v in node]
    if isinstance(node, str) and len(node) > limit:
        return node[:limit] + f"…<truncated, {len(node)} chars>"
    return node


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workload", action="append", required=True,
                        help="bare app name (grafana); repeatable")
    parser.add_argument("--max-tree-entries", type=int, default=20000)
    parser.add_argument("--max-files", type=int, default=200,
                        help="config-like files included in full per mount (hit files always)")
    parser.add_argument("--max-file-bytes", type=int, default=256 * 1024)
    parser.add_argument("--max-scan-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--max-config-rows", type=int, default=200,
                        help="tables up to this many rows are dumped in full")
    parser.add_argument("--max-hit-lines", type=int, default=20)
    args = parser.parse_args()

    key = os.environ.get("EVIDENCE_KEY", "")
    if len(key) < 8:
        sys.exit("EVIDENCE_KEY missing or shorter than 8 characters "
                 "(start this through tools/collect-evidence.sh).")
    if os.geteuid() != 0:
        sys.exit("Run as root: midclt and the app data need it.")
    server_id = socket.gethostname().split(".")[0].removeprefix("truenas-")
    if server_id not in HOSTS:
        sys.exit(f"Unknown host '{socket.gethostname()}': expected truenas-master or -backup.")

    collector = Collector(key, server_id, args)
    print(f"Collecting on truenas-{server_id} (read-only)")
    apps = collector.collect_platform()
    for workload in args.workload:
        collector.collect_app(workload, apps)
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    archive = collector.write_archive(os.path.join(repo, "tmp", "evidence"))
    if archive is None:
        sys.exit(1)
    print(f"Leak check passed ({len(collector.secrets)} secret values searched for).")
    print(f"{len(collector.errors)} error(s), listed in errors.txt inside the archive.")
    print(f"Archive: {archive}")


if __name__ == "__main__":
    main()
