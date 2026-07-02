# Plan 03 — Makefile fixes & repo hygiene

The `promote`/`rollback` Makefile targets are broken in three independent ways
— strong evidence they've never completed a real deploy (SFTP sync via
`.vscode/sftp.json` is presumably the actual deploy path). **Decide first:**
either fix the Makefile (items 1–4) or delete it and document SFTP as the
deploy mechanism. Don't keep a broken deploy tool around.

---

## 1. `deploy` flattens the directory tree — [Makefile:21](../Makefile#L21)

```make
cd $(SOURCE_DIR) && git ls-files | xargs -I{} rsync -a {} $(TARGET_DIR)/
```

`rsync -a lib/cli.bash TARGET/` copies to `TARGET/cli.bash` (basename only) —
every tracked file lands flat in `TARGET_DIR`, destroying the `bin/`, `lib/`,
`config/` layout the script requires (`SCRIPT_DIR/../lib`, `../config`).

**Fix:** use `--files-from`, which preserves relative paths:

```make
cd $(SOURCE_DIR) && git ls-files | rsync -a --files-from=- $(SOURCE_DIR)/ $(TARGET_DIR)/
```

## 2. `rollback` can never find its backup — [Makefile:3](../Makefile#L3) + [Makefile:28-35](../Makefile#L28-L35)

`BACKUP_DIR` embeds `$(shell date +%Y%m%d%H%M%S)`, evaluated **per make
invocation**. `make promote` writes `backup_<T1>`; a later `make rollback`
computes a fresh `backup_<T2>` that doesn't exist → always "No backup found".

**Fix:** make rollback find the newest backup at runtime:

```make
rollback:
	@LATEST=$$(ls -dt $(dir $(TARGET_DIR))backup_* 2>/dev/null | head -1); \
	if [ -n "$$LATEST" ]; then \
		echo "Rolling back to $$LATEST"; \
		rm -rf $(TARGET_DIR); \
		cp -a $$LATEST $(TARGET_DIR); \
	else \
		echo "No backup found to roll back to"; \
	fi
```

## 3. `config.local.bash` is looked up at the wrong path — [Makefile:23-26](../Makefile#L23-L26)

The file lives at `config/config.local.bash`, but the Makefile tests and copies
`$(SOURCE_DIR)/config.local.bash` — the branch never fires, so a fresh deploy
target has **no config** and the script refuses to start.

**Fix:**

```make
	@if [ -f "$(SOURCE_DIR)/config/config.local.bash" ]; then \
		echo "Copying local config"; \
		rsync -a $(SOURCE_DIR)/config/config.local.bash $(TARGET_DIR)/config/; \
	fi
```

## 4. Paths hardcoded to the backup server — [Makefile:1-3](../Makefile#L1-L3)

`/mnt/backup-pool/homedir-ds/...` is only correct on `truenas-backup`. Running
`make promote` on master would create/overwrite wrong paths. Either derive the
pool from the hostname:

```make
POOL := $(shell hostname -s | grep -q master && echo master-pool || echo backup-pool)
SOURCE_DIR := /mnt/$(POOL)/homedir-ds/home/admin/bin/sync_truenas_servers
TARGET_DIR := /mnt/$(POOL)/homedir-ds/home/root/bin/sync_truenas_servers
```

…or, minimum viable: add a guard target that aborts unless
`hostname -s = truenas-backup`. **Before using the derived form on master,
verify** that master's home datasets really live at
`/mnt/master-pool/homedir-ds/` — this was inferred, not confirmed.

## 5. Add `.gitattributes` (line-ending safety)

Per the user's own way-of-working rule, and genuinely load-bearing here: the
repo is edited on Windows and deployed to Linux; a CRLF-infected bash file
fails on TrueNAS with cryptic `\r` errors. Create `.gitattributes` at the repo
root:

```
* text=auto
*.bash text eol=lf
*.json text eol=lf
Makefile text eol=lf
bin/sync_truenas_servers text eol=lf
```

Then normalize once: `git add --renormalize .` and commit. Also spot-check the
SFTP-synced copies on the host afterwards (`file bin/sync_truenas_servers` or
`grep -rl $'\r' lib/` on the server) since SFTP copies bytes as-is.

## 6. Add a README.md (optional)

Repo-hygiene convention (README + .gitignore + .gitattributes). A short
README: what it does (2 sentences), the two example invocations from
`CLAUDE.md`, deploy mechanism (SFTP and/or `make promote`), and a pointer to
`config/config.example.bash`. Don't duplicate `CLAUDE.md` content at length.

---

## Verification checklist

1. On the TrueNAS host: `make promote`, then diff the deployed tree:
   `cd <SOURCE_DIR> && git ls-files | while read f; do cmp -s "$f" <TARGET_DIR>/"$f" || echo "DIFFERS: $f"; done`
   — no output, and `TARGET_DIR/bin`, `lib`, `config` subdirs must exist.
2. `make rollback` after a promote restores the previous tree.
3. `./bin/sync_truenas_servers --test --task=master_to_backup` from the
   deployed copy works (proves config landed in `config/`).
