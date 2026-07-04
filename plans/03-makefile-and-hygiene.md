# Plan 03 — Makefile fixes & repo hygiene

## Decided (2026-07-05): deleted, not fixed

SFTP (`.vscode/sftp.json`, profiles for both hosts) is confirmed as the real,
working, actively-used deploy mechanism — proven during the
`homedir-ds` → `encrypted-ds/app-ds` migration (both hosts now fully deployed
this way, verified via `--test`). The Makefile's `promote`/`rollback` targets
were broken 3 independent ways (items 1–4 below, kept for history) and there
was no evidence they'd ever completed a real deploy. Deleted `Makefile`
outright rather than fixing it; `AGENTS.md` documents SFTP as the mechanism.

Items 1–4 (the actual fixes) are moot post-deletion and removed from this
file. Items 5–6 below are unrelated repo hygiene, still open.

---

## 5. Add `.gitattributes` (line-ending safety)

Per the user's own way-of-working rule, and genuinely load-bearing here: the
repo is edited on Windows and deployed to Linux; a CRLF-infected bash file
fails on TrueNAS with cryptic `\r` errors. Create `.gitattributes` at the repo
root:

```
* text=auto
*.bash text eol=lf
*.json text eol=lf
bin/sync_truenas_servers text eol=lf
```

Then normalize once: `git add --renormalize .` and commit. Also spot-check the
SFTP-synced copies on the host afterwards (`file bin/sync_truenas_servers` or
`grep -rl $'\r' lib/` on the server) since SFTP copies bytes as-is.

## 6. Add a README.md (optional)

Repo-hygiene convention (README + .gitignore + .gitattributes). A short
README: what it does (2 sentences), the two example invocations from
`CLAUDE.md`, deploy mechanism (SFTP), and a pointer to
`config/config.example.bash`. Don't duplicate `CLAUDE.md` content at length.
