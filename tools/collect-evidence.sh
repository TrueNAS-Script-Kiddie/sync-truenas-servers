#!/usr/bin/bash
# tools/collect-evidence.sh
# Read-only evidence collector for plans/replication-redesign.md (phase 1). Not part of a
# replication run. Reads a workload's definition, files and databases, masks secrets, scans
# for host tokens (IP, hostname, pool names, -master/-backup). Its single write is the
# archive: <repo>/tmp/evidence/evidence-<server_id>-<date>.tar.gz
#
# Usage (as root, on each host, from the deployed copy):
#   bash tools/collect-evidence.sh --workload grafana
#
# Asks for a masking passphrase: type the SAME one on both hosts, so an identical secret
# gets an identical placeholder on both. It is kept in memory only, never written.

SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

if [[ -z "${EVIDENCE_KEY}" ]]; then
    read -rsp "Masking passphrase (same on both hosts, 8+ characters): " EVIDENCE_KEY
    echo
fi
export EVIDENCE_KEY

exec python3 -B "${SCRIPT_DIR}/collect_evidence.py" "$@"
