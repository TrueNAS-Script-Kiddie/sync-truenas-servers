#!/usr/bin/bash
# tools/mountwatch.sh
# Standalone diagnostic watcher for the "dataset is busy" zfs recv failures
# (plans/08-known-operational-issues.md). Not part of the replication run --
# start it by hand on the TARGET host before a sync you want instrumented.
#
# Samples every dataset under ${DS_ROOT} and logs a block whenever the state
# changes. Two fields per dataset, plus one pool-level field:
#   mounted=  what the host reports (what zfs umount/mount changes)
#   ns=       processes that still have it mounted in THEIR namespace, counted
#             only while the host says it is unmounted -- so the expensive
#             /proc scan never runs during normal operation
#   freeing=  the pool's async-destroy backlog, as zero/nonzero so only the
#             transition triggers a block instead of every step of the counter
#
# mounted=no with ns>0, and freeing=nonzero during a receive, are the two states
# that put zfs recv on EBUSY while fuser and smbstatus show nothing at all.
#
# Usage (target host, as root):
#   setsid bash tools/mountwatch.sh >> /path/to/mountwatch.log 2>&1 < /dev/null &
#   disown
#
# setsid detaches it from the terminal: a plain 'nohup ... &' under sudo gets
# killed by sudo's process monitor. Keep the log off /tmp -- that is tmpfs and
# noexec on TrueNAS, so the log dies on reboot and a script there will not run.
# /mnt/<pool>-pool/homedir-ds/home/root/ is the persistent, unencrypted choice.

DS_ROOT="${DS_ROOT:-backup-pool/encrypted-ds}"
POOL="${POOL:-${DS_ROOT%%/*}}"
INTERVAL="${INTERVAL:-10}"
LOGTAIL="${LOGTAIL:-40}"

function Sample_state() {
    local DS MOUNTED N FREEING

    while read -r DS MOUNTED; do
        N=0
        [ "$MOUNTED" != yes ] && N=$(grep -l " /mnt/$DS zfs " /proc/[0-9]*/mounts 2>/dev/null | wc -l)
        echo "$DS mounted=$MOUNTED ns=$N"
    done < <(zfs get -r -t filesystem -H -o name,value mounted "$DS_ROOT")

    FREEING=$(zpool get -H -o value freeing "$POOL" 2>/dev/null)
    [ "$FREEING" = 0 ] && echo "$POOL freeing=zero" || echo "$POOL freeing=nonzero"
}

function Print_diagnostics() {
    local PREV="$1" CUR="$2"
    local DS PIDS

    if [ -n "$PREV" ]; then
        echo "--- diff vs previous sample:"
        diff <(echo "$PREV") <(echo "$CUR") | sed 's/^/    /'
    fi

    echo "--- SUSPECT (host reports unmounted, a namespace still holds it):"
    echo "$CUR" | awk '$2=="mounted=no" && $3!="ns=0" {print "    "$0}'
    echo "$CUR" | awk '$2=="mounted=no" && $3!="ns=0" {print $1}' | while read -r DS; do
        PIDS=$(grep -l " /mnt/$DS zfs " /proc/[0-9]*/mounts 2>/dev/null | cut -d/ -f3 | tr '\n' ' ')
        [ -n "$PIDS" ] && {
            echo "    $DS held by:"
            ps -o comm= -p $PIDS 2>/dev/null | sort | uniq -c | sort -rn | sed 's/^/      /'
        }
    done

    echo "--- pool $POOL, async-destroy backlog:"
    zpool get -H -o property,value freeing,health "$POOL" 2>&1 | sed 's/^/    /'
    zpool status "$POOL" 2>&1 | grep -E '^[[:space:]]*(state|scan):' | sed 's/^/    /'

    echo "--- fuser on what is mounted:"
    echo "$CUR" | awk '$2=="mounted=yes"{print $1}' | while read -r DS; do
        echo "  /mnt/$DS"
        fuser -vm "/mnt/$DS" 2>&1 | sed 's/^/    /'
    done

    echo "--- snapshots carrying a hold (userrefs > 0):"
    zfs get -r -t snapshot -H -o name,value userrefs "$DS_ROOT" 2>&1 | awk '$2>0' | sed 's/^/    /'

    echo "--- running zfs send/recv:"
    ps -eo pid,etime,args | grep -E 'zfs (send|recv|receive)' | grep -v grep | sed 's/^/    /'

    echo "--- smbstatus -L:"
    smbstatus -L 2>&1 | sed 's/^/    /'

    # Truncated: TrueNAS writes multi-KB python tracebacks as single lines, and
    # one of those swallows the whole block. The timestamp and the first part of
    # the message are all this tail is for.
    echo "--- middlewared.log, last $LOGTAIL lines:"
    tail -n "$LOGTAIL" /var/log/middlewared.log 2>&1 | cut -c1-200 | sed 's/^/    /'
}

PREV=""
while :; do
    CUR=$(Sample_state)
    if [ "$CUR" != "$PREV" ]; then
        echo "=== $(date +%F_%T)"
        echo "$CUR"
        Print_diagnostics "$PREV" "$CUR"
        PREV="$CUR"
    fi
    sleep "$INTERVAL"
done
