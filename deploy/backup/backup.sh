#!/usr/bin/env bash
# Daily off-site backup of the mini-redis AOF to Backblaze B2 via rclone.
# Installed as /etc/cron.daily/backup-mini-redis (see deploy/RUNBOOK.md §4).
#
# Uploads the gzipped AOF under a UTC-dated name, then prunes remote copies
# older than $RETAIN_DAYS. The live AOF may be mid-append; gzip captures a valid
# prefix and replay tolerates a torn tail, so the server does NOT need to stop.
#
# ponytail: rclone does the transport + age-based pruning; this is just glue.
# Configure once as root:  sudo rclone config   (remote name must match B2_REMOTE)
# Dry-run test:            B2_REMOTE=... rclone copyto --dry-run <file> <dest>
set -euo pipefail

AOF="${AOF_PATH:-/var/lib/mini-redis/appendonly.aof}"
REMOTE="${B2_REMOTE:-b2:mini-redis-backups}"   # rclone <remote>:<bucket>[/prefix]
RETAIN_DAYS="${RETAIN_DAYS:-30}"

[ -f "$AOF" ] || { echo "backup: no AOF at $AOF, nothing to do" >&2; exit 0; }

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

gzip -c "$AOF" > "$tmp"
rclone copyto "$tmp" "$REMOTE/appendonly-$stamp.aof.gz"
rclone delete --min-age "${RETAIN_DAYS}d" "$REMOTE"   # 30-day retention

echo "backup: uploaded appendonly-$stamp.aof.gz to $REMOTE"
