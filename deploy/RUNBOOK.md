# mini-redis-go — RUNBOOK

Operational guide for deploying and running the server on a VPS via Docker +
systemd. For what the server *is*, see the top-level `README.md`.

Conventions used below: image is `thatdeparted2061/mini-redis-go:<tag>`, the AOF
lives on the host bind-mount `/var/lib/mini-redis/appendonly.aof`, and the
service is the systemd unit `mini-redis` (`deploy/systemd/mini-redis.service`).

---

## Section 1 — From-zero deployment (fresh VPS → running service)

Copy-paste, top to bottom, on a brand-new Ubuntu/Debian VPS.

```bash
# 1. Harden the box (do this before anything is listening).
sudo apt-get update && sudo apt-get -y upgrade
sudo adduser --disabled-password --gecos "" harsh
sudo usermod -aG sudo harsh
# SSH keys only: put your pubkey in /home/harsh/.ssh/authorized_keys, then:
sudo sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/'               /etc/ssh/sshd_config
sudo systemctl restart ssh
sudo apt-get -y install ufw fail2ban unattended-upgrades
sudo ufw default deny incoming && sudo ufw allow OpenSSH && sudo ufw --force enable
sudo dpkg-reconfigure -plow unattended-upgrades

# 2. Install Docker.
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker harsh            # let the unit run docker; re-login after

# 3. Ship the code (clone the repo, or scp deploy/ up).
git clone https://github.com/ThatDeparted2061/mini-redis-go.git
cd mini-redis-go

# 4. Install + start the service (it pulls + runs the published image).
sudo cp deploy/systemd/mini-redis.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mini-redis

# 5. Verify.
systemctl status mini-redis
redis-cli -p 6380 ping                   # → PONG  (run on the box, or via tunnel)
```

The AOF dir `/var/lib/mini-redis` is created and chowned to the container's
nonroot uid (65532) automatically by the unit's `ExecStartPre` — no manual step.

**Reaching the port (no `AUTH` yet).** The unit publishes to
`127.0.0.1:6380:6380` (loopback only). Never expose 6380 to the internet — an
unauthenticated DB is compromised within minutes. Connect over an SSH tunnel:

```bash
ssh -L 6380:localhost:6380 you@vps
redis-cli -p 6380 ping        # → PONG, over the encrypted tunnel
```

`stunnel`/TLS to expose it safely is the upgrade path, but only *after* `AUTH`
lands — TLS encrypts the channel, it doesn't authenticate the caller.

Optional: buy a domain, point an A record at the VPS, and `deploy/caddy/Caddyfile`
serves an HTTPS status page. Caddy is HTTP-only — it does **not** proxy the raw
Redis port; it's a placeholder for a future `/metrics` endpoint.

---

## Section 2 — Updates (push new image, restart, verify)

```bash
# From your laptop: build + push the new tag.
docker build -t thatdeparted2061/mini-redis-go:1.1 -f deploy/Dockerfile .
docker push thatdeparted2061/mini-redis-go:1.1

# On the VPS: point the unit at the new tag and restart.
sudo sed -i 's#mini-redis-go:1.0#mini-redis-go:1.1#' /etc/systemd/system/mini-redis.service
sudo systemctl daemon-reload
sudo systemctl restart mini-redis        # graceful: docker stop → SIGTERM → AOF fsync

# Verify.
systemctl status mini-redis
docker inspect --format '{{.Config.Image}}' mini-redis   # → ...:1.1
redis-cli -p 6380 ping                                   # → PONG
```

Data survives the restart: the AOF is on the host bind-mount and is replayed on
boot. Always use an explicit version tag in the unit, never `latest` — Section 3
depends on it.

---

## Section 3 — Rollback (pin previous image, restart)

Something's wrong after an update. Revert to the last-good tag:

```bash
sudo sed -i 's#mini-redis-go:1.1#mini-redis-go:1.0#' /etc/systemd/system/mini-redis.service
sudo systemctl daemon-reload
sudo systemctl restart mini-redis
docker inspect --format '{{.Config.Image}}' mini-redis   # → ...:1.0
```

The AOF format is append-only RESP commands and hasn't changed across these
tags, so an older binary replays a newer AOF fine. If a release ever changes the
on-disk format, a rollback also needs a data restore — see Section 4.

---

## Section 4 — Disaster recovery (restore from B2 backup)

Backups: `deploy/backup/backup.sh` (installed as `/etc/cron.daily/backup-mini-redis`)
gzips the AOF nightly and uploads it to Backblaze B2 via rclone, keeping 30 days.

**One-time setup:**
```bash
sudo apt-get -y install rclone
sudo rclone config          # new remote named "b2", type "b2", paste keyID + appKey
sudo cp deploy/backup/backup.sh /etc/cron.daily/backup-mini-redis
sudo chmod +x /etc/cron.daily/backup-mini-redis
# Smoke-test it now (don't wait for cron):
sudo /etc/cron.daily/backup-mini-redis
```

**Restore** (AOF lost/corrupted, or migrating to a new VPS):
```bash
sudo systemctl stop mini-redis                       # stop writers first

# Pick a backup (newest last) and pull it down.
rclone lsf b2:mini-redis-backups | sort | tail
rclone copyto b2:mini-redis-backups/appendonly-<STAMP>.aof.gz /tmp/restore.aof.gz

# Decompress into place, fix ownership, restart.
sudo sh -c 'gzip -dc /tmp/restore.aof.gz > /var/lib/mini-redis/appendonly.aof'
sudo chown 65532:65532 /var/lib/mini-redis/appendonly.aof
sudo systemctl start mini-redis

redis-cli -p 6380 get <a-known-key>                  # confirm data is back
```

On start the server replays the restored AOF and rebuilds the keyspace. A torn
tail from the last backup is tolerated (replay stops at the last whole command).

---

## Section 5 — Ops tasks

**Checking replication lag.** There is **no `INFO` command yet**, so lag isn't a
single number. Two honest checks:

```bash
# 1. The primary logs any replica silent > 30s (StaleReplicas heartbeat check).
journalctl -u mini-redis | grep -i 'stale\|replica'

# 2. Probe end-to-end: write on the primary, read on the replica.
redis-cli -p 6380 set __lag_probe "$(date +%s)"      # primary
redis-cli -p 6381 get __lag_probe                     # replica (via its own tunnel)
```

If the replica's value trails or is missing, it's behind. Note the v1 contract:
replicas mirror only writes made *after* they connect (no snapshot bootstrap),
and a slow replica is dropped-and-logged rather than disconnected — so a drifted
replica needs a restart to catch up (it comes back empty and takes new writes).

**Forcing an AOF rewrite.** There is **no `BGREWRITEAOF` command yet**. Rewrite
is automatic: a background goroutine compacts once the AOF passes a 64 KiB floor
*and* has doubled since the last rewrite. To shrink it on demand today you'd add
that command (upgrade path) — restarting does **not** compact (replay loads the
log as-is). Watch a rewrite happen:

```bash
ls -l /var/lib/mini-redis/appendonly.aof     # size before
journalctl -u mini-redis -f | grep -i rewrite
```

**Growing the data volume.** The AOF is on the host bind-mount, so "grow the
volume" = grow the VPS disk/filesystem:

```bash
df -h /var/lib/mini-redis                     # check headroom
# After resizing the disk in your VPS provider's panel:
sudo growpart /dev/sda 1                       # extend the partition
sudo resize2fs /dev/sda1                        # extend the filesystem (xfs: xfs_growfs)
df -h /var/lib/mini-redis                       # confirm the new size
```

No service restart needed — it's the same mount the container already writes.

---

## Section 6 — Incident log

Start empty; append newest-first. Real entries here are the L2 evidence.

Template:

```
### YYYY-MM-DD — <one-line title>
- Impact:     <who/what was affected, for how long>
- Detection:  <alert / how you found out>
- Cause:      <root cause>
- Fix:        <what you did>
- Follow-up:  <prevention / TODO>
```

<!-- no incidents yet -->
