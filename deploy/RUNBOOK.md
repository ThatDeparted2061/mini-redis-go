# mini-redis-go — RUNBOOK

Operational guide for deploying and running the server on a VPS via Docker +
systemd. For what the server *is*, see the top-level `README.md`.

## Deploy

1. **Build & push the image** (from your laptop):
   ```bash
   docker build -t thatdeparted2061/mini-redis-go:1.0 -f deploy/Dockerfile .
   docker push thatdeparted2061/mini-redis-go:1.0
   ```
2. **On the VPS**, install the systemd unit (it pulls + runs the image):
   ```bash
   sudo usermod -aG docker harsh                 # one-time: let the unit run docker
   sudo cp deploy/systemd/mini-redis.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now mini-redis
   ```
   The AOF directory `/var/lib/mini-redis` is created and chowned to the
   container's nonroot uid (65532) automatically by the unit's `ExecStartPre`.

## Operate

```bash
systemctl status mini-redis          # is it up?
journalctl -u mini-redis -f          # follow logs
sudo systemctl restart mini-redis    # restart
sudo systemctl stop mini-redis       # stop (graceful: docker stop → SIGTERM → AOF fsync)
```

## Reaching the Redis port — the decision

The server has **no `AUTH` yet**, so port 6380 must **never** be exposed to the
public internet. Three options the guide raises, and what we chose:

- ❌ **Expose 6380 directly.** Rejected — an unauthenticated database open to the
  internet is compromised within minutes.
- ✅ **Localhost-only + SSH tunnel (current).** The unit publishes to
  `127.0.0.1:6380:6380`, so only the VPS itself can reach it. You connect from
  your laptop through an SSH tunnel:
  ```bash
  ssh -L 6380:localhost:6380 you@vps
  redis-cli -p 6380 ping        # → PONG, over the encrypted tunnel
  ```
- ⏳ **stunnel/TLS (upgrade path).** Wrapping 6380 in TLS (via `stunnel`) to
  expose it safely only makes sense *after* `AUTH` lands — TLS encrypts the
  channel but doesn't authenticate the caller. Revisit once auth exists.

## Domain + HTTPS (optional)

Buy a domain (Porkbun / Namecheap / Cloudflare Registrar) and point an **A
record** at the VPS IP. `deploy/caddy/Caddyfile` gives an HTTPS status page via
Caddy's automatic Let's Encrypt certs. Note: Caddy is HTTP-only — it does **not**
proxy the raw Redis TCP port (see the file's header). It's a placeholder for a
future `/metrics` endpoint; skip it entirely if you don't need a web surface.

## Data / backup

State is the single AOF at `/var/lib/mini-redis/appendonly.aof`, persisted on the
host bind-mount so it survives container/VPS restarts. Automated backups are not
wired up yet (`deploy/backup/backup.sh` is still a stub).
