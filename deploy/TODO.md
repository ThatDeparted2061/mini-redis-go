# Deploy TODO — manual steps (accounts + secrets)

Code/docs are done (`backup.sh`, `RUNBOOK.md`, README badges). These need real
accounts/credentials and a live VPS, so they're deferred. See `RUNBOOK.md` for
the full context on each.

## Backups (Backblaze B2 + rclone)
- [ ] Sign up Backblaze B2, create bucket `mini-redis-backups`, make an app key (keyID + appKey).
- [ ] On the VPS: `sudo apt-get install rclone && sudo rclone config` → remote named **`b2`**, type `b2`, paste keys (as root — cron.daily runs as root).
- [ ] `sudo cp deploy/backup/backup.sh /etc/cron.daily/backup-mini-redis && sudo chmod +x /etc/cron.daily/backup-mini-redis`
- [ ] Smoke-test: `sudo /etc/cron.daily/backup-mini-redis` (don't wait for cron).

## Uptime monitoring (UptimeRobot)
- [ ] Add a **TCP** monitor for host:6380, 5-min interval. (Not internet-reachable yet — revisit once AUTH/TLS or a `/metrics` HTTP endpoint lands.)
- [ ] Add alert contacts: email + Telegram.
- [ ] Create a public status page; paste its monitor id + slug into the two README badge URLs (replace `mXXXXXXXXX-YYYY` and `YOUR_PAGE`).

## VPS (prereq for both)
- [ ] Provision + harden a VPS (SSH keys-only, `ufw`, `fail2ban`, unattended-upgrades) — see RUNBOOK §1.
- [ ] Push the image and deploy the systemd unit.

## Nice-to-have
- [ ] `deploy/backup/rclone.conf.example` + `.gitignore` guard so real creds never get committed.
