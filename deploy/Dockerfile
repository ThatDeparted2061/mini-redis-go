# syntax=docker/dockerfile:1.6
#
# Multi-stage build: a full Go toolchain compiles the binary, then we throw
# the toolchain away and ship only the static binary on a distroless base.
# Result is a tiny image (~15-20MB) with no shell, no package manager, no OS
# cruft — a smaller attack surface and less to pull.

FROM golang:1.26-alpine AS builder
WORKDIR /build

# Copy manifests first and download deps in their own layer, so `go mod
# download` is only re-run when go.mod/go.sum change, not on every source edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 → a fully static binary (no libc), which distroless/static needs.
# -s -w strips the symbol table and DWARF debug info to shrink the binary.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
    -o /usr/local/bin/mini-redis ./cmd/server

# Create the AOF dir here and hand it to the nonroot uid, so the runtime user
# can write it (a fresh Docker volume inherits the mountpoint's ownership).
RUN mkdir -p /data

FROM gcr.io/distroless/static-debian12
COPY --from=builder /usr/local/bin/mini-redis /usr/local/bin/mini-redis
COPY --from=builder --chown=65532:65532 /data /data

EXPOSE 6380
# 9091 = Prometheus /metrics (default --metrics-addr). Scraped over the compose
# network; not published to the host in deploy/docker-compose.yml.
EXPOSE 9091
# 65532 = the 'nonroot' user baked into distroless. Run unprivileged.
USER nonroot

ENTRYPOINT ["/usr/local/bin/mini-redis"]
# Bind all interfaces (default) so the published port is reachable from outside
# the container; the AOF lives on the /data volume for persistence across
# container restarts. Our server uses --aof-path (there is no --data-dir).
CMD ["--port", "6380", "--appendonly=true", "--appendfsync", "everysec", \
     "--aof-path", "/data/appendonly.aof"]
VOLUME ["/data"]
