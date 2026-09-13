# syntax=docker/dockerfile:1

# ==============================================================================
# Stage 1: Build static Go binary
# ==============================================================================
FROM golang:1.24-alpine AS builder

# Install build essentials and certs
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree
COPY . .

# Build static binary with CGO disabled and stripped symbols
ARG VERSION=1.0.0
ARG COMMIT=head
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X github.com/AnkitxRot/ForgeFlow/internal/version.Version=${VERSION} -X github.com/AnkitxRot/ForgeFlow/internal/version.Commit=${COMMIT} -X github.com/AnkitxRot/ForgeFlow/internal/version.BuildDate=${BUILD_DATE}" \
    -o /bin/forgeflow ./cmd/forgeflow

# ==============================================================================
# Stage 2: Minimal Runtime Image
# ==============================================================================
FROM alpine:3.21

# Install CA certificates, tzdata, and busybox wget for healthcheck
RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -g 10001 -S forgeflow \
    && adduser -u 10001 -S -G forgeflow -h /home/forgeflow forgeflow \
    && mkdir -p /data \
    && chown -R forgeflow:forgeflow /data /home/forgeflow

# Copy compiled binary from builder stage
COPY --from=builder /bin/forgeflow /usr/local/bin/forgeflow

# Expose HTTP REST API and Prometheus metrics port
EXPOSE 8080

# Persist data directory for SQLite / local WAL storage
VOLUME ["/data"]

# Switch to unprivileged user
USER forgeflow:forgeflow
WORKDIR /home/forgeflow

# Healthcheck targeting the ForgeFlow HTTP /healthz endpoint
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/healthz || exit 1

# Default entrypoint and command
ENTRYPOINT ["forgeflow"]
CMD ["server", "-addr=:8080", "-db-type=sqlite", "-db=/data/forgeflow.db"]
