# Dockerfile
#
# Multi-stage build for Volnux.
# Stage 1 (builder):  installs all dependencies including build tools.
# Stage 2 (runtime):  copies only installed packages, no build tools.
#
# This keeps the runtime image small (~200MB vs ~800MB with build tools)
# and reduces the attack surface in production.
#
# Usage:
#   Build:  docker build -t your-registry/volnux-engine:2.0.0 .
#   Run:    docker run -e VOLNUX_POSTGRES_PASSWORD=... your-registry/volnux-engine:2.0.0 api

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System build dependencies
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      g++ \
      libpq-dev \
      libffi-dev \
      libssl-dev \
      pkg-config \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install wheel builder
RUN pip install --upgrade pip wheel

# Copy dependency declarations first to exploit layer caching.
# If only application code changes (not pyproject.toml), the
# dependency install layer is not rebuilt.
COPY pyproject.toml README.md ./

# Install all production dependencies into a custom prefix.
# Using --prefix allows us to copy only the installed packages
# into the runtime stage cleanly.
RUN pip install \
      --prefix=/install \
      --no-cache-dir \
      ".[postgres,redis,grpc,celery,kubernetes]"

# Copy application code after dependencies (second cache layer)
COPY volnux/ ./volnux/
COPY workflows/ ./workflows/

# Install the application itself
RUN pip install \
      --prefix=/install \
      --no-cache-dir \
      --no-deps \
      .


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Runtime system dependencies only — no build tools
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libpq5 \
      libssl3 \
      curl \
 && rm -rf /var/lib/apt/lists/* \
 && apt-get clean

# Non-root user for security — matches K8s securityContext runAsUser: 1000
RUN groupadd -r -g 1000 volnux \
 && useradd -r -u 1000 -g volnux -d /opt/volnux -s /sbin/nologin volnux

# Application directory
WORKDIR /opt/volnux

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy application code
COPY --chown=volnux:volnux --from=builder /build/volnux/ ./volnux/
COPY --chown=volnux:volnux --from=builder /build/workflows/ ./workflows/

# Copy entrypoint
COPY --chown=volnux:volnux docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create directories that need to exist at runtime.
# /data — SQLite trigger state, local CLI IPC
# /tmp  — temporary files (readOnlyRootFilesystem in K8s uses emptyDir)
RUN mkdir -p /data /tmp \
 && chown -R volnux:volnux /data /tmp

USER volnux

# ── Environment defaults ──────────────────────────────────────────────────────
# All of these are overridden by ConfigMap/Secrets in Kubernetes,
# or by environment variables in docker-compose.yml for local development.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    VOLNUX_ENV=production \
    VOLNUX_LOG_LEVEL=INFO \
    VOLNUX_LOG_FORMAT=json \
    VOLNUX_API_HOST=0.0.0.0 \
    VOLNUX_API_PORT=8080 \
    VOLNUX_API_WORKERS=1 \
    VOLNUX_PROMETHEUS_PORT=9464 \
    VOLNUX_CHANNEL_BACKEND=redis \
    VOLNUX_CHECKPOINT_DRAIN_TIMEOUT=120 \
    VOLNUX_NODE_ID=""

# ── Ports ─────────────────────────────────────────────────────────────────────
# 8080 — REST API
# 9464 — Prometheus metrics + worker health endpoint
# 45545 — gRPC RemoteManager (worker role only)
EXPOSE 8080 9464 45545

# ── Health check ──────────────────────────────────────────────────────────────
# Used by Docker and docker-compose. K8s uses its own liveness probe.
HEALTHCHECK \
    --interval=15s \
    --timeout=5s \
    --start-period=30s \
    --retries=3 \
    CMD curl -f http://localhost:${VOLNUX_API_PORT}/api/v1/health || exit 1

ENTRYPOINT ["/entrypoint.sh"]

# Default role: API server.
# Override with "worker", "migrate", or "init" as the CMD argument.
CMD ["api"]
