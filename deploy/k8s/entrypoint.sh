#!/bin/bash
# docker/entrypoint.sh
#
# Container entrypoint for all Volnux roles.
# The CMD argument determines what this container runs.
#
# Roles:
#   api      REST API + TriggerEngine + RehydrationManager
#   worker   Workflow execution engine (no REST API, no trigger registration)
#   migrate  Run database migrations then exit (init container role)
#   init     Pre-flight configuration validation then exit
#
# Graceful shutdown:
#   On SIGTERM, the engine stops accepting new workflows, waits for
#   in-flight events to reach a safe checkpoint boundary, drains the
#   checkpoint queue, then exits cleanly.
#
#   terminationGracePeriodSeconds in K8s must exceed CHECKPOINT_DRAIN_TIMEOUT.
#   Both are set to 120s / 180s respectively in the K8s manifests.

set -euo pipefail

ROLE="${1:-api}"

# ── Logging helper ────────────────────────────────────────────────────────────
log() {
    if [ "${VOLNUX_LOG_FORMAT:-text}" = "json" ]; then
        echo "{\"timestamp\":\"$(date -u +%FT%TZ)\",\"level\":\"INFO\",\"component\":\"entrypoint\",\"message\":\"$*\"}"
    else
        echo "[$(date -u +%FT%TZ)] [entrypoint] $*"
    fi
}

log_error() {
    if [ "${VOLNUX_LOG_FORMAT:-text}" = "json" ]; then
        echo "{\"timestamp\":\"$(date -u +%FT%TZ)\",\"level\":\"ERROR\",\"component\":\"entrypoint\",\"message\":\"$*\"}"
    else
        echo "[$(date -u +%FT%TZ)] [entrypoint] ERROR: $*" >&2
    fi
}

# ── Node ID ───────────────────────────────────────────────────────────────────
# In Kubernetes, VOLNUX_NODE_ID is injected from metadata.name via downward API.
# In Docker Compose, it falls back to the container hostname.
if [ -z "${VOLNUX_NODE_ID:-}" ]; then
    export VOLNUX_NODE_ID="${HOSTNAME:-$(hostname 2>/dev/null || echo "unknown")}"
fi
log "Starting role=${ROLE} node=${VOLNUX_NODE_ID}"

# ── Dependency readiness checks ───────────────────────────────────────────────

wait_for_postgres() {
    log "Waiting for PostgreSQL at ${VOLNUX_POSTGRES_HOST:-localhost}:${VOLNUX_POSTGRES_PORT:-5432}..."
    python3 - <<'EOF'
import asyncio, os, sys

async def check():
    try:
        import asyncpg
    except ImportError:
        print("asyncpg not installed, skipping PostgreSQL check")
        return

    host = os.environ.get("VOLNUX_POSTGRES_HOST", "localhost")
    port = int(os.environ.get("VOLNUX_POSTGRES_PORT", "5432"))
    db   = os.environ.get("VOLNUX_POSTGRES_DB",   "volnux")
    user = os.environ.get("VOLNUX_POSTGRES_USER",  "volnux")
    pwd  = os.environ.get("VOLNUX_POSTGRES_PASSWORD", "")

    for attempt in range(1, 31):
        try:
            conn = await asyncpg.connect(
                host=host, port=port, database=db, user=user, password=pwd
            )
            await conn.close()
            print(f"PostgreSQL ready (attempt {attempt})")
            return
        except Exception as exc:
            print(f"PostgreSQL not ready ({attempt}/30): {exc}")
            await asyncio.sleep(2)
    sys.exit(1)

asyncio.run(check())
EOF
}

wait_for_redis() {
    local url="${1:-${VOLNUX_REDIS_CHECKPOINT_URL:-redis://localhost:6379/0}}"
    log "Waiting for Redis at ${url}..."
    python3 - <<EOF
import asyncio, sys

async def check():
    try:
        import redis.asyncio as aioredis
    except ImportError:
        print("redis not installed, skipping Redis check")
        return

    for attempt in range(1, 31):
        try:
            client = aioredis.Redis.from_url("${url}", socket_timeout=3)
            await client.ping()
            await client.aclose()
            print(f"Redis ready (attempt {attempt})")
            return
        except Exception as exc:
            print(f"Redis not ready ({attempt}/30): {exc}")
            await asyncio.sleep(2)
    sys.exit(1)

asyncio.run(check())
EOF
}

# ── Signal handlers ───────────────────────────────────────────────────────────
# Forward SIGTERM to the child process to trigger graceful shutdown.
# The child (volnux serve) handles SIGTERM by draining checkpoints.
_CHILD_PID=0
_trap_sigterm() {
    log "SIGTERM received — forwarding to child process ${_CHILD_PID}"
    if [ "${_CHILD_PID}" -gt 0 ]; then
        kill -TERM "${_CHILD_PID}" 2>/dev/null || true
    fi
}
trap _trap_sigterm TERM INT

# ── Role dispatch ─────────────────────────────────────────────────────────────
case "${ROLE}" in

    migrate)
        log "Running database migrations..."
        wait_for_postgres
        exec volnux migrate up
        ;;

    init)
        log "Running pre-flight validation..."
        wait_for_postgres
        wait_for_redis "${VOLNUX_REDIS_CHECKPOINT_URL:-redis://localhost:6379/0}"
        exec volnux validate --all
        ;;

    api)
        wait_for_postgres
        wait_for_redis "${VOLNUX_REDIS_CHECKPOINT_URL:-redis://localhost:6379/0}"
        wait_for_redis "${VOLNUX_REDIS_BROKER_URL:-redis://localhost:6380/0}"
        log "Starting API (node=${VOLNUX_NODE_ID} port=${VOLNUX_API_PORT:-8080})"
        volnux serve \
            --host "${VOLNUX_API_HOST:-0.0.0.0}" \
            --port "${VOLNUX_API_PORT:-8080}" \
            --role api \
            --log-level "${VOLNUX_LOG_LEVEL:-INFO}" &
        _CHILD_PID=$!
        wait "${_CHILD_PID}"
        ;;

    worker)
        wait_for_postgres
        wait_for_redis "${VOLNUX_REDIS_CHECKPOINT_URL:-redis://localhost:6379/0}"
        wait_for_redis "${VOLNUX_REDIS_BROKER_URL:-redis://localhost:6380/0}"
        log "Starting Worker (node=${VOLNUX_NODE_ID})"
        volnux serve \
            --host "${VOLNUX_WORKER_HOST:-0.0.0.0}" \
            --port "${VOLNUX_WORKER_PORT:-9090}" \
            --role worker \
            --log-level "${VOLNUX_LOG_LEVEL:-INFO}" &
        _CHILD_PID=$!
        wait "${_CHILD_PID}"
        ;;

    *)
        log_error "Unknown role: '${ROLE}'. Valid roles: api, worker, migrate, init"
        exit 1
        ;;
esac
