import asyncio
import logging
import os
import time
import uvicorn
import importlib
from pathlib import Path
from fastapi import FastAPI
from typing import Optional, TYPE_CHECKING, Callable

from volnux.app import get_current_app
from volnux.models import NodeHeartbeat
from .utils import _setup_otel, _start_prometheus

if TYPE_CHECKING:
    from volnux.engine.workflows.trigger import TriggerEngine

logger = logging.getLogger(__name__)


async def start_worker_server(
    engine,
    host: str,
    port: int,
    metrics_port: int,
    log_level: str,
    stop_event: asyncio.Event,
) -> None:
    """
    Start the Volnux Worker process and all associated background services.

    The worker role is fundamentally different from the API role:

    What the worker runs:
        - CheckpointManager      — writes checkpoint snapshots to Redis
        - HealthMonitor          — monitors component health, restarts failures
        - RemoteManager          — gRPC server accepting execution requests
                                   from GRPCExecutor on other nodes
        - NodeHeartbeat          — publishes liveness key to Redis
        - Prometheus + /health   — metrics scrape + liveness/readiness endpoints

    What the worker does NOT run:
        - REST API (FastAPI / Uvicorn) — API pods handle HTTP
        - TriggerEngine               — trigger registration belongs to the API role
        - Web dashboard               — served by the API role

    Why the worker has no Uvicorn:
        Workers are execution nodes. Their inbound interface is gRPC (RemoteManager
        on port 45545). The metrics/health HTTP endpoint is a minimal stdlib server
        on metrics_port — it does not need the full ASGI stack.

    Shutdown order:
        1. RemoteManager stops accepting new gRPC execution requests
        2. CheckpointManager drains — in-flight events write final checkpoints
        3. HealthMonitor stops
        4. NodeHeartbeat stops — deregisters from Redis
        5. OTel flushes — exports pending spans
        6. Prometheus closes

    Parameters
    ----------
    engine:
        Initialised WorkflowEngine. Carries CheckpointManager, HealthMonitor,
        RemoteManager, and all registered event/workflow classes.

    host:
        Network interface for the gRPC RemoteManager. "0.0.0.0" in K8s.

    port:
        Port for the worker health endpoint HTTP server.
        Separate from metrics_port — health is cheap, metrics may be expensive.
        Default: 9090.

    metrics_port:
        Port for the Prometheus /metrics scrape endpoint.
        Also serves /health for the K8s liveness probe.
        Default: 9464.

    log_level:
        Logger level string ("debug", "info", "warning", "error").

    stop_event:
        asyncio.Event set by the signal handler on SIGTERM / SIGINT.
        Drives the graceful shutdown sequence.
    """
    otel_shutdown = _setup_otel(engine, dev_mode=False)

    prometheus_shutdown = _start_prometheus(metrics_port)
    # health_server_shutdown = await _start_worker_health_server(port, engine)

    # The worker is now live. Block here until SIGTERM / SIGINT sets stop_event.
    # All actual event execution happens in the executor pool — this coroutine
    # just keeps the process alive and monitors for shutdown.
    try:
        await _worker_run_until_stop(
            stop_event=stop_event,
            engine=engine,
            health_monitor=health_monitor,
        )
    finally:
        await _worker_shutdown(
            remote_manager=remote_manager,
            checkpoint_manager=checkpoint_manager,
            health_monitor=health_monitor,
            heartbeat=heartbeat,
            health_server_shutdown=health_server_shutdown,
            otel_shutdown=otel_shutdown,
            prometheus_shutdown=prometheus_shutdown,
        )


# ── Worker run loop ────────────────────────────────────────────────────────────


async def _worker_run_until_stop(
    stop_event: asyncio.Event,
    engine,
    health_monitor,
) -> None:
    """
    Keep the worker alive until stop_event is set.

    Runs a lightweight heartbeat loop that:
    - Logs periodic status lines (every 60s at DEBUG level)
    - Surfaces HealthMonitor alerts via the logger
    - Yields to the event loop on every iteration so other tasks can run

    The actual work happens in asyncio Tasks created by the executor pool
    and RemoteManager — this loop is purely a keepalive and status reporter.
    """
    last_log = time.monotonic()

    while not stop_event.is_set():
        await asyncio.sleep(1.0)  # yield — allow executor tasks to run

        now = time.monotonic()
        if now - last_log >= 60.0:
            last_log = now
            _log_worker_status(engine)

    logger.info("Stop event received — beginning worker shutdown sequence")


def _log_worker_status(engine) -> None:
    """Log a one-line worker status summary at DEBUG level."""
    try:
        checkpoint_manager = getattr(engine, "checkpoint_manager", None)
        queue_depth = 0
        if checkpoint_manager and hasattr(checkpoint_manager, "queue_depth"):
            queue_depth = checkpoint_manager.queue_depth

        pool_manager = getattr(engine, "pool_manager", None)
        active_contexts = 0
        if pool_manager and hasattr(pool_manager, "active_context_ids"):
            active_contexts = len(pool_manager.active_context_ids())

        logger.debug(
            "Worker status — active_contexts=%d checkpoint_queue=%d",
            active_contexts,
            queue_depth,
        )
    except Exception as exc:
        logger.debug("Worker status check failed: %s", exc)


# ── Worker-specific NodeHeartbeat ──────────────────────────────────────────────


async def _start_heartbeat_worker():
    """
    Start the Redis NodeHeartbeat for a worker node.
    Identical to the API heartbeat but publishes role=worker in the metadata.
    """
    try:
        from volnux.distribution.channels.redis_channel import NodeHeartbeat

        node_id = os.environ.get("VOLNUX_NODE_ID", "local")
        metadata = {
            "role": "worker",
            "version": os.environ.get("VOLNUX_VERSION", "unknown"),
            "grpc_port": os.environ.get("VOLNUX_GRPC_PORT", "45545"),
        }
        heartbeat = NodeHeartbeat(node_id=node_id, metadata=metadata)
        await heartbeat.start()
        logger.info("NodeHeartbeat (worker) started — node_id=%s", node_id)
        return heartbeat

    except ImportError:
        logger.debug("RedisChannel not available — NodeHeartbeat disabled")
        return None
    except Exception as exc:
        logger.warning("NodeHeartbeat (worker) failed to start: %s", exc)
        return None


# ── Worker graceful shutdown ───────────────────────────────────────────────────


async def _worker_shutdown(
    remote_manager,
    checkpoint_manager,
    health_monitor,
    heartbeat,
    health_server_shutdown,
    otel_shutdown,
    prometheus_shutdown,
) -> None:
    """
    Ordered graceful shutdown for the worker role.

    Shutdown order:
        1. RemoteManager   — stop accepting new gRPC execution requests
        2. Checkpoint drain — in-flight events write final checkpoints
        3. HealthMonitor   — stop component health checks
        4. NodeHeartbeat   — deregister from Redis
        5. Health server   — close health/readiness endpoint
        6. OTel flush      — export all pending spans
        7. Prometheus      — close metrics scrape endpoint

    Why RemoteManager stops first:
        New gRPC requests must be rejected before draining begins. If the
        RemoteManager continues accepting requests during the drain, the
        drain window could extend indefinitely.

    Why NodeHeartbeat stops after the drain:
        The heartbeat TTL (30s) acts as a dead-man switch. Other nodes use
        heartbeat expiry to detect that this node's tasks need recovery. If
        the heartbeat stops before the drain is complete, other nodes may
        start recovering tasks that are still executing on this node — a
        race condition producing duplicate execution.
    """

    # ── 1. Stop RemoteManager ─────────────────────────────────────────────────
    if remote_manager is not None and hasattr(remote_manager, "stop"):
        logger.info("Stopping RemoteManager...")
        try:
            await asyncio.wait_for(remote_manager.stop(), timeout=10.0)
            logger.info("RemoteManager stopped — no new gRPC requests accepted")
        except asyncio.TimeoutError:
            logger.warning("RemoteManager stop timed out after 10s")
        except Exception as exc:
            logger.warning("RemoteManager stop error: %s", exc)

    # ── 2. Drain checkpoint queue ─────────────────────────────────────────────
    drain_timeout = float(os.environ.get("VOLNUX_CHECKPOINT_DRAIN_TIMEOUT", "120"))
    if checkpoint_manager is not None and hasattr(checkpoint_manager, "drain"):
        logger.info("Draining checkpoint queue (timeout=%.0fs)...", drain_timeout)
        try:
            await asyncio.wait_for(checkpoint_manager.drain(), timeout=drain_timeout)
            logger.info("Checkpoint queue drained")
        except asyncio.TimeoutError:
            logger.warning(
                "Checkpoint drain timed out after %.0fs — "
                "some events may resume from an earlier checkpoint on the next run",
                drain_timeout,
            )
        except Exception as exc:
            logger.error("Checkpoint drain error: %s", exc)

    # ── 3. Stop HealthMonitor ─────────────────────────────────────────────────
    if health_monitor is not None and hasattr(health_monitor, "stop"):
        try:
            await asyncio.wait_for(health_monitor.stop(), timeout=5.0)
            logger.info("HealthMonitor stopped")
        except Exception as exc:
            logger.warning("HealthMonitor stop error: %s", exc)

    # ── 4. Stop NodeHeartbeat ─────────────────────────────────────────────────
    # Stopped AFTER drain — see docstring for why
    if heartbeat is not None:
        try:
            await asyncio.wait_for(heartbeat.stop(), timeout=5.0)
            logger.info("NodeHeartbeat stopped — node deregistered from Redis")
        except Exception as exc:
            logger.warning("NodeHeartbeat stop error: %s", exc)

    # ── 5. Stop health HTTP server ────────────────────────────────────────────
    if callable(health_server_shutdown):
        try:
            await asyncio.get_event_loop().run_in_executor(None, health_server_shutdown)
        except Exception as exc:
            logger.warning("Health server shutdown error: %s", exc)

    # ── 6. Flush OTel ────────────────────────────────────────────────────────
    if callable(otel_shutdown):
        try:
            await asyncio.get_event_loop().run_in_executor(None, otel_shutdown)
        except Exception as exc:
            logger.warning("OTel flush error: %s", exc)

    # ── 7. Stop Prometheus ────────────────────────────────────────────────────
    if callable(prometheus_shutdown):
        try:
            await asyncio.get_event_loop().run_in_executor(None, prometheus_shutdown)
        except Exception as exc:
            logger.warning("Prometheus shutdown error: %s", exc)

    logger.info("Worker server shutdown complete")
