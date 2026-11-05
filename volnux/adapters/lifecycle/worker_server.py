import asyncio
import logging
import time

from volnux.config import setting
from .utils import _setup_otel, _start_prometheus

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
        - CheckpointManager      — writes checkpoint snapshots (configured by
                                   CHECKPOINT_CONFIG, stored in the 'checkpoints'
                                   entry of DATABASES)
        - HealthMonitor          — monitors the checkpoint manager, restarts its
                                   tasks if they die
        - Prometheus             — the metrics scrape endpoint
        - Telemetry              — OpenTelemetry, started by initialise_workflows

    What the worker does NOT run:
        - REST API (FastAPI / Uvicorn) — API pods handle HTTP
        - TriggerEngine               — trigger registration belongs to the API role
        - Web dashboard               — served by the API role

    Why the worker has no Uvicorn:
        Workers are execution nodes. Their inbound interface is gRPC (RemoteManager
        on port 45545). The metrics/health HTTP endpoint is a minimal stdlib server
        on metrics_port — it does not need the full ASGI stack.

    Shutdown order:
        1. CheckpointManager drains — in-flight events write final checkpoints
        2. HealthMonitor stops
        3. CheckpointManager stops
        4. OTel flushes — exports pending spans
        5. Prometheus closes

    Parameters
    ----------
    engine:
        The initialised engine (from ``initialise_workflows``).

    host, port:
        Accepted for the CLI (``--worker-port``); the worker has no inbound
        interface yet, so they are not used.

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
    otel_shutdown = _setup_otel(engine, dev_mode=False, role="worker")
    prometheus_shutdown = _start_prometheus(metrics_port)

    checkpoint_manager = None
    health_monitor = None
    try:
        checkpoint_manager = await _start_checkpoint_manager(engine)
        health_monitor = await _start_health_monitor(checkpoint_manager)

        # The worker is now live. Block here until SIGTERM / SIGINT sets stop_event.
        # All actual event execution happens in the executor pool — this coroutine
        # just keeps the process alive and monitors for shutdown.
        await _worker_run_until_stop(
            stop_event=stop_event,
            engine=engine,
            health_monitor=health_monitor,
        )
    finally:
        await _worker_shutdown(
            checkpoint_manager=checkpoint_manager,
            health_monitor=health_monitor,
            otel_shutdown=otel_shutdown,
            prometheus_shutdown=prometheus_shutdown,
        )


# ── Worker components ──────────────────────────────────────────────────────────


async def _start_checkpoint_manager(engine):
    """
    Build the checkpoint manager from the ``CHECKPOINT_CONFIG`` setting and start it.

    The manager is also set as ``engine.checkpoint_manager`` so that code that looks
    for it there (the CLI's drain on exit) finds the one that is running.
    """
    from volnux.engine.checkpoint_config import CheckPointConfig
    from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager

    manager = VolnuxCheckPointManager.from_config(CheckPointConfig.from_settings())
    await manager.start()
    engine.checkpoint_manager = manager
    logger.info("CheckpointManager started")
    return manager


async def _start_health_monitor(checkpoint_manager):
    """Supervise the checkpoint manager: its tasks are restarted if they die."""
    from volnux.execution.resilience.health_check import HealthMonitor

    monitor = HealthMonitor()
    monitor.register(checkpoint_manager, "checkpoint")
    await monitor.start()
    return monitor


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


# ── Worker graceful shutdown ───────────────────────────────────────────────────


async def _worker_shutdown(
    checkpoint_manager,
    health_monitor,
    otel_shutdown,
    prometheus_shutdown,
) -> None:
    """
    Ordered graceful shutdown for the worker role.

    Shutdown order:
        1. Checkpoint drain  — in-flight events write their final checkpoints
        2. HealthMonitor     — stop supervising (before the manager it restarts)
        3. CheckpointManager — stop its tasks
        4. OTel flush        — export all pending spans
        5. Prometheus        — close the metrics scrape endpoint

    Each step is attempted even if an earlier one failed. ``None`` for a component
    that never started.
    """

    # ── 1. Drain checkpoint queue ─────────────────────────────────────────────
    drain_timeout = float(setting("CHECKPOINT_DRAIN_TIMEOUT", 120.0))
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

    # ── 2. Stop HealthMonitor ─────────────────────────────────────────────────
    if health_monitor is not None and hasattr(health_monitor, "stop"):
        try:
            await asyncio.wait_for(health_monitor.stop(), timeout=5.0)
            logger.info("HealthMonitor stopped")
        except Exception as exc:
            logger.warning("HealthMonitor stop error: %s", exc)

    # ── 3. Stop CheckpointManager ─────────────────────────────────────────────
    if checkpoint_manager is not None and hasattr(checkpoint_manager, "stop"):
        try:
            await asyncio.wait_for(checkpoint_manager.stop(), timeout=10.0)
            logger.info("CheckpointManager stopped")
        except Exception as exc:
            logger.warning("CheckpointManager stop error: %s", exc)

    # ── 4. Flush OTel ────────────────────────────────────────────────────────
    if callable(otel_shutdown):
        try:
            await asyncio.get_running_loop().run_in_executor(None, otel_shutdown)
        except Exception as exc:
            logger.warning("OTel flush error: %s", exc)

    # ── 5. Stop Prometheus ────────────────────────────────────────────────────
    if callable(prometheus_shutdown):
        try:
            await asyncio.get_running_loop().run_in_executor(None, prometheus_shutdown)
        except Exception as exc:
            logger.warning("Prometheus shutdown error: %s", exc)

    logger.info("Worker server shutdown complete")
