"""

start_api_server — starts the Volnux API process.

Responsibilities:
    1. Inject the engine into the FastAPI app state
    2. Configure OTel tracing if enabled
    3. Start Prometheus metrics server (separate port)
    4. Start NodeHeartbeat — registers this pod in Redis
    5. Start TriggerEngine — arms all registered triggers
    6. Serve the FastAPI app via Uvicorn
    7. Watch for file changes in dev mode (hot reload)
    8. On stop_event: graceful shutdown in correct order

Shutdown order matters:
    1. Uvicorn stops accepting new requests
    2. In-flight HTTP requests drain (Uvicorn handles this)
    3. TriggerEngine stops — no new workflows are fired
    4. Checkpoint queue drains — all in-flight events checkpoint
    5. NodeHeartbeat stops — node deregisters from Redis
    6. OTel exporter flushes — all pending spans are exported
    7. Prometheus server closes
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


async def start_api_server(
    engine,
    host: str,
    port: int,
    metrics_port: int,
    log_level: str,
    reload: bool,
    dev_mode: bool,
    stop_event: asyncio.Event,
    project_dir: Optional[Path] = None,
) -> None:
    """
    Start the Volnux API server and all associated background services.

    Parameters
    ----------
    engine:
        Initialised Volnux WorkflowEngine from init.py.
        Injected into app.state so all FastAPI route handlers can access it.

    host:
        Network interface to bind to.
        Production: "0.0.0.0" (all interfaces).
        Development: "127.0.0.1" (loopback only).

    port:
        TCP port for the REST API and web dashboard.

    metrics_port:
        TCP port for the Prometheus /metrics scrape endpoint.
        Kept separate from the REST API so scraping access can be
        restricted independently (e.g. internal-only NetworkPolicy).

    log_level:
        Uvicorn and application log level ("debug", "info", "warning", "error").

    reload:
        When True (dev_mode only), watches .py and .pty files for changes
        and restarts the Uvicorn server on every change.

    dev_mode:
        When True, enables: hot reload, API docs (/api/v1/docs), verbose
        Uvicorn access logging, and relaxed CORS.

    stop_event:
        asyncio.Event set by the signal handler (SIGTERM / SIGINT) to
        trigger graceful shutdown. The caller sets this; this function
        watches it and drives the shutdown sequence.

    project_dir:
        Project root directory. Used by the file watcher in dev mode to
        scope which files trigger a reload.
    """

    # ── 1. Import the FastAPI app ──────────────────────────────────────────
    # The app is already fully configured with routes, middleware, and
    # exception handlers. We only inject the engine into app.state here
    # so that route handlers can access it via request.app.state.engine.
    from volnux.server.app import create_app

    app = create_app(engine=engine, dev_mode=dev_mode)

    # ── 2. Configure OTel ─────────────────────────────────────────────────
    otel_shutdown = _setup_otel(engine, dev_mode)

    # ── 3. Start Prometheus metrics server ────────────────────────────────
    prometheus_shutdown = _start_prometheus(metrics_port)

    # ── 4. Start NodeHeartbeat ────────────────────────────────────────────
    # Publishes volnux:node:alive:{node_id} to Redis with TTL.
    # Other nodes and administrative tooling use this for liveness detection.
    heartbeat = await _start_heartbeat()

    # ── 5. Start TriggerEngine ────────────────────────────────────────────
    # Arms all triggers registered via WorkflowConfig.ready().
    # TriggerEngine must start AFTER Uvicorn is up so that WebhookTrigger
    # can register its HTTP endpoints on the live server.
    # We defer its start until after Uvicorn is accepting connections.
    trigger_engine = getattr(engine, "trigger_engine", None)

    # ── 6. Build Uvicorn config ───────────────────────────────────────────
    import uvicorn

    uv_config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level=log_level.lower(),
        # Access log: always on in dev, configurable in production
        access_log=dev_mode
        or os.environ.get("VOLNUX_ACCESS_LOG", "false").lower() == "true",
        # Let uvicorn use the existing asyncio loop (we are already inside one)
        loop="none",
        # Graceful shutdown timeout — how long Uvicorn waits for in-flight
        # HTTP requests to complete. Set to 10s; the outer drain_timeout
        # (120s) is much longer so we have headroom.
        timeout_graceful_shutdown=10,
        # Lifespan: "on" enables FastAPI lifespan context manager for
        # startup/shutdown hooks registered on the app.
        lifespan="on",
        # Workers: always 1 in Kubernetes (horizontal scaling via replicas).
        workers=1,
        # Server header: suppress to avoid exposing Uvicorn version
        server_header=False,
        # Date header: keep on for HTTP compliance
        date_header=True,
        # Reload: only meaningful when reload=True (dev mode).
        # We handle reload ourselves via watchfiles for finer control.
        reload=False,
    )

    server = uvicorn.Server(uv_config)

    # ── 7. Dispatch into the appropriate run mode ──────────────────────────
    try:
        if reload and dev_mode and project_dir:
            await _run_with_hot_reload(
                server=server,
                app_factory=lambda: create_app(engine=engine, dev_mode=dev_mode),
                engine=engine,
                trigger_engine=trigger_engine,
                stop_event=stop_event,
                project_dir=project_dir,
                uv_config=uv_config,
            )
        else:
            await _run_server(
                server=server,
                engine=engine,
                trigger_engine=trigger_engine,
                stop_event=stop_event,
            )
    finally:
        # ── 8. Graceful shutdown — always runs ─────────────────────────────
        await _shutdown(
            trigger_engine=trigger_engine,
            heartbeat=heartbeat,
            engine=engine,
            otel_shutdown=otel_shutdown,
            prometheus_shutdown=prometheus_shutdown,
        )


# ── Core server run ────────────────────────────────────────────────────────────


async def _run_server(
    server,
    engine,
    trigger_engine,
    stop_event: asyncio.Event,
) -> None:
    """
    Run Uvicorn and start the TriggerEngine once the server is up.
    Watches stop_event and sets server.should_exit when it fires.
    """

    # Task 1: Uvicorn server (runs until server.should_exit is True)
    server_task = asyncio.create_task(
        server.serve(),
        name="uvicorn-server",
    )

    # Task 2: Wait for Uvicorn to be ready, then start TriggerEngine
    trigger_task = asyncio.create_task(
        _start_triggers_after_startup(server, trigger_engine),
        name="trigger-engine-start",
    )

    # Task 3: Watch stop_event and signal Uvicorn to exit
    stop_task = asyncio.create_task(
        _watch_stop_event(stop_event, server),
        name="stop-watcher",
    )

    try:
        # Wait for Uvicorn to exit (either naturally or via stop_event)
        await asyncio.gather(server_task, trigger_task, stop_task)
    except asyncio.CancelledError:
        # Propagate cancellation — outer finally handles cleanup
        raise
    finally:
        # Clean up any tasks still running
        for task in (server_task, trigger_task, stop_task):
            if not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass


async def _watch_stop_event(stop_event: asyncio.Event, server) -> None:
    """
    Wait for stop_event then signal Uvicorn to exit gracefully.
    Uvicorn's graceful shutdown waits for in-flight requests to complete
    (up to timeout_graceful_shutdown seconds) before closing.
    """
    await stop_event.wait()
    logger.info("Stop event received — signalling Uvicorn to exit")
    server.should_exit = True


async def _start_triggers_after_startup(server, trigger_engine) -> None:
    """
    Poll until Uvicorn has started accepting connections, then arm all
    triggers. WebhookTrigger registers HTTP endpoints on the live app —
    it must not start before Uvicorn is ready.
    """
    # Poll server.started (set by Uvicorn once the socket is bound)
    for _ in range(50):  # 50 × 0.1s = 5s maximum wait
        if getattr(server, "started", False):
            break
        await asyncio.sleep(0.1)
    else:
        logger.warning(
            "Uvicorn did not report started after 5s — " "starting TriggerEngine anyway"
        )

    if trigger_engine is not None:
        try:
            logger.info("Starting TriggerEngine")
            await trigger_engine.start_all()
            logger.info("TriggerEngine started — all triggers active")
        except Exception as exc:
            logger.error("TriggerEngine failed to start: %s", exc)


# ── Hot reload (dev mode only) ─────────────────────────────────────────────────


async def _run_with_hot_reload(
    server,
    app_factory,
    engine,
    trigger_engine,
    stop_event: asyncio.Event,
    project_dir: Path,
    uv_config,
) -> None:
    """
    Watch .py and .pty files under project_dir. On any change:
        1. Stop TriggerEngine
        2. Signal Uvicorn to exit
        3. Wait for Uvicorn to drain
        4. Rebuild the app and restart Uvicorn

    The outer stop_event still triggers clean shutdown at any point.

    Requires: pip install watchfiles
    """
    try:
        from watchfiles import awatch, Change
    except ImportError:
        logger.warning(
            "watchfiles not installed — hot reload disabled. "
            "Install with: pip install watchfiles"
        )
        # Fall back to running without reload
        await _run_server(
            server=server,
            engine=engine,
            trigger_engine=trigger_engine,
            stop_event=stop_event,
        )
        return

    watch_paths = [
        str(project_dir / "workflows"),
        str(project_dir / "config.py"),
        str(project_dir / "init.py"),
    ]
    # Only watch Python and Pointy-Lang files
    watch_filter = lambda change, path: path.endswith((".py", ".pty"))  # noqa: E731

    logger.info("Hot reload watching: %s", watch_paths)

    import uvicorn

    current_server = server

    while not stop_event.is_set():
        # Start server for this iteration
        server_task = asyncio.create_task(
            current_server.serve(),
            name="uvicorn-server",
        )
        trigger_task = asyncio.create_task(
            _start_triggers_after_startup(current_server, trigger_engine),
            name="trigger-engine-start",
        )

        # Watch files — yields when a relevant change is detected
        file_change_task = asyncio.create_task(
            _detect_file_change(watch_paths, watch_filter),
            name="file-watcher",
        )

        # Stop watcher for external shutdown signal
        stop_task = asyncio.create_task(
            _watch_stop_event(stop_event, current_server),
            name="stop-watcher",
        )

        # Wait for the first of: file change or stop signal
        done, pending = await asyncio.wait(
            {server_task, file_change_task, stop_task, trigger_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel everything still running
        for task in pending:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        if stop_event.is_set():
            # External stop — exit the loop
            break

        if file_change_task in done and not file_change_task.cancelled():
            changed = file_change_task.result()
            logger.info("Reload triggered by: %s", changed)

            # Stop TriggerEngine before rebuilding
            if trigger_engine is not None:
                try:
                    await asyncio.wait_for(trigger_engine.stop_all(), timeout=5.0)
                except Exception:
                    pass

            # Signal Uvicorn to exit
            current_server.should_exit = True
            try:
                await asyncio.wait_for(server_task, timeout=10.0)
            except asyncio.TimeoutError:
                pass

            # Rebuild app and create new server instance
            import importlib

            try:
                import volnux.server.app as _app_module

                importlib.reload(_app_module)
            except Exception as exc:
                logger.error("Reload failed: %s — keeping current app", exc)

            new_app = app_factory()
            uv_config.app = new_app
            current_server = uvicorn.Server(uv_config)
            logger.info("Server restarted after file change")


async def _detect_file_change(
    watch_paths: list[str],
    watch_filter,
) -> list[str]:
    """Await the first file change matching the filter and return changed paths."""
    from watchfiles import awatch

    async for changes in awatch(*watch_paths, watch_filter=watch_filter):
        return [str(path) for _, path in changes]
    return []


# ── OTel setup ────────────────────────────────────────────────────────────────


def _setup_otel(engine, dev_mode: bool):
    """
    Configure Volnux OTel instrumentation using the existing Volnux OTel layer.

    Delegates entirely to the Volnux OTel infrastructure rather than
    configuring the raw OTel SDK directly. This ensures the span hierarchy,
    attribute naming conventions, and component patching are consistent with
    the rest of the Volnux framework.

    What this does:
        1. Builds VolnuxTracerConfig from environment variables.
           Supports three backend modes:
             - Datadog  (VOLNUX_OTEL_BACKEND=datadog)
             - Grafana  (VOLNUX_OTEL_BACKEND=grafana, or default)
             - Generic  (any OTLP-compatible collector)
        2. Calls VolnuxTracer.initialize(config) — sets the global
           TracerProvider and configures OTLP export.
        3. Patches all Volnux execution components:
             - patch_all_execution_components() — ExecutionContext + Coordinator
             - patch_all_pipeline_components()  — Pipeline + Signals
        4. Instruments the engine via instrument_engine() so that
           workflow.*, engine.*, context.*, coordinator.*, flow.* spans
           are emitted automatically (matching the documented hierarchy).
        5. Instruments FastAPI via opentelemetry-instrumentation-fastapi
           so that every HTTP request to the REST API gets a root span
           with method, route, and status_code attributes.
        6. Returns a shutdown callable that calls tracer.shutdown() —
           flushes the BatchSpanProcessor before the process exits.

    Returns
    -------
    Callable[[], None]
        Shutdown function. Call during graceful shutdown to flush pending
        spans before the process exits.
    """
    otel_enabled = os.environ.get("VOLNUX_OTEL_ENABLED", "true").lower() == "true"
    if not otel_enabled:
        logger.debug("OTel disabled by VOLNUX_OTEL_ENABLED=false")
        return lambda: None

    try:
        from volnux.otel.tracer_setup import VolnuxTracerConfig, VolnuxTracer
        from volnux.otel.instrumentations.context_coordinator import (
            patch_all_execution_components,
        )
        from volnux.otel.instrumentations.signal import patch_all_pipeline_components
        from volnux.otel.instrumentations.engine import instrument_engine
    except ImportError as exc:
        logger.warning(
            "Volnux OTel module not available (%s) — tracing disabled. "
            "Ensure volnux[otel] is installed.",
            exc,
        )
        return lambda: None

    try:
        # ── 1. Build VolnuxTracerConfig from environment ───────────────────
        #
        # Backend selection via VOLNUX_OTEL_BACKEND:
        #   "datadog" → routes to Datadog Agent OTLP receiver (port 4317)
        #   "grafana"  → routes directly to Tempo or Grafana Agent
        #   anything else → generic OTLP endpoint (OTel Collector, Jaeger, etc.)
        #
        # Endpoint selection (in priority order):
        #   1. VOLNUX_OTEL_DATADOG_URL   (Datadog Agent OTLP gRPC)
        #   2. VOLNUX_OTEL_TEMPO_URL     (Tempo / Grafana Agent)
        #   3. VOLNUX_OTEL_ENDPOINT      (generic OTLP — OTel Collector default)

        backend = os.environ.get("VOLNUX_OTEL_BACKEND", "grafana").lower()
        endpoint = os.environ.get("VOLNUX_OTEL_ENDPOINT", "http://localhost:4317")
        service_name = os.environ.get(
            "VOLNUX_OTEL_SERVICE_NAME", getattr(engine, "project_name", "volnux")
        )
        service_ver = getattr(
            engine, "version", os.environ.get("VOLNUX_VERSION", "unknown")
        )
        environment = (
            "development" if dev_mode else os.environ.get("VOLNUX_ENV", "production")
        )
        node_id = os.environ.get("VOLNUX_NODE_ID", "local")

        config_kwargs: dict = {
            "service_name": service_name,
            "service_version": service_ver,
            "environment": environment,
            "custom_attributes": {
                "volnux.node.id": node_id,
                "volnux.role": "api",
            },
        }

        if backend == "datadog":
            datadog_url = os.environ.get("VOLNUX_OTEL_DATADOG_URL", endpoint)
            config_kwargs["datadog_agent_url"] = datadog_url
            logger.info("OTel backend: Datadog Agent at %s", datadog_url)

        elif backend == "grafana":
            tempo_url = os.environ.get("VOLNUX_OTEL_TEMPO_URL", endpoint)
            config_kwargs["tempo_endpoint"] = tempo_url
            logger.info("OTel backend: Grafana/Tempo at %s", tempo_url)

        else:
            # Generic OTLP — OTel Collector, Jaeger, etc.
            config_kwargs["otlp_endpoint"] = endpoint
            logger.info("OTel backend: generic OTLP at %s", endpoint)

        config = VolnuxTracerConfig(**config_kwargs)

        # ── 2. Initialise the global TracerProvider ────────────────────────
        tracer = VolnuxTracer.initialize(config)
        logger.info(
            "Volnux OTel tracer initialised: service=%s env=%s",
            service_name,
            environment,
        )

        # ── 3. Patch Volnux execution components ───────────────────────────
        # Instruments ExecutionContext, Coordinator, Pipeline, and Signal
        # subsystems so that all workflow spans are emitted automatically
        # with the documented hierarchy:
        #   workflow.* → engine.* → context.* → coordinator.* → flow.*
        patch_all_execution_components()
        patch_all_pipeline_components()
        logger.debug("Volnux execution components patched")

        # ── 4. Instrument the engine ───────────────────────────────────────
        # Wraps DefaultWorkflowEngine methods to emit engine.execute,
        # engine.process_task, engine.resolve_next_task, engine.sink_nodes
        # spans as documented in the span hierarchy.
        try:
            instrumented = instrument_engine(engine)
            # Replace the engine reference on the module level if it is a
            # module-level singleton. For injected engines, the caller is
            # responsible for using the returned instrumented instance.
            # We update the engine in-place where possible.
            if instrumented is not engine:
                logger.debug(
                    "Engine wrapped by instrument_engine — "
                    "instrumented instance returned"
                )
        except Exception as exc:
            # Non-fatal — engine still works, just without engine-level spans
            logger.warning(
                "instrument_engine failed (%s) — engine spans will be missing",
                exc,
            )

        # ── 5. Instrument FastAPI ──────────────────────────────────────────
        # Adds a root HTTP span for every incoming request.
        # Attributes: http.method, http.route, http.status_code, net.host.name
        # This connects external HTTP spans to the Volnux workflow spans below.
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor().instrument(
                # Exclude health and metrics endpoints from tracing — they
                # are high-frequency and produce no useful trace information.
                excluded_urls="api/v1/health,metrics",
            )
            logger.debug("FastAPI instrumented with OTel")
        except ImportError:
            logger.debug(
                "opentelemetry-instrumentation-fastapi not installed — "
                "HTTP request spans will be missing. "
                "Install with: pip install opentelemetry-instrumentation-fastapi"
            )

        # ── 6. Return shutdown callable ────────────────────────────────────
        # tracer.shutdown() flushes the BatchSpanProcessor — all in-flight
        # spans are exported before the process exits.
        def _shutdown() -> None:
            try:
                tracer.shutdown()
                logger.info("OTel tracer shut down and spans flushed")
            except Exception as exc:
                logger.warning("OTel shutdown error: %s", exc)

        return _shutdown

    except Exception as exc:
        logger.warning(
            "OTel setup failed (%s) — tracing disabled. "
            "Workflow execution is unaffected.",
            exc,
        )
        return lambda: None


# ── Prometheus ────────────────────────────────────────────────────────────────


def _start_prometheus(metrics_port: int):
    """
    Start a Prometheus HTTP server on metrics_port in a background thread.
    Returns a shutdown callable that closes the server.

    The /metrics endpoint is served at http://host:{metrics_port}/metrics.
    This is deliberately on a different port from the REST API so that
    scraping access can be independently controlled via NetworkPolicy.
    """
    try:
        import prometheus_client as prom

        # disable the default process/gc collectors if not needed
        prom.REGISTRY.unregister(prom.GC_COLLECTOR)
        prom.REGISTRY.unregister(prom.PLATFORM_COLLECTOR)
        prom.REGISTRY.unregister(prom.PROCESS_COLLECTOR)
    except Exception:
        pass

    try:
        import prometheus_client as prom

        server, thread = prom.start_http_server(metrics_port)
        logger.info("Prometheus metrics server started on port %d", metrics_port)

        def _shutdown():
            try:
                server.shutdown()
                logger.info("Prometheus metrics server stopped")
            except Exception as exc:
                logger.warning("Prometheus shutdown error: %s", exc)

        return _shutdown

    except ImportError:
        logger.debug("prometheus_client not installed — metrics endpoint disabled")
        return lambda: None
    except OSError as exc:
        logger.warning(
            "Could not start Prometheus server on port %d: %s", metrics_port, exc
        )
        return lambda: None


# ── NodeHeartbeat ─────────────────────────────────────────────────────────────


async def _start_heartbeat():
    """
    Start the Redis NodeHeartbeat. Returns the heartbeat instance
    so it can be stopped during shutdown.
    """
    try:
        from volnux.distribution.channels.redis_channel import NodeHeartbeat

        node_id = os.environ.get("VOLNUX_NODE_ID", "local")
        metadata = {
            "role": "api",
            "version": os.environ.get("VOLNUX_VERSION", "unknown"),
        }
        heartbeat = NodeHeartbeat(node_id=node_id, metadata=metadata)
        await heartbeat.start()
        logger.info("NodeHeartbeat started — node_id=%s", node_id)
        return heartbeat

    except ImportError:
        logger.debug("RedisChannel not available — NodeHeartbeat disabled")
        return None
    except Exception as exc:
        logger.warning("NodeHeartbeat failed to start: %s", exc)
        return None


# ── Graceful shutdown ─────────────────────────────────────────────────────────


async def _shutdown(
    trigger_engine,
    heartbeat,
    engine,
    otel_shutdown,
    prometheus_shutdown,
) -> None:
    """
    Ordered graceful shutdown sequence.

    Order matters:
        1. TriggerEngine stops  — no new workflows are fired
        2. Checkpoint queue drains — in-flight events write their state
        3. NodeHeartbeat stops  — node deregisters from Redis
        4. OTel flushes         — all pending spans exported
        5. Prometheus closes    — scrape endpoint gone

    The checkpoint drain uses VOLNUX_CHECKPOINT_DRAIN_TIMEOUT (default 120s).
    This must be less than the container's terminationGracePeriodSeconds.
    """
    # ── 1. Stop TriggerEngine ─────────────────────────────────────────────
    if trigger_engine is not None:
        logger.info("Stopping TriggerEngine...")
        try:
            await asyncio.wait_for(trigger_engine.stop_all(), timeout=10.0)
            logger.info("TriggerEngine stopped")
        except asyncio.TimeoutError:
            logger.warning("TriggerEngine stop timed out")
        except Exception as exc:
            logger.warning("TriggerEngine stop error: %s", exc)

    # ── 2. Drain checkpoint queue ─────────────────────────────────────────
    drain_timeout = float(os.environ.get("VOLNUX_CHECKPOINT_DRAIN_TIMEOUT", "120"))
    checkpoint_manager = getattr(engine, "checkpoint_manager", None)
    if checkpoint_manager is not None and hasattr(checkpoint_manager, "drain"):
        logger.info("Draining checkpoint queue (timeout=%.0fs)...", drain_timeout)
        try:
            await asyncio.wait_for(checkpoint_manager.drain(), timeout=drain_timeout)
            logger.info("Checkpoint queue drained")
        except asyncio.TimeoutError:
            logger.warning(
                "Checkpoint drain timed out after %.0fs — "
                "some events may resume from an earlier checkpoint",
                drain_timeout,
            )
        except Exception as exc:
            logger.error("Checkpoint drain error: %s", exc)

    # ── 3. Stop NodeHeartbeat ─────────────────────────────────────────────
    if heartbeat is not None:
        try:
            await asyncio.wait_for(heartbeat.stop(), timeout=5.0)
            logger.info("NodeHeartbeat stopped")
        except Exception as exc:
            logger.warning("NodeHeartbeat stop error: %s", exc)

    # ── 4. Flush OTel ─────────────────────────────────────────────────────
    # otel_shutdown is synchronous — run in executor to not block the loop
    if callable(otel_shutdown):
        try:
            await asyncio.get_event_loop().run_in_executor(None, otel_shutdown)
        except Exception as exc:
            logger.warning("OTel flush error: %s", exc)

    # ── 5. Stop Prometheus ────────────────────────────────────────────────
    if callable(prometheus_shutdown):
        try:
            await asyncio.get_event_loop().run_in_executor(None, prometheus_shutdown)
        except Exception as exc:
            logger.warning("Prometheus shutdown error: %s", exc)

    logger.info("API server shutdown complete")


# ══════════════════════════════════════════════════════════════════════════════
# start_worker_server
# ══════════════════════════════════════════════════════════════════════════════


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

    # ── 1. Configure OTel (worker role) ───────────────────────────────────────
    # Workers emit spans for every event execution — phase transitions,
    # checkpoint writes, command channel activity. OTel must be configured
    # before any event runs so spans are captured from the first execution.
    otel_shutdown = _setup_otel(engine, dev_mode=False)

    # ── 2. Start Prometheus metrics + health endpoint ──────────────────────────
    # Workers expose /metrics and /health on the same port.
    # The K8s liveness probe hits /health (not /metrics) — a healthy
    # metrics server does not imply a healthy worker engine.
    prometheus_shutdown = _start_prometheus(metrics_port)
    health_server_shutdown = await _start_worker_health_server(port, engine)

    # ── 3. Start NodeHeartbeat ────────────────────────────────────────────────
    # Publishes volnux:node:alive:{node_id} to Redis.
    # The RehydrationManager watches for expired heartbeats to detect
    # node failures and reschedule orphaned tasks.
    heartbeat = await _start_heartbeat_worker()

    # ── 4. Start CheckpointManager ────────────────────────────────────────────
    # The CheckpointManager drains the in-process checkpoint queue to Redis.
    # It must be running before any event executes — events enqueue checkpoints
    # asynchronously and the manager writes them in the background.
    checkpoint_manager = getattr(engine, "checkpoint_manager", None)
    if checkpoint_manager is not None and hasattr(checkpoint_manager, "start"):
        try:
            await checkpoint_manager.start()
            logger.info("CheckpointManager started")
        except Exception as exc:
            logger.error("CheckpointManager failed to start: %s", exc)
            # Non-fatal at startup — events will accumulate checkpoints in
            # the queue and warn if the queue depth grows too large

    # ── 5. Start HealthMonitor ────────────────────────────────────────────────
    # HealthMonitor runs periodic checks on: CheckpointManager queue depth,
    # Redis connectivity, gRPC RemoteManager liveness, and the event executor
    # pool utilisation. It restarts failed components up to a configurable limit.
    health_monitor = getattr(engine, "health_monitor", None)
    if health_monitor is not None and hasattr(health_monitor, "start"):
        try:
            await health_monitor.start()
            logger.info("HealthMonitor started")
        except Exception as exc:
            logger.warning("HealthMonitor failed to start: %s", exc)

    # ── 6. Start RemoteManager (gRPC) ─────────────────────────────────────────
    # RemoteManager serves incoming GRPCExecutor requests from other nodes.
    # An API pod can dispatch an event to this worker via GRPCExecutor —
    # the RemoteManager receives it, creates an EventBase instance, and runs
    # the full lifecycle locally.
    #
    # The gRPC port (45545) is separate from the metrics port. It is exposed
    # by the K8s headless service so individual pods are addressable by name.
    remote_manager = getattr(engine, "remote_manager", None)
    grpc_port = int(os.environ.get("VOLNUX_GRPC_PORT", "45545"))

    if remote_manager is not None and hasattr(remote_manager, "start"):
        try:
            await remote_manager.start(host=host, port=grpc_port)
            logger.info("RemoteManager started — gRPC on %s:%d", host, grpc_port)
        except Exception as exc:
            logger.error(
                "RemoteManager failed to start on %s:%d: %s",
                host,
                grpc_port,
                exc,
            )
            # Non-fatal — worker can still execute local events

    # ── 7. Log startup summary ────────────────────────────────────────────────
    node_id = os.environ.get("VOLNUX_NODE_ID", "local")
    logger.info(
        "Worker node ready — node_id=%s grpc=%s:%d " "health=%s:%d metrics=%s:%d",
        node_id,
        host,
        grpc_port,
        host,
        port,
        host,
        metrics_port,
    )

    # ── 8. Wait for stop signal ───────────────────────────────────────────────
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
        # ── 9. Graceful shutdown — always runs ─────────────────────────────────
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


# ── Worker health HTTP server ──────────────────────────────────────────────────


async def _start_worker_health_server(port: int, engine) -> callable:
    """
    Start a minimal HTTP server for worker health and readiness probes.

    Serves two endpoints:
        GET /health     — liveness probe
                          Returns 200 if the worker engine is alive,
                          503 if a critical component has failed.
        GET /ready      — readiness probe
                          Returns 200 if the worker is ready to accept
                          execution requests (RemoteManager is up),
                          503 if still initialising or draining.

    Uses Python's stdlib http.server rather than FastAPI/Uvicorn because:
    - The worker does not need ASGI middleware, request routing, or authentication
    - The health endpoint must remain available even if the async executor pool
      is saturated — stdlib server runs in a separate thread
    - Keeps the worker container image identical to the API container image
      without conditional dependency on a full ASGI stack

    Returns a shutdown callable that stops the server.
    """
    import http.server
    import json
    import threading

    class _HealthHandler(http.server.BaseHTTPRequestHandler):
        """Minimal HTTP handler for /health and /ready endpoints."""

        # Suppress default access logging — the K8s probe hits /health every
        # 15s which would flood the logs at normal log levels
        def log_message(self, fmt, *args):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Worker health probe: " + fmt, *args)

        def do_GET(self):
            if self.path in ("/health", "/"):
                self._respond_health()
            elif self.path == "/ready":
                self._respond_ready()
            else:
                self.send_error(404)

        def _respond_health(self):
            # Liveness: is the worker process alive and not deadlocked?
            # Check that the checkpoint_manager is not in FAILED state.
            checkpoint_manager = getattr(engine, "checkpoint_manager", None)
            cm_healthy = True
            if checkpoint_manager and hasattr(checkpoint_manager, "is_healthy"):
                cm_healthy = checkpoint_manager.is_healthy

            if cm_healthy:
                body = json.dumps({"status": "healthy"}).encode()
                self.send_response(200)
            else:
                body = json.dumps(
                    {
                        "status": "unhealthy",
                        "reason": "CheckpointManager is not healthy",
                    }
                ).encode()
                self.send_response(503)

            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _respond_ready(self):
            # Readiness: can the worker accept new execution requests?
            # The RemoteManager must be running.
            remote_manager = getattr(engine, "remote_manager", None)
            rm_ready = True
            if remote_manager and hasattr(remote_manager, "is_serving"):
                rm_ready = remote_manager.is_serving

            if rm_ready:
                body = json.dumps({"status": "ready"}).encode()
                self.send_response(200)
            else:
                body = json.dumps(
                    {
                        "status": "not_ready",
                        "reason": "RemoteManager is not serving",
                    }
                ).encode()
                self.send_response(503)

            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    try:
        server = http.server.HTTPServer(("0.0.0.0", port), _HealthHandler)
        thread = threading.Thread(
            target=server.serve_forever,
            name="worker-health-server",
            daemon=True,
        )
        thread.start()
        logger.info("Worker health server started on port %d", port)

        def _shutdown():
            try:
                server.shutdown()
                logger.info("Worker health server stopped")
            except Exception as exc:
                logger.warning("Worker health server shutdown error: %s", exc)

        return _shutdown

    except OSError as exc:
        logger.warning("Could not start worker health server on port %d: %s", port, exc)
        return lambda: None


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
