import asyncio
import logging
import os
import uvicorn
import importlib
from pathlib import Path
from fastapi import FastAPI
from typing import Optional, TYPE_CHECKING, Callable

from volnux.app import get_current_app
from volnux.models import NodeHeartbeat

if TYPE_CHECKING:
    from volnux.engine.workflows.trigger import TriggerEngine

logger = logging.getLogger(__name__)


async def _run_server(
    server: "uvicorn.Server",
    trigger_engine: "TriggerEngine",
    stop_event: asyncio.Event,
) -> None:
    """
    Runs the Uvicorn server, initializes the TriggerEngine after the server startup,
    and watches for a stop_event to gracefully terminate the server.

    This asynchronous function orchestrates three tasks:
    - Launch the Uvicorn server.
    - Start the TriggerEngine when the server is up.
    - Monitor the stop_event to signal server shutdown.

    :param server: The Uvicorn server instance that will be managed and launched.
    :type server: uvicorn.Server
    :param trigger_engine: The TriggerEngine instance to be initialized after the server starts.
    :type trigger_engine: TriggerEngine
    :param stop_event: An asyncio.Event used to signal that the server should shut down.
    :type stop_event: asyncio.Event
    :return: None
    """

    server_task = asyncio.create_task(
        server.serve(),
        name="uvicorn-server",
    )

    trigger_task = asyncio.create_task(
        _start_triggers_after_startup(server, trigger_engine),
        name="trigger-engine-start",
    )

    stop_task = asyncio.create_task(
        _watch_stop_event(stop_event, server),
        name="stop-watcher",
    )

    try:
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


async def _watch_stop_event(stop_event: asyncio.Event, server: uvicorn.Server) -> None:
    """
    Waits for the provided stop_event to be set and then signals the Uvicorn server
    to exit gracefully. A graceful shutdown allows Uvicorn to complete any in-flight
    requests before closing, respecting the configured timeout_graceful_shutdown.

    :param stop_event: An asyncio Event used to signal when the server should shut
        down. The event must be set externally to trigger the shutdown process.
    :param server: An instance of the Uvicorn Server class that is being managed.
        The server's `should_exit` attribute will be set to True to initiate
        a graceful shutdown.
    :return: This coroutine does not return anything. It simply manages the shutdown
        signal.
    """
    await stop_event.wait()
    logger.info("Stop event received — signalling Uvicorn to exit")
    server.should_exit = True


async def _start_triggers_after_startup(
    server: "uvicorn.Server", trigger_engine: "TriggerEngine"
) -> None:
    """
    Polls until Uvicorn has started accepting connections, then arms all triggers.
    This ensures that components like WebhookTrigger which register HTTP
    endpoints on the live app do not start before Uvicorn is ready.

    :param server: The Uvicorn server instance to monitor.
    :type server: uvicorn.Server
    :param trigger_engine: The trigger engine responsible for managing triggers.
        This may be None if no engine is being used.
    :type trigger_engine: TriggerEngine
    :return: None
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
            await trigger_engine.start()
            logger.info("TriggerEngine started — all triggers active")
        except Exception as exc:
            logger.error("TriggerEngine failed to start: %s", exc)


async def _run_with_hot_reload(
    server: "uvicorn.Server",
    app_factory: Callable[[], FastAPI],
    trigger_engine: "TriggerEngine",
    stop_event: asyncio.Event,
    project_dir: Path,
    uv_config: "uvicorn.Config",
) -> None:
    """
    Monitors specified Python and Pointy-Lang files under the provided project directory
    for changes and implements a hot reload mechanism. This includes stopping the
    TriggerEngine, signaling Uvicorn to exit, rebuilding the app, and restarting Uvicorn
    upon a detected change. The function also ensures a clean shutdown when triggered
    by an external stop event.

    Detailed behavior:
    - Creates file watchers with a filter to monitor .py and .pty files within specified paths.
    - Begins a Uvicorn server instance and starts the TriggerEngine.
    - Waits for either a file change or an external shutdown signal.
    - Upon file changes, shuts down the TriggerEngine and Uvicorn gracefully, reloads the app,
      and starts new instances of the server and TriggerEngine.
    - Terminates cleanly upon an external stop signal.

    Note that functionality depends on the `watchfiles` library, which must be installed.

    :param server: A Uvicorn server instance responsible for executing the application.
    :type server: uvicorn.Server
    :param app_factory: Callable that generates a new FastAPI application instance on rebuild.
    :type app_factory: Callable[[], FastAPI]
    :param trigger_engine: An instance of the TriggerEngine responsible for managing workflows.
    :type trigger_engine: TriggerEngine
    :param stop_event: An asyncio Event used to signal an external stop request.
    :type stop_event: asyncio.Event
    :param project_dir: Path to the project directory containing monitored files.
    :type project_dir: Path
    :param uv_config: Uvicorn configuration object used to restart the server with updated settings.
    :type uv_config: uvicorn.Config
    :return: None
    :rtype: None
    """

    try:
        from watchfiles import awatch, Change
    except ImportError:
        logger.warning(
            "watchfiles not installed — hot reload disabled. "
            "Install with: pip install watchfiles"
        )
        # Fall back to running without a reload
        await _run_server(
            server=server,
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

    current_server = server

    while not stop_event.is_set():
        # Start a server for this iteration
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
                    await asyncio.wait_for(trigger_engine.stop(), timeout=5.0)
                except Exception:
                    logger.error("Graceful shutdown timed out — forcing stop.")
                    dropped = await trigger_engine.force_stop()
                    if dropped:
                        logger.error(f"{dropped} workflow(s) were lost.")

            # Signal Uvicorn to exit
            current_server.should_exit = True
            try:
                await asyncio.wait_for(server_task, timeout=10.0)
            except asyncio.TimeoutError:
                pass

            # Rebuild the app and create a new server instance
            try:
                import volnux.app as _app_module

                importlib.reload(_app_module)
            except Exception as exc:
                logger.error("Reload failed: %s — keeping current app", exc)

            new_app = get_current_app()
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


def _setup_otel(engine: "TriggerEngine", dev_mode: bool):
    """
    Configure Volnux OTel instrumentation using the existing Volnux OTel layer.

    Delegates entirely to the Volnux OTel infrastructure rather than
    configuring the raw OTel SDK directly. This ensures the span hierarchy,
    attribute naming conventions, and component patching are consistent with
    the rest of the Volnux framework.

    What this does:
        1. Builds VolnuxTracerConfig from environment variables.
           Supports three backend modes:
             - Datadog (VOLNUX_OTEL_BACKEND=datadog)
             - Grafana (VOLNUX_OTEL_BACKEND=grafana, or default)
             - Generic (any OTLP-compatible collector)
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

    Return:
        Callable[[], None]
            Shutdown function. Call during a graceful shutdown to flush pending
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
        # Build VolnuxTracerConfig from environment
        #
        # Backend selection via VOLNUX_OTEL_BACKEND:
        #   "datadog" → routes to Datadog Agent OTLP receiver (port 4317)
        #   "grafana"  → routes directly to Tempo or Grafana Agent
        #   anything else → generic OTLP endpoint (OTel Collector, Jaeger, etc.)
        #
        # Endpoint selection (in priority order):
        #   1. VOLNUX_OTEL_DATADOG_URL (Datadog Agent OTLP gRPC)
        #   2. VOLNUX_OTEL_TEMPO_URL (Tempo / Grafana Agent)
        #   3. VOLNUX_OTEL_ENDPOINT (generic OTLP — OTel Collector default)

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

        # Initialize the global TracerProvider
        tracer = VolnuxTracer.initialize(config)
        logger.info(
            "Volnux OTel tracer initialised: service=%s env=%s",
            service_name,
            environment,
        )

        # Patch Volnux execution components
        # Instruments ExecutionContext, Coordinator, Pipeline, and Signal
        # subsystems so that all workflow spans are emitted automatically
        # with the documented hierarchy:
        #   workflow.* → engine.* → context.* → coordinator.* → flow.*
        patch_all_execution_components()
        patch_all_pipeline_components()
        logger.debug("Volnux execution components patched")

        # Instrument the engine
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

        # Instrument FastAPI
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

        # Return shutdown callable
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


# Prometheus
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


# NodeHeartbeat
async def _start_heartbeat():
    """
    Start the Redis NodeHeartbeat. Returns the heartbeat instance
    so it can be stopped during shutdown.
    """
    try:
        from volnux.adapters.redis_channel import NodeHeartbeat

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


# Graceful shutdown
async def _shutdown(
    trigger_engine: "TriggerEngine",
    heartbeat: "NodeHeartbeat",
    otel_shutdown: Callable[[], None],
    prometheus_shutdown: Callable[[], None],
) -> None:
    """
    Ordered graceful shutdown sequence.

    Order matters:
        1. TriggerEngine stops — no new workflows are fired
        2. NodeHeartbeat stops — node deregisters from Redis
        3. OTel flushes — all pending spans exported
        4. Prometheus closes — scrape endpoint gone

    The checkpoint drain uses VOLNUX_CHECKPOINT_DRAIN_TIMEOUT (default 120s).
    This must be less than the container's terminationGracePeriodSeconds.
    """

    if trigger_engine is not None:
        logger.info("Stopping TriggerEngine...")
        try:
            await asyncio.wait_for(trigger_engine.stop(), timeout=10.0)
            logger.info("TriggerEngine stopped")
        except asyncio.TimeoutError:
            logger.warning("TriggerEngine stop timed out")
        except Exception as exc:
            logger.warning("TriggerEngine stop error: %s", exc)

    if heartbeat is not None:
        try:
            await asyncio.wait_for(heartbeat.stop(), timeout=5.0)
            logger.info("NodeHeartbeat stopped")
        except Exception as exc:
            logger.warning("NodeHeartbeat stop error: %s", exc)

    # otel_shutdown is synchronous — run in executor to not block the loop
    if callable(otel_shutdown):
        try:
            await asyncio.get_event_loop().run_in_executor(None, otel_shutdown)
        except Exception as exc:
            logger.warning("OTel flush error: %s", exc)

    if callable(prometheus_shutdown):
        try:
            await asyncio.get_event_loop().run_in_executor(None, prometheus_shutdown)
        except Exception as exc:
            logger.warning("Prometheus shutdown error: %s", exc)

    logger.info("API server shutdown complete")
