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

__all__ = ("start_api_server",)


async def start_api_server(
    engine: "TriggerEngine",
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

    This function initializes and starts the FastAPI application, configures
    OpenTelemetry (OTel) for distributed tracing, launches the Prometheus
    metrics server, starts the NodeHeartbeat service for Redis-based
    liveness checks, and activates the TriggerEngine for registered triggers.
    It also configures the Uvicorn server for handling HTTP requests, with
    options for development features like hot reload and detailed logging.
    Graceful shutdown is ensured by reacting to a stop event triggered by signal
    handlers.

    Parameters
    ----------
    engine: TriggerEngine
        Injected Volnux TriggerEngine instance, used as a core component for
        executing workflows.

    host : str
        Network interface address to bind the server. Common values include
        "0.0.0.0" to listen on all interfaces, or "127.0.0.1" for loopback-only.

    port : int
        TCP port used by the API server for REST endpoints and the web
        dashboard.

    metrics_port : int
        Separate TCP port to expose Prometheus-compatible metrics at the
        /metrics endpoint. Useful for monitoring purposes.

    log_level : str
        Logging granularity for both Uvicorn and application logs. Supported
        values include "debug", "info", "warning", and "error".

    reload : bool
        When enabled in development mode, this watches file changes in the
        project directory and automatically reloads the API server upon
        modification.

    dev_mode : bool
        Activates development-specific features such as hot reload, API
        documentation access, verbose logging, and relaxed Cross-Origin
        Resource Sharing (CORS) policies.

    stop_event : asyncio.Event
        An event object that serves as the mechanism for signaling shutdown.
        Triggered by system signals like SIGTERM and SIGINT to initiate cleanup
        and termination tasks.

    project_dir : Optional[Path]
        Specifies the root directory of the project, which is monitored during
        hot reload in development mode. Ignored in production or when reload is
        disabled.

    Returns
    -------
    None
        This is an entry-point coroutine that runs indefinitely until the stop
        event is triggered, after which it ensures a graceful shutdown of all
        running services including the API server, OTel configuration,
        Prometheus metrics server, and TriggerEngine.
    """

    app = get_current_app()

    otel_shutdown = _setup_otel(engine, dev_mode)

    prometheus_shutdown = _start_prometheus(metrics_port)

    # Start NodeHeartbeat
    # Publishes volnux:node:alive:{node_id} to Redis with TTL.
    # Other nodes and administrative tooling use this for liveness detection.
    heartbeat = await _start_heartbeat()

    # Start TriggerEngine
    # Arms all triggers registered via WorkflowConfig.ready().
    # TriggerEngine must start AFTER Uvicorn is up so that WebhookTrigger
    # can register its HTTP endpoints on the live server.
    # We defer its start until after Uvicorn is accepting connections.
    trigger_engine = engine

    # Build Uvicorn config
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
        # Server header: suppress to avoid exposing an Uvicorn version
        server_header=False,
        # Date header: keep on for HTTP compliance
        date_header=True,
        # Reload: only meaningful when reload=True (dev mode).
        # We handle reload ourselves via watchfiles for finer control.
        reload=False,
    )

    server = uvicorn.Server(uv_config)

    # Dispatch into the appropriate run mode
    try:
        if reload and dev_mode and project_dir:
            await _run_with_hot_reload(
                server=server,
                app_factory=lambda: app,
                trigger_engine=trigger_engine,
                stop_event=stop_event,
                project_dir=project_dir,
                uv_config=uv_config,
            )
        else:
            await _run_server(
                server=server,
                trigger_engine=trigger_engine,
                stop_event=stop_event,
            )
    finally:
        # Graceful shutdown — always runs
        await _shutdown(
            trigger_engine=trigger_engine,
            heartbeat=heartbeat,
            otel_shutdown=otel_shutdown,
            prometheus_shutdown=prometheus_shutdown,
        )
