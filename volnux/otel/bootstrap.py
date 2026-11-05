"""
Start OpenTelemetry from the settings.

``initialise_workflows`` (``volnux.setup``) starts telemetry through
``volnux.observability`` as soon as the project's configuration is loaded, so every
way of running workflows (the CLI, the servers, Celery workers, Kubernetes Jobs) is
traced from its first step. The servers' :func:`setup_otel` then finds telemetry
already running and only hands back the function that shuts it down.

``setup_otel`` reads the ``OTEL_*`` settings, calls :func:`volnux.otel.initialize_otel`
(one path for the tracer, the patched components, the metrics and the trace
propagation) and returns the function that shuts it down. It never raises: a
problem with telemetry must not stop a server.

Settings: ``OTEL_ENABLED`` (default False), ``OTEL_BACKEND`` (``grafana``,
``datadog``, ``jaeger`` or ``otlp``), ``OTEL_ENDPOINT``, ``OTEL_TEMPO_URL``,
``OTEL_DATADOG_URL``, ``OTEL_SERVICE_NAME``, ``OTEL_SAMPLE_RATE``.
"""

import logging
from typing import Any, Callable, Dict

from volnux.config import setting

logger = logging.getLogger(__name__)

__all__ = ["setup_otel"]


def _noop() -> None:
    return None


def setup_otel(
    project_name: str = "volnux",
    version: str = "unknown",
    dev_mode: bool = False,
    role: str = "api",
) -> Callable[[], None]:
    """Initialise OpenTelemetry if ``OTEL_ENABLED``; returns the shutdown function
    (a no-op when telemetry is off or failed to start)."""
    if not bool(setting("OTEL_ENABLED", False)):
        logger.debug("OTel disabled (OTEL_ENABLED is false)")
        return _noop

    try:
        from volnux.otel import VolnuxObservability, initialize_otel, shutdown_otel
    except ImportError as exc:
        logger.warning(
            "Volnux OTel module not available (%s) — tracing disabled. "
            "Ensure volnux[otel] is installed.",
            exc,
        )
        return _noop

    if VolnuxObservability.is_initialized():
        # (already started: by initialise_workflows, or by the project itself)
        logger.debug("OTel already initialised; not starting it again")
        return _shutdown_function(shutdown_otel)

    try:
        backend = str(setting("OTEL_BACKEND", "grafana")).lower()
        endpoint = setting("OTEL_ENDPOINT", "http://localhost:4317")
        service_name = setting("OTEL_SERVICE_NAME", None) or project_name

        config: Dict[str, Dict[str, Any]]
        if backend == "datadog":
            config = {
                "datadog": {"agent_url": setting("OTEL_DATADOG_URL", None) or endpoint}
            }
        elif backend in ("grafana", "tempo"):
            config = {
                backend: {"endpoint": setting("OTEL_TEMPO_URL", None) or endpoint}
            }
        elif backend == "jaeger":
            config = {"jaeger": {"endpoint": endpoint}}
        else:
            backend = "otlp"
            config = {"otlp": {"endpoint": endpoint}}

        environment = (
            "development" if dev_mode or setting("DEBUG", False) else "production"
        )
        initialize_otel(
            service_name,
            service_version=version,
            environment=environment,
            backends=[backend],
            config=config,
            custom_attributes={
                "volnux.node.id": setting("NODE_ID", "local"),
                "volnux.role": role,
            },
            sample_rate=float(setting("OTEL_SAMPLE_RATE", 1.0)),
        )
        logger.info(
            "Volnux OTel initialised: service=%s env=%s backend=%s",
            service_name,
            environment,
            backend,
        )

        _instrument_fastapi()

        return _shutdown_function(shutdown_otel)

    except Exception as exc:
        logger.warning(
            "OTel setup failed (%s) — tracing disabled. "
            "Workflow execution is unaffected.",
            exc,
        )
        return _noop


def _shutdown_function(shutdown_otel: Callable[[], None]) -> Callable[[], None]:
    def shutdown() -> None:
        try:
            shutdown_otel()
            logger.info("OTel shut down and spans flushed")
        except Exception as exc:
            logger.warning("OTel shutdown error: %s", exc)

    return shutdown


def _instrument_fastapi() -> None:
    """A root span for every HTTP request (health and metrics are left out: they
    are frequent and say nothing)."""
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor().instrument(excluded_urls="api/v1/health,metrics")
        logger.debug("FastAPI instrumented with OTel")
    except ImportError:
        logger.debug(
            "opentelemetry-instrumentation-fastapi not installed — "
            "HTTP request spans will be missing."
        )
