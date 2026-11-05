"""
Telemetry for ``initialise_workflows``, without importing OpenTelemetry.

OpenTelemetry is an optional install and ``volnux.otel`` imports it as soon as the
package is imported. Everything here therefore looks telemetry up lazily: it only
imports ``volnux.otel`` when ``OTEL_ENABLED`` is set, and otherwise (or if the
install is missing) does nothing. Nothing here can make setup fail.
"""

import contextlib
import logging
import sys
from typing import Any, Iterator, Optional

from volnux.config import setting

logger = logging.getLogger(__name__)

__all__ = [
    "start_tracing_for_setup",
    "setup_phase",
    "record_setup_duration",
    "shutdown_observability",
]


def _running_otel() -> Optional[Any]:
    """The ``volnux.otel`` module if telemetry is running, else None. Looked up in
    ``sys.modules``: asking must not import OpenTelemetry (an optional install)."""
    module = sys.modules.get("volnux.otel")
    if module is None:
        return None
    try:
        return module if module.VolnuxObservability.is_initialized() else None
    except Exception:  # pragma: no cover - defensive
        return None


def start_tracing_for_setup(
    project_name: str = "volnux", version: str = "unknown", role: Optional[str] = None
) -> bool:
    """Start telemetry for ``initialise_workflows`` if ``OTEL_ENABLED``.

    Does nothing (and imports nothing) when it is off, and nothing more when it is
    already running. The role (``api``, ``worker``, ...) defaults to the ``ROLE``
    setting. Never raises.

    :return: whether telemetry is running afterwards.
    """
    try:
        if not bool(setting("OTEL_ENABLED", False)):
            return False
        if _running_otel() is not None:
            return True
        from volnux.otel.bootstrap import setup_otel

        setup_otel(
            project_name=project_name,
            version=version,
            dev_mode=bool(setting("DEBUG", False)),
            role=role or str(setting("ROLE", "api")),
        )
        return _running_otel() is not None
    except ImportError as exc:
        logger.warning(
            "Volnux OTel module not available (%s) — tracing disabled. "
            "Ensure volnux[otel] is installed.",
            exc,
        )
        return False
    except Exception as exc:  # setup_otel does not raise; defensive
        logger.warning("Could not start telemetry (%s)", exc)
        return False


@contextlib.contextmanager
def setup_phase(name: str, **attributes: Any) -> Iterator[Optional[Any]]:
    """A ``setup.<name>`` span around a phase of ``initialise_workflows``.

    Yields the span (None when telemetry is off). An exception is recorded on the
    span and propagates unchanged.
    """
    otel = _running_otel()
    tracer = None
    if otel is not None:
        try:
            from volnux.otel.instrumentations._common import get_tracer

            tracer = get_tracer()
        except Exception:
            tracer = None
    if tracer is None:
        yield None
        return

    from volnux.otel.instrumentations._common import set_attributes

    with tracer.start_as_current_span(f"setup.{name}") as span:
        set_attributes(span, attributes)
        yield span


def record_setup_duration(seconds: float, outcome: str) -> None:
    """The ``setup.duration`` metric (ms), if metrics are collected."""
    if _running_otel() is None:
        return
    try:
        from volnux.otel.metrics import VolnuxMetricsRegistry, get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_metric(
                VolnuxMetricsRegistry.SETUP_DURATION,
                seconds * 1000.0,
                {"outcome": outcome},
            )
    except Exception:
        logger.debug("Could not record setup.duration", exc_info=True)


def shutdown_observability() -> None:
    """Shut telemetry down if it is running (nothing to do otherwise)."""
    otel = _running_otel()
    if otel is None:
        return
    try:
        otel.shutdown_otel()
    except Exception as exc:
        logger.warning("OTel shutdown error: %s", exc)
