"""Pluggable pipeline lifecycle reporters.

A team may want a running pipeline to report its lifecycle (started, task
events, HITL pauses, ...) to some external system — the Volnux governance
event stream this repo ships (see ``volnux.governance``), or anything else a
team builds. Nothing here names that reporter or assumes it is the only
consumer: this module knows how to run a *configured list* of installers
once, and nothing about what they do.

Configuration
-------------
``PIPELINE_REPORTERS`` (default: unset / empty) is a list where each entry is
either:

* a dotted path string, e.g.
  ``"volnux.governance.reporter.install_governance_reporter"``, called with
  no arguments; or
* a mapping ``{"path": "<dotted path>", "kwargs": {...}}``, whose ``kwargs``
  are passed through to the callable.

    PIPELINE_REPORTERS = [
        "volnux.governance.reporter.install_governance_reporter",
        {"path": "myteam.reporting.install", "kwargs": {"endpoint": "..."}},
    ]

Each installer is expected to install itself for the process lifetime and be
safe to no-op on a second call (``install_governance_reporter`` and most
"connect a signal" style installers already are) — this module only
guarantees the *configured list* runs at most once per process, not that any
individual installer is deduplicated against another mechanism that also
happens to call it.

Where this runs
---------------
``volnux.setup.initialise_workflows()`` calls
``ensure_pipeline_reporters_installed()`` right after the project's
``init.py`` has been loaded — the process bootstrap every CLI command, the
Celery worker init, the Kubernetes job runner and ``deploy/k8s/init.py`` go
through. It is deliberately *not* called from ``Pipeline.start()``: installing
process-global signal listeners and a delivery thread is a bootstrap concern,
not something a run should do as a hidden side-effect.

Library users that never call ``initialise_workflows()`` install reporters
explicitly::

    from volnux.reporting import ensure_pipeline_reporters_installed
    ensure_pipeline_reporters_installed()

Cost when unconfigured
-----------------------
Reading ``PIPELINE_REPORTERS`` is one lookup on an already-constructed
``VolnuxConfig`` singleton, done once per process — dozens of core mixins
(connection pooling, checkpointing, retry) already force that construction at
bootstrap, so this adds no new I/O. Critically, this module imports nothing
about *any* specific reporter (not even ``volnux.governance``): a deployment
that never sets ``PIPELINE_REPORTERS`` pays for exactly one dict lookup and
nothing else.
"""

import logging
import typing

from volnux.config import VolnuxConfig
from volnux.import_utils import import_string

logger = logging.getLogger(__name__)

_attempted = False


def ensure_pipeline_reporters_installed() -> None:
    """Run every configured reporter installer, at most once per process.

    Failures are logged and isolated per-installer: one team's misconfigured
    or failing reporter must not stop another's from installing, and neither
    may ever raise into a running pipeline — reporting is observational.
    """
    global _attempted
    if _attempted:
        return
    _attempted = True

    config = VolnuxConfig.get_instance()
    reporters = config.get("PIPELINE_REPORTERS", None)
    if not reporters:
        return

    for entry in reporters:
        if isinstance(entry, str):
            path, kwargs = entry, {}
        elif isinstance(entry, typing.Mapping):
            path = entry["path"]
            kwargs = dict(entry.get("kwargs") or {})
        else:
            logger.error(
                "Invalid PIPELINE_REPORTERS entry %r: expected a dotted path "
                "string or a {'path': ..., 'kwargs': ...} mapping",
                entry,
            )
            continue

        try:
            installer = import_string(path)
            installer(**kwargs)
        except Exception:  # noqa: BLE001 - one bad reporter must not break the rest
            logger.exception("Failed to install pipeline reporter %r", path)
