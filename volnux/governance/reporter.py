"""Bridge the engine's in-process lifecycle signals to governance events.

The engine already fires a rich set of ``volnux.signal.signals`` at every
lifecycle boundary: a pipeline run starting and ending, each task starting,
finishing and retrying, and a task suspending for human input. This module
subscribes to those signals, translates each into a validated
``GovernanceEvent``, and hands it to a ``GovernanceEventPublisher`` that delivers
it off the hot path over whatever messaging backend the deployment has
provisioned.

Three properties are load-bearing and deliberate:

* **Opt-in.** Nothing here runs unless ``install_governance_reporter`` (or
  ``SignalGovernanceReporter.install``) is called. If it is never installed, the
  signals simply have no listener and the engine behaves exactly as it does
  standalone.

* **Non-blocking and exception-isolated.** Translation is cheap and happens
  inside ``_emit``'s guard, so a validation failure is logged and dropped rather
  than raised into the run; the actual send is deferred to the publisher's
  background thread, so a slow or failing transport never disturbs a running
  workflow. Reporting is observational.

* **Retained for the process lifetime.** The signal system holds listeners by
  *weak reference*. On ``install`` the reporter registers itself in a module-level
  strong-reference registry, so it survives for the process lifetime even if the
  caller does not keep the returned instance; ``uninstall`` releases it.

Correlation
-----------
Every event is correlated by ``execution_id = pipeline.id``. One ``Pipeline``
instance corresponds to one run, and ``Pipeline`` exposes ``change_object_id()``,
so when the platform backend dispatches a run it can stamp the governance
``Execution`` id onto the pipeline, and every event this reporter emits then
carries that exact id. Run standalone, the id is simply the engine's own
pipeline id.
"""

import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from volnux.config import VolnuxConfig

from .events import EventType, GovernanceEvent
from .publisher import GovernanceEventPublisher
from .sampling import EventSampler

logger = logging.getLogger(__name__)

volnux_config = VolnuxConfig.get_instance()


# Strong references to every installed reporter. ``SoftSignal`` holds its
# listeners by *weak* reference, so without a strong reference somewhere an
# installed reporter (and its bound-method handlers) can be garbage-collected and
# silently stop reporting. Keeping it here guarantees an installed reporter lives
# for the process lifetime regardless of whether the caller retains the instance.
_installed_reporters: "List[SignalGovernanceReporter]" = []


# ---------------------------------------------------------------------------
# Field extraction
#
# Signals hand us live engine objects (a Pipeline, an ExecutionContext, a task
# event). These helpers pull correlation fields off them defensively: the
# reporter must never raise into the engine, so a missing or renamed attribute
# degrades to ``None`` rather than an AttributeError.
# ---------------------------------------------------------------------------


def _safe_str(value: Any) -> Optional[str]:
    """Stringify a value, mapping ``None`` through unchanged."""
    return None if value is None else str(value)


def _resolve(**kwargs: Any) -> Tuple[Any, Any]:
    """The ``(pipeline, context)`` a signal delivered — one place, one rule.

    Every wired signal names what it sends: ``execution_context`` for anything
    *inside* a run (everything except ``pipeline_execution_start``), ``pipeline``
    for the run itself (``pipeline_execution_start``), and both for
    ``pipeline_stop``/``pipeline_shutdown``. So the name of the keyword is the
    type — no inspecting the object to guess. Whichever the signal did not send
    comes back as ``None``, and the context's pipeline is taken from the context
    (it is declared ``Pipeline``, so absence means the context lost its transient
    link — a rehydrated context — not that we were handed a pipeline).
    """
    context = kwargs.get("execution_context")
    if context is not None:
        return getattr(context, "pipeline", None), context
    return kwargs.get("pipeline"), None


def _task_id(event: Any) -> Optional[str]:
    """Identify a task/event within a run — prefer its name, fall back to id."""
    name = getattr(event, "name", None)
    if name:
        return _safe_str(name)
    return _safe_str(getattr(event, "id", None))


# ---------------------------------------------------------------------------
# Translators: signal payload -> a validated GovernanceEvent
#
# Most wired signals differ only in which event type they map to and whether
# they carry a fixed payload, so they are expressed as table rows in
# ``_build_registrations`` and share the two generic translators below. Only the
# three signals whose payload is derived from the signal's own arguments need a
# function of their own.
#
# Translators touch no backend and are pure apart from the id/timestamp stamped
# in ``_event``; the status of a terminal execution event is taken from *which
# signal fired* rather than read from the async state manager, which keeps these
# synchronous and unambiguous.
# ---------------------------------------------------------------------------


def _event(
    event_type: str,
    *,
    pipeline: Any = None,
    context: Any = None,
    task_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> GovernanceEvent:
    """Build a validated event from what the signal delivered.

    Identity and time are stamped here rather than declared as model defaults:
    formax evaluates ``default_factory`` once when the class is created, so an
    in-model default would give every event in the process the same id.

    ``node_id`` and ``project_id`` are filled here for the same reason. They are
    constant for the life of the process, so a snapshot would in fact be
    correct, but resolving them explicitly keeps provenance out of the model's
    evaluation order and makes it assertable. ``get_node_id`` always returns a
    value (the config generates one when the environment does not supply it);
    ``PROJECT_ID`` is guaranteed present, because the model's own factory would
    have failed at import if it were not.

    Correlation: ``execution_id`` is the run's id (the platform stamps it at
    dispatch), ``workflow_id``/``workflow_name`` come from the context — a
    pipeline does not know its own workflow. A field with nothing to read stays
    ``None``: the model declares them ``Optional`` precisely so this function
    never has to invent a sentinel.
    """
    return GovernanceEvent(
        event_type=event_type,
        event_id=str(uuid.uuid4()),
        occurred_at=time.time(),
        execution_id=_safe_str(getattr(pipeline, "id", None)),
        workflow_id=_safe_str(getattr(context, "workflow_id", None)),
        workflow_name=_safe_str(getattr(context, "workflow_name", None)),
        task_id=task_id,
        sequence=None,
        payload=payload or {},
        node_id=volnux_config.get_node_id(),
        project_id=volnux_config.get("PROJECT_ID"),
    )


def translate_execution(
    event_type: str, payload: Optional[Dict[str, Any]] = None, **kwargs: Any
) -> GovernanceEvent:
    """Execution-level signals: correlation only, plus an optional fixed payload."""
    pipeline, context = _resolve(**kwargs)
    # Copy so the table's payload literal is never shared between events.
    return _event(
        event_type,
        pipeline=pipeline,
        context=context,
        payload=dict(payload) if payload else None,
    )


def translate_task(event_type: str, **kwargs: Any) -> GovernanceEvent:
    """Task-level signals: correlation plus the task the signal names."""
    pipeline, context = _resolve(**kwargs)
    return _event(
        event_type,
        pipeline=pipeline,
        context=context,
        task_id=_task_id(kwargs.get("event")),
    )


def translate_execution_failed(
    execution_context: Any = None, state: Any = None, **_: Any
) -> GovernanceEvent:
    # event_execution_failed is emitted by ExecutionContext.failed() at the
    # execution level. It fires mid-run, before the unconditional
    # pipeline_execution_end (which maps to COMPLETED), so the backend
    # projector's terminal-state freeze keeps the run FAILED.
    pipeline, context = _resolve(execution_context=execution_context)
    return _event(
        EventType.EXECUTION_FAILED,
        pipeline=pipeline,
        context=context,
        payload={"state": _safe_str(state)},
    )


def translate_task_retried(
    event: Any = None,
    execution_context: Any = None,
    task_id: Any = None,
    retry_count: Any = None,
    max_attempts: Any = None,
    backoff: Any = None,
    **_: Any,
) -> GovernanceEvent:
    resolved_task_id = _safe_str(task_id) or _task_id(event)
    pipeline, context = _resolve(execution_context=execution_context)
    return _event(
        EventType.TASK_RETRIED,
        pipeline=pipeline,
        context=context,
        task_id=resolved_task_id,
        payload={
            "retry_count": retry_count,
            "max_attempts": max_attempts,
            "backoff": backoff,
        },
    )


def translate_hitl_requested(
    execution_context: Any = None, request: Any = None, **_: Any
) -> GovernanceEvent:
    # The suspension request carries the prompt/options and its own request_id
    # (the key the engine resumes on); those travel in the payload.
    pipeline, context = _resolve(execution_context=execution_context)
    return _event(
        EventType.HITL_REQUESTED,
        pipeline=pipeline,
        context=context,
        task_id=_safe_str(getattr(request, "task_id", None)),
        payload={
            "request_id": _safe_str(getattr(request, "request_id", None)),
            "title": _safe_str(getattr(request, "title", None)),
            "description": _safe_str(getattr(request, "description", None)),
            "options": list(getattr(request, "options", None) or []),
            "timeout_hours": getattr(request, "timeout_hours", None),
        },
    )


class SignalGovernanceReporter:
    """Connect lifecycle signals to a ``GovernanceEventPublisher``.

    Usage::

        reporter = SignalGovernanceReporter(GovernanceEventPublisher())
        reporter.install()
        # ... keep ``reporter`` referenced for the life of the process ...
        reporter.uninstall()  # optional, on shutdown

    Prefer ``install_governance_reporter``, which constructs, installs and
    returns the reporter in one call.
    """

    def __init__(
        self,
        publisher: GovernanceEventPublisher,
        sampler: Optional[EventSampler] = None,
    ) -> None:
        self._publisher = publisher
        self._sampler = sampler
        self._registrations: List[Tuple[Any, Any]] = []
        self._installed = False

    @property
    def installed(self) -> bool:
        return self._installed

    def install(self) -> None:
        """Start the publisher and subscribe to the lifecycle signals. Idempotent."""
        if self._installed:
            return

        from volnux.signal.signals import GenericSender

        self._publisher.start()
        for signal, handler in self._build_registrations():
            # GenericSender connects to *every* sender of the signal, which is
            # what we want: report all runs regardless of which class emitted.
            # We use GenericSender explicitly (rather than the None alias
            # connect() accepts) so uninstall's disconnect matches: disconnect()
            # does not apply the same None -> GenericSender mapping that connect
            # does, so a None-connected listener cannot be disconnected.
            signal.connect(GenericSender, handler)
            self._registrations.append((signal, handler))

        # Hold a strong reference so weakly-held signal listeners cannot be GC'd.
        if self not in _installed_reporters:
            _installed_reporters.append(self)
        self._installed = True

    def uninstall(self) -> None:
        """Disconnect from all signals and stop the publisher. Idempotent."""
        from volnux.signal.signals import GenericSender

        for signal, handler in self._registrations:
            signal.disconnect(GenericSender, handler)
        self._registrations.clear()
        if self in _installed_reporters:
            _installed_reporters.remove(self)
        self._publisher.stop()
        self._installed = False

    def _build_registrations(self) -> List[Tuple[Any, Any]]:
        """Pair each wired signal with the handler that translates it.

        Wired signals and their mappings:

        * pipeline run: start / end (completed) / stop (cancelled) /
          shutdown (aborted);
        * execution state transitions: failed / paused / resumed (emitted by
          ``ExecutionContext`` at the execution level, with the target state in
          the payload). Wiring ``failed`` also corrects the run status: a failed
          run still fires the unconditional ``pipeline_execution_end``
          (COMPLETED), but ``failed`` fires first and the projector freezes on
          the terminal FAILED;
        * task: start / end (completed) / retry;
        * HITL: a task suspending for human input.

        ``event_execution_cancelled``/``aborted`` are deliberately *not* wired:
        ``pipeline_stop``/``pipeline_shutdown`` already report those run endings,
        so the event-level twins would only duplicate them.
        """
        from volnux.signal import signals as sig

        # Signals that need only correlation and (sometimes) a fixed payload.
        # pipeline_stop fires when the run ended CANCELLED; pipeline_shutdown
        # when it ended ABORTED, which the platform records as a failed
        # execution while keeping the reason.
        execution_signals = (
            (sig.pipeline_execution_start, EventType.EXECUTION_STARTED, None),
            (sig.pipeline_execution_end, EventType.EXECUTION_COMPLETED, None),
            (sig.pipeline_stop, EventType.EXECUTION_STOPPED, {"reason": "cancelled"}),
            (sig.pipeline_shutdown, EventType.EXECUTION_FAILED, {"reason": "aborted"}),
            (sig.event_execution_paused, EventType.EXECUTION_PAUSED, None),
            (sig.event_execution_resumed, EventType.EXECUTION_RESUMED, None),
        )
        task_signals = (
            (sig.event_execution_start, EventType.TASK_STARTED),
            (sig.event_execution_end, EventType.TASK_COMPLETED),
        )

        registrations: List[Tuple[Any, Any]] = [
            (signal, self._handler(translate_execution, event_type, payload))
            for signal, event_type, payload in execution_signals
        ]
        registrations += [
            (signal, self._handler(translate_task, event_type))
            for signal, event_type in task_signals
        ]
        # Signals whose payload is derived from their own arguments.
        registrations += [
            (sig.event_execution_failed, self._handler(translate_execution_failed)),
            (sig.event_execution_retry, self._handler(translate_task_retried)),
            (sig.hitl_requested, self._handler(translate_hitl_requested)),
        ]
        return registrations

    # -- Signal handlers: translate, then hand off (isolated) ---------------

    def _handler(self, translate: Any, *bound: Any) -> Any:
        """Build the listener a signal is connected to.

        Each call closes over its own ``translate``/``bound``, so table-driven
        registration cannot fall foul of late binding. The returned closure is
        kept alive by ``self._registrations`` (and the reporter itself by
        ``_installed_reporters``), which matters because the signal system holds
        its listeners weakly.
        """

        def handle(**kwargs: Any) -> None:
            self._emit(translate, *bound, **kwargs)

        return handle

    def _emit(self, translate: Any, *args: Any, **kwargs: Any) -> None:
        """Translate, sample, and hand off to the publisher (non-blocking).

        Translation happens *inside* the guard rather than at the call site: the
        translator now builds a validated ``GovernanceEvent``, so it can raise,
        and reporting must never raise into the engine. A bad event is logged and
        dropped while the run continues.

        With a sampler, critical events pass straight through while telemetry is
        held back for the publisher's periodic reservoir flush; without one, every
        event goes straight to the publisher.
        """
        try:
            event = translate(*args, **kwargs)
            if event is None:
                return
            if self._sampler is None:
                self._publisher.submit(event)
            else:
                for sampled in self._sampler.offer(event):
                    self._publisher.submit(sampled)
        except Exception:  # noqa: BLE001 - reporting must never raise into the engine
            logger.exception(
                "Failed to emit governance event from %s",
                getattr(translate, "__name__", translate),
            )


def install_governance_reporter(
    publisher: Optional[GovernanceEventPublisher] = None,
    sampler: Optional[EventSampler] = None,
    *,
    enable_sampling: bool = True,
    flush_interval: float = 1.0,
) -> SignalGovernanceReporter:
    """Construct, install and return a reporter.

    The installed reporter is kept alive by a module-level registry, so the
    caller need not retain the returned instance (it is returned for convenience
    and so ``uninstall`` can be called).

    By default telemetry is reservoir-sampled: a default ``EventSampler`` is
    created and a default ``GovernanceEventPublisher`` is wired to flush it every
    ``flush_interval`` seconds. Pass ``enable_sampling=False`` to send every
    event. If you pass your own ``publisher`` together with a sampler, wire its
    ``flush_source`` to ``sampler.drain`` yourself; only a publisher created here
    is wired automatically.
    """
    if not enable_sampling:
        sampler = None
    elif sampler is None:
        sampler = EventSampler()

    if publisher is None:
        publisher = GovernanceEventPublisher(
            flush_source=(sampler.drain if sampler is not None else None),
            flush_interval=flush_interval,
        )

    reporter = SignalGovernanceReporter(publisher, sampler=sampler)
    reporter.install()
    return reporter
