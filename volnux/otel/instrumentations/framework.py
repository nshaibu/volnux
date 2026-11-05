"""
OpenTelemetry instrumentation of the key parts of the framework beyond the
pipeline itself.

======================  ===========================================================
part                    spans (metrics in brackets)
======================  ===========================================================
trigger engine          ``trigger_engine.start|stop|force_stop``, ``trigger.fire``,
                        ``trigger.activate`` [trigger.activations]
workflow executors      ``workflow.run`` (every executor, via ``__call__``) and
                        ``executor.<Class>.execute`` [workflow.run.count,
                        workflow.run.duration]
workflow registry       ``registry.load``, ``registry.load_source``
                        [registry.load.duration]
checkpoints             ``checkpoint.persist``, ``checkpoint.build_event``
                        [checkpoint.duration, checkpoint.operations]
rehydration             ``rehydrate.event``, ``rehydrate.resume``,
                        ``rehydrate.node``
sagas                   ``saga.execute|resume|step|compensate|dead_letter``,
                        ``saga.recovery.save|load|delete`` [saga.duration,
                        saga.executions]
messaging               ``messaging.<operation>`` for publish, push, pop, pop_many,
                        enqueue, dequeue, queue_delete, stream_append, stream_read,
                        stream_ack [messaging.duration, messaging.operations]
remote batch streaming  ``remote.execute_task``, ``remote.read_stream``
======================  ===========================================================

Attributes are bounded: class names, workflow names, statuses. Ids (execution,
saga) are span attributes, never metric attributes.
"""

import importlib
import logging
from typing import Any, Callable, Dict, Optional, Tuple

from ..metrics import VolnuxMetricsRegistry as Registry, get_metrics_collector
from ._common import traced
from ._patching import patch_attr

logger = logging.getLogger(__name__)

__all__ = ["patch_all", "PATCH_TABLE"]


def _record(definition, value: float, **attributes: Any) -> None:
    """Record a metric if metrics are collected; never raises."""
    collector = get_metrics_collector()
    if collector is None:
        return
    try:
        collector.record_metric(
            definition,
            value,
            {k: str(v) for k, v in attributes.items() if v is not None},
        )
    except Exception:
        logger.debug("Could not record %s", definition.name, exc_info=True)


def _outcome(error: Optional[BaseException]) -> str:
    return "error" if error is not None else "success"


def _ms(seconds: float) -> float:
    return seconds * 1000.0


# ----------------------------------------------------------------- trigger engine
def _trigger_type(activation) -> Optional[str]:
    source = getattr(activation, "activation_source", None)
    return getattr(source, "value", source)


def _activation_attributes(
    self, activation, *args: Any, **kwargs: Any
) -> Dict[str, Any]:
    trigger = getattr(self, "triggers", {}).get(getattr(activation, "trigger_id", None))
    return {
        "trigger.id": getattr(activation, "trigger_id", None),
        "trigger.type": _trigger_type(activation),
        "workflow.name": getattr(trigger, "workflow_name", None),
    }


def _activation_finished(args, kwargs, duration, error) -> None:
    activation = args[1] if len(args) > 1 else kwargs.get("activation")
    _record(
        Registry.TRIGGER_ACTIVATIONS,
        1,
        trigger_type=_trigger_type(activation),
        outcome=_outcome(error),
    )


def _fire_attributes(
    self, workflow_name, workflow_params=None, trigger_type=None, *a, **k
):
    return {
        "workflow.name": workflow_name,
        "trigger.type": getattr(trigger_type, "value", trigger_type),
    }


def _engine_attributes(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return {
        "trigger_engine.triggers": len(getattr(self, "triggers", {})),
        "trigger_engine.consumers": getattr(self, "consumer_concurrency", None),
    }


# ------------------------------------------------------------- workflow executors
def _run_attributes(self, workflow_name, params=None, *args: Any, **kwargs: Any):
    from volnux.context import (
        get_current_correlation_id,
        get_current_execution_id,
    )

    return {
        "workflow.name": workflow_name,
        "executor.type": self.__class__.__name__,
        "run.type": (
            (params or {}).get("run_type", "single")
            if isinstance(params, dict)
            else None
        ),
        "volnux.correlation_id": get_current_correlation_id(),
        "volnux.execution_id": get_current_execution_id(),
    }


def _run_finished(args, kwargs, duration, error) -> None:
    self = args[0]
    workflow_name = args[1] if len(args) > 1 else kwargs.get("workflow_name")
    executor = self.__class__.__name__
    status = _outcome(error)
    _record(
        Registry.WORKFLOW_RUN_COUNT,
        1,
        workflow=workflow_name,
        executor=executor,
        outcome=status,
    )
    _record(
        Registry.WORKFLOW_RUN_DURATION,
        _ms(duration),
        workflow=workflow_name,
        executor=executor,
        outcome=status,
    )


def _executor_execute_attributes(self, workflow_name, *args: Any, **kwargs: Any):
    return {"workflow.name": workflow_name, "executor.type": self.__class__.__name__}


# --------------------------------------------------------------------- registry
def _load_attributes(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return {"registry.sources": len(self.combined_workflow_sources)}


def _load_after(args, kwargs, result) -> Dict[str, Any]:
    registry = args[0]
    return {
        "registry.workflows": len(registry.get_workflow_configs()),
        "registry.failed_sources": len(registry.load_errors),
    }


def _load_finished(args, kwargs, duration, error) -> None:
    _record(Registry.REGISTRY_LOAD_DURATION, _ms(duration), outcome=_outcome(error))


def _source_attributes(workflow_source, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    source_type = getattr(workflow_source, "source_type", None)
    return {
        "source.name": getattr(workflow_source, "name", None),
        "source.type": getattr(source_type, "value", source_type),
    }


def _source_result(result) -> Dict[str, Any]:
    return {"source.error": bool(getattr(result, "error", False))}


# ----------------------------------------------------- checkpoints & rehydration
def _checkpoint_finished(operation: str) -> Callable[..., None]:
    def finished(args, kwargs, duration, error) -> None:
        status = _outcome(error)
        _record(Registry.CHECKPOINT_COUNT, 1, operation=operation, outcome=status)
        _record(
            Registry.CHECKPOINT_DURATION,
            _ms(duration),
            operation=operation,
            outcome=status,
        )

    return finished


def _snapshot_class(self, snapshot=None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return {"checkpoint.snapshot": snapshot.__class__.__name__}


def _event_class(self, event=None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return {"event.class": event.__class__.__name__}


def _rehydrate_event_attributes(
    self, snapshot=None, *a: Any, **k: Any
) -> Dict[str, Any]:
    return {
        "event.class": getattr(snapshot, "class_path", "").rpartition(".")[2] or None,
        "event.phase": getattr(getattr(snapshot, "phase", None), "name", None),
    }


def _state_id_attributes(self, state_id=None, *a: Any, **k: Any) -> Dict[str, Any]:
    return {"rehydrate.state_id": state_id}


# ------------------------------------------------------------------------ sagas
def _saga_attributes(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return {
        "saga.id": getattr(self, "saga_id", None),
        "saga.name": getattr(self, "_name", None) or getattr(self, "name", None),
        "saga.steps": len(getattr(self, "_steps", ())),
    }


def _saga_result(result) -> Dict[str, Any]:
    status = getattr(result, "status", None)
    return {
        "saga.status": getattr(status, "value", status),
        "saga.failed_step": getattr(result, "failed_step_index", None),
        "saga.duration_s": getattr(result, "total_time", None),
    }


def _saga_finished(args, kwargs, duration, error) -> None:
    _record(Registry.SAGA_COUNT, 1, outcome=_outcome(error))
    _record(Registry.SAGA_DURATION, _ms(duration), outcome=_outcome(error))


def _saga_step_attributes(self, index=None, step=None, *a: Any, **k: Any):
    return {
        "saga.id": getattr(self, "saga_id", None),
        "saga.step.index": index,
        "saga.step.name": getattr(step, "name", None),
    }


def _saga_compensate_attributes(self, failed_step_index=None, *a: Any, **k: Any):
    return {
        "saga.id": getattr(self, "saga_id", None),
        "saga.failed_step": failed_step_index,
    }


def _saga_resume_attributes(cls, saga_id=None, *a: Any, **k: Any):
    return {"saga.id": saga_id}


def _recovery_attributes(cls, saga_id=None, *a: Any, **k: Any):
    return {"saga.id": saga_id}


# -------------------------------------------------------------------- messaging
def _messaging_attributes(operation: str) -> Callable[..., Dict[str, Any]]:
    def attributes(cls, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {
            "messaging.operation": operation,
            "messaging.model": cls.__name__,
            "messaging.timeout": kwargs.get("timeout"),
        }

    return attributes


def _messaging_finished(operation: str) -> Callable[..., None]:
    def finished(args, kwargs, duration, error) -> None:
        model = args[0].__name__ if args else None
        status = _outcome(error)
        _record(
            Registry.MESSAGING_OPERATIONS,
            1,
            operation=operation,
            model=model,
            outcome=status,
        )
        _record(
            Registry.MESSAGING_DURATION,
            _ms(duration),
            operation=operation,
            model=model,
        )

    return finished


MESSAGING_OPERATIONS = (
    "publish",
    "push",
    "pop",
    "pop_many",
    "enqueue",
    "dequeue",
    "queue_delete",
    "stream_append",
    "stream_read",
    "stream_ack",
)


def _messaging_wrapper(operation: str) -> Callable[[Callable], Callable]:
    return traced(
        f"messaging.{operation}",
        attributes=_messaging_attributes(operation),
        on_finish=_messaging_finished(operation),
    )


# ----------------------------------------------------------------------- remote
def _remote_attributes(payload=None, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    context = getattr(payload, "mini_context", None) or {}
    return {
        "remote.event": getattr(payload, "event_class_path", "").rpartition(".")[2]
        or None,
        "remote.task_id": getattr(payload, "task_id", None),
        "remote.state_id": (
            context.get("state_id") if isinstance(context, dict) else None
        ),
    }


def _remote_result(result) -> Dict[str, Any]:
    delta = getattr(result, "context_delta", None) or {}
    return {
        "remote.mode": delta.get("mode") if isinstance(delta, dict) else None,
        "remote.final_status": (
            delta.get("final_status") if isinstance(delta, dict) else None
        ),
        "remote.duration_s": getattr(result, "execution_duration", None),
    }


def _stream_attributes(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    return {"remote.stream": getattr(self, "key", None)}


# ------------------------------------------------------------------- the table
Wrapper = Callable[[Callable[..., Any]], Callable[..., Any]]

#: (module, owner, attribute, instrumenter). The owner is a class name in the
#: module, or "" for a function of the module itself.
PATCH_TABLE: Tuple[Tuple[str, str, str, Wrapper], ...] = (
    # trigger engine
    (
        "volnux.engine.workflows.trigger.engine",
        "TriggerEngine",
        "_handle_activation",
        traced(
            "trigger.activate",
            attributes=_activation_attributes,
            on_finish=_activation_finished,
        ),
    ),
    (
        "volnux.engine.workflows.trigger.engine",
        "TriggerEngine",
        "fire_trigger",
        traced("trigger.fire", attributes=_fire_attributes),
    ),
    (
        "volnux.engine.workflows.trigger.engine",
        "TriggerEngine",
        "start",
        traced("trigger_engine.start", attributes=_engine_attributes),
    ),
    (
        "volnux.engine.workflows.trigger.engine",
        "TriggerEngine",
        "stop",
        traced("trigger_engine.stop", attributes=_engine_attributes),
    ),
    (
        "volnux.engine.workflows.trigger.engine",
        "TriggerEngine",
        "force_stop",
        traced(
            "trigger_engine.force_stop",
            attributes=_engine_attributes,
            result_attributes=lambda dropped: {"trigger_engine.dropped": dropped},
        ),
    ),
    # workflow executors: every run goes through __call__
    (
        "volnux.engine.workflows.trigger.executors.base",
        "WorkflowExecutor",
        "__call__",
        traced("workflow.run", attributes=_run_attributes, on_finish=_run_finished),
    ),
    (
        "volnux.engine.workflows.trigger.executors.asyncio_executor",
        "AsyncTaskWorkflowExecutor",
        "execute",
        traced(
            "executor.AsyncTaskWorkflowExecutor.execute",
            attributes=_executor_execute_attributes,
        ),
    ),
    (
        "volnux.engine.workflows.trigger.executors.celery",
        "CeleryWorkflowExecutor",
        "execute",
        traced(
            "executor.CeleryWorkflowExecutor.execute",
            attributes=_executor_execute_attributes,
        ),
    ),
    (
        "volnux.engine.workflows.trigger.executors.kubernetes",
        "KubernetesWorkflowExecutor",
        "execute",
        traced(
            "executor.KubernetesWorkflowExecutor.execute",
            attributes=_executor_execute_attributes,
        ),
    ),
    # registry
    (
        "volnux.engine.workflows.registry",
        "WorkflowRegistry",
        "load_workflow_configs",
        traced(
            "registry.load",
            attributes=_load_attributes,
            after=_load_after,
            on_finish=_load_finished,
        ),
    ),
    (
        "volnux.engine.workflows.registry",
        "WorkflowRegistry",
        "process_workflow_source",
        traced(
            "registry.load_source",
            attributes=_source_attributes,
            result_attributes=_source_result,
        ),
    ),
    # checkpoints & rehydration
    (
        "volnux.execution.rehydrator.checkpoint_manager",
        "VolnuxCheckPointManager",
        "_persist_with_retry",
        traced(
            "checkpoint.persist",
            attributes=_snapshot_class,
            on_finish=_checkpoint_finished("persist"),
        ),
    ),
    (
        "volnux.execution.rehydrator.event.builder",
        "SnapshotBuilder",
        "build",
        traced(
            "checkpoint.build_event",
            attributes=_event_class,
            on_finish=_checkpoint_finished("build_event"),
        ),
    ),
    (
        "volnux.execution.rehydrator.event.rehydrator",
        "EventRehydrator",
        "rehydrate",
        traced(
            "rehydrate.event",
            attributes=_rehydrate_event_attributes,
            on_finish=_checkpoint_finished("rehydrate_event"),
        ),
    ),
    (
        "volnux.execution.rehydrator.engine.rehydrator",
        "LazyRehydrator",
        "resume_from",
        traced(
            "rehydrate.resume",
            attributes=_state_id_attributes,
            on_finish=_checkpoint_finished("resume"),
        ),
    ),
    (
        "volnux.execution.rehydrator.engine.rehydrator",
        "LazyRehydrator",
        "rehydrate_node",
        traced("rehydrate.node", attributes=_state_id_attributes),
    ),
    # sagas
    (
        "volnux.backends.saga.saga",
        "Saga",
        "execute",
        traced(
            "saga.execute",
            attributes=_saga_attributes,
            result_attributes=_saga_result,
            on_finish=_saga_finished,
        ),
    ),
    (
        "volnux.backends.saga.saga",
        "Saga",
        "resume",
        traced(
            "saga.resume",
            attributes=_saga_resume_attributes,
            result_attributes=_saga_result,
            on_finish=_saga_finished,
        ),
    ),
    (
        "volnux.backends.saga.saga",
        "Saga",
        "_execute_step",
        traced("saga.step", attributes=_saga_step_attributes),
    ),
    (
        "volnux.backends.saga.saga",
        "Saga",
        "_run_compensation",
        traced("saga.compensate", attributes=_saga_compensate_attributes),
    ),
    (
        "volnux.backends.saga.saga",
        "Saga",
        "_enqueue_dead_letter",
        traced("saga.dead_letter", attributes=_saga_compensate_attributes),
    ),
    (
        "volnux.backends.saga.recovery",
        "SagaRecoveryManager",
        "save_state",
        traced("saga.recovery.save", attributes=_recovery_attributes),
    ),
    (
        "volnux.backends.saga.recovery",
        "SagaRecoveryManager",
        "load_state",
        traced("saga.recovery.load", attributes=_recovery_attributes),
    ),
    (
        "volnux.backends.saga.recovery",
        "SagaRecoveryManager",
        "delete_state",
        traced("saga.recovery.delete", attributes=_recovery_attributes),
    ),
    # remote execution
    (
        "volnux.execution.remote.task",
        "",
        "execute_remote_task",
        traced(
            "remote.execute_task",
            attributes=_remote_attributes,
            result_attributes=_remote_result,
        ),
    ),
    (
        "volnux.execution.remote.celery",
        "",
        "execute_remote_task",
        traced(
            "remote.execute_task",
            attributes=_remote_attributes,
            result_attributes=_remote_result,
        ),
    ),
    (
        "volnux.execution.remote.stream",
        "BatchStreamReader",
        "read_all",
        traced("remote.read_stream", attributes=_stream_attributes),
    ),
) + tuple(
    (
        "volnux.mixins.messaging",
        "MessagingBackendIntegrationMixin",
        operation,
        _messaging_wrapper(operation),
    )
    for operation in MESSAGING_OPERATIONS
)


def patch_all() -> int:
    """Instrument everything in :data:`PATCH_TABLE` (each once). A part whose
    module cannot be imported (an optional dependency) is skipped. Returns how many
    attributes this call patched."""
    patched = 0
    for module_name, owner_name, attribute, wrapper in PATCH_TABLE:
        try:
            module = importlib.import_module(module_name)
            owner = getattr(module, owner_name) if owner_name else module
            patched += bool(patch_attr(owner, attribute, wrapper))
        except ImportError as exc:
            logger.debug("Skipping %s.%s: %s", module_name, attribute, exc)
        except AttributeError as exc:
            logger.warning("Cannot instrument %s.%s: %s", module_name, attribute, exc)
    if patched:
        logger.info(
            "Framework components instrumented with OpenTelemetry (%d)", patched
        )
    return patched
