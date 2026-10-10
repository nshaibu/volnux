"""
Wrapper for executing tasks on remote workers with mini-context.
"""

import os
import time
import typing
import logging
from dataclasses import dataclass

from .delta import ContextDelta
from .context import MiniExecutionContext
from volnux.import_utils import import_string
from volnux.result import EventResult

logger = logging.getLogger(__name__)


@dataclass
class RemoteTaskPayload:
    """
    Payload sent to a remote worker for task execution.

    Contains everything needed to execute the task:
    - Event class and initialization args
    - Mini execution context
    - Task execution args

    :ivar event_class_path: Dotted import path to the event class,
        e.g. ``"myapp.events.MyEvent"``.
    :ivar event_init_kwargs: Keyword arguments forwarded to the event
        constructor (``execution_context`` is injected by the worker and
        must NOT be included here).
    :ivar mini_context: Serialised ``MiniExecutionContext`` dict.
    :ivar event_call_args: Positional arguments are forwarded to the event call.
    :ivar event_call_kwargs: Keyword arguments forwarded to the event call.
    :ivar task_id: Unique identifier for this task invocation.
    """

    event_class_path: str
    event_init_kwargs: typing.Dict[str, typing.Any]
    mini_context: typing.Dict[str, typing.Any]
    event_call_args: typing.Tuple[typing.Any, ...]
    event_call_kwargs: typing.Dict[str, typing.Any]
    task_id: str

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "event_class_path": self.event_class_path,
            "event_init_kwargs": self.event_init_kwargs,
            "mini_context": self.mini_context,
            # Serialise as list; from_dict restores the tuple.
            "event_call_args": list(self.event_call_args),
            "event_call_kwargs": self.event_call_kwargs,
            "task_id": self.task_id,
        }

    @classmethod
    def from_dict(cls, data: typing.Dict[str, typing.Any]) -> "RemoteTaskPayload":
        return cls(
            event_class_path=data["event_class_path"],
            event_init_kwargs=data["event_init_kwargs"],
            mini_context=data["mini_context"],
            # Restore tuple after JSON round-trip.
            event_call_args=tuple(data.get("event_call_args", ())),
            event_call_kwargs=data["event_call_kwargs"],
            task_id=data["task_id"],
        )


@dataclass
class RemoteTaskResult:
    """
    Result returned from the remote worker after task execution.

    :ivar event_result: Serialised ``EventResult`` dict.
    :ivar context_delta: Serialised ``ContextDelta`` dict.
    :ivar worker_hostname: Hostname of the machine that ran the task.
    :ivar worker_pid: PID of the process that ran the task.
    :ivar execution_duration: Wall-clock seconds from task start to finish.
    """

    event_result: typing.Dict[str, typing.Any]
    context_delta: typing.Dict[str, typing.Any]
    worker_hostname: str
    worker_pid: int
    execution_duration: float

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "event_result": self.event_result,
            "context_delta": self.context_delta,
            "worker_hostname": self.worker_hostname,
            "worker_pid": self.worker_pid,
            "execution_duration": self.execution_duration,
        }

    @classmethod
    def from_dict(cls, data: typing.Dict[str, typing.Any]) -> "RemoteTaskResult":
        return cls(**data)


async def execute_remote_task(
    payload: RemoteTaskPayload,
    flush_callback: typing.Optional[typing.Callable[[typing.Dict], None]] = None,
) -> RemoteTaskResult:
    """
    Execute a task on the current worker process and return the result.

    Orchestrates:
    1. Deserialising the ``MiniExecutionContext`` and injecting the flush
       callback (required for batch/aggregate mode streaming).
    2. Importing and instantiating the event class.
    3. Running the event coroutine in a fresh event loop.
    4. Finalising the context so all buffered data is flushed.
    5. Computing and returning the ``ContextDelta`` alongside the result.

    Errors raised by the event are caught, recorded via
    ``mini_context.add_error``, and returned as an error ``EventResult``
    rather than propagating — the worker itself does not crash.

    :param payload: All data needed to reconstruct and execute the event.
    :param flush_callback: Optional callable that receives batch dicts of the
        form ``{"type": "result_batch"|"error_batch", "data": [...], "count": N}``.
        Must be provided when the event is expected to produce more than
        ``MiniExecutionContext.SMALL_THRESHOLD`` items; omitting it for
        high-volume tasks will raise ``RuntimeError`` on the first flush.
    :raises TypeError: If the event returns a value that is not an
        ``EventResult`` instance.
    :return: Execution result, context delta, and worker metadata.
    """
    start_time = time.time()

    hostname: str = os.uname().nodename
    pid: int = os.getpid()

    mini_context = MiniExecutionContext.from_dict(payload.mini_context)
    if flush_callback is not None:
        await mini_context.set_flush_callback(flush_callback)

    EventClass = import_string(payload.event_class_path)

    event_init_kwargs = payload.event_init_kwargs.copy()
    event_init_kwargs["execution_context"] = mini_context
    event = EventClass(**event_init_kwargs)

    event_result: EventResult

    try:

        raw_result = await event(*payload.event_call_args, **payload.event_call_kwargs)

        if not isinstance(raw_result, EventResult):
            logger.error(f"Event {EventClass.__name__} returned {raw_result}")
            raise TypeError(
                f"{EventClass.__name__} returned {type(raw_result).__name__}, "
                f"expected EventResult. Ensure every event coroutine returns "
                f"an EventResult instance."
            )

        event_result = raw_result

    except Exception as exc:
        logger.error(f"Error executing event {EventClass.__name__}: {exc}")
        event_result = EventResult(
            error=True,
            content=str(exc),
            task_id=payload.task_id,
            event_name=EventClass.__name__,
        )
        await mini_context.add_error(
            {
                "error": True,
                "content": str(exc),
                "type": type(exc).__name__,
            }
        )

    await mini_context.finalize()

    execution_duration = time.time() - start_time

    context_delta = ContextDelta.compute(
        mini_context,
        execution_duration=execution_duration,
        worker_info={"hostname": hostname, "pid": pid},
    )

    return RemoteTaskResult(
        event_result=event_result.as_dict(),
        context_delta=context_delta.to_dict(),
        worker_hostname=hostname,
        worker_pid=pid,
        execution_duration=execution_duration,
    )
