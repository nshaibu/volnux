import asyncio
import inspect
import logging
import threading
import uuid
from collections import deque
from copy import deepcopy
from typing import (
    Any,
    Deque,
    Dict,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
    TYPE_CHECKING,
)
from dataclasses import dataclass, field, InitVar

from volnux.constants import EMPTY
from volnux.exceptions import TaskCancelledError
from volnux.concurrency.async_utils import to_thread
from volnux.execution.context import ExecutionContext
from volnux.executors import BaseExecutor, ProcessPoolExecutor
from volnux.executors.utils.registry import get_global_executor_registry
from volnux.mixins import ObjectIdentityMixin

# from volnux.mixins.event.checkpointing import SubPhaseSuspension
from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskGroupingProtocol, TaskProtocol, TaskType
from volnux.signal import SoftSignal
from volnux.signal.signals import event_execution_end, event_execution_start
from volnux.utils import build_event_arguments_from_pipeline, get_function_call_args
from .bridge.communications.tasks import (
    ChannelUnavailable,
    TaskCommand,
    TaskCommunicationBridge,
    TaskExecutionHandle,
    CommandType,
    create_communication_bridge,
)

if TYPE_CHECKING:
    from volnux import Event

logger = logging.getLogger(__name__)

_GLOBAL_EXECUTOR_REGISTRY = get_global_executor_registry()


def _log_signal_task_failure(task: "asyncio.Future[Any]") -> None:
    if task.cancelled():
        return
    exc = task.exception()  # also marks the exception as retrieved
    if exc is not None:
        logger.error("Signal emission failed: %s", exc, exc_info=exc)


def _emit_signal_sync(
    signal: SoftSignal, **kwargs: Any
) -> Optional["asyncio.Future[Any]"]:
    """Emit ``signal`` from a (sync) done-callback without blocking.

    Returns the scheduled task so the caller can keep a reference to it (the
    event loop only holds tasks weakly, so an unreferenced task may be garbage
    collected before it runs) and await it on shutdown. Returns ``None`` when
    there is no running loop and the signal was emitted synchronously.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Callback invoked off the loop thread: emit synchronously. SoftSignal.emit
        # resolves coroutine listeners itself.
        try:
            signal.emit(**kwargs)
        except Exception as exc:
            logger.error("Signal emission failed: %s", exc, exc_info=True)
        return None
    task = loop.create_task(signal.emit_async(**kwargs))
    task.add_done_callback(_log_signal_task_failure)
    return task


def _run_event_sync(event: Any, call_kwargs: Dict[str, Any]) -> Any:
    """Run an async event to completion on a fresh event loop.

    Thread, process and remote executors only know how to call a *synchronous*
    function, but ``EventBase.__call__`` is a coroutine function. This module-
    level function (importable, hence picklable by reference) is what they
    execute: it owns a private loop for the lifetime of the event.
    """
    try:
        return asyncio.run(event(**call_kwargs))
    except asyncio.CancelledError:
        # CancelledError is a BaseException. Celery re-raises BaseException out
        # of its task machinery (it only handles Exception), which can take the
        # worker down, so the cancellation travels as an ordinary exception.
        raise TaskCancelledError("task cancelled") from None


def format_task_profiles(task_profiles: Any) -> Set[TaskType]:
    if isinstance(task_profiles, (TaskProtocol, TaskGroupingProtocol)):
        return {task_profiles}
    return cast(Set[TaskType], task_profiles)


@dataclass
class BaseFlow(ObjectIdentityMixin):
    """Base execution flow for Volnux pipelines.

    Manages task profiles, event initialization (with instance caching),
    executor lifecycle, communication bridging, and sub-phase suspension
    propagation. Subclasses must implement ``run()`` and
    ``get_flow_executor()``.
    """

    context: ExecutionContext
    task_profiles: Optional[Deque[TaskType]] = None
    enable_communication: InitVar[bool] = True

    # Internal state — not part of the public dataclass interface
    _comm_bridge: Optional[TaskCommunicationBridge] = field(
        default=None, init=False, repr=False
    )
    _initialized_events: Dict[str, Tuple["Event", Dict[str, Any]]] = field(
        default_factory=dict, init=False, repr=False
    )
    _flow_executors: list[BaseExecutor] = field(
        default_factory=list, init=False, repr=False
    )
    # Futures of the tasks this flow submitted, by task id (cancel fallback)
    _task_futures: Dict[str, "asyncio.Future[Any]"] = field(
        default_factory=dict, init=False, repr=False
    )
    _channel_warned: bool = field(default=False, init=False, repr=False)
    # Bridge kind of ``_comm_bridge`` (the first bridge created), and the extra
    # bridges needed when one flow dispatches to executors of different kinds.
    _comm_bridge_kind: Optional[str] = field(default=None, init=False, repr=False)
    _extra_bridges: Dict[str, TaskCommunicationBridge] = field(
        default_factory=dict, init=False, repr=False
    )
    # Which bridge carries each task's channel (absent: ``_comm_bridge``)
    _task_bridges: Dict[str, TaskCommunicationBridge] = field(
        default_factory=dict, init=False, repr=False
    )
    # Futures replaced in ``_task_futures`` by a resubmission of the same task id
    _superseded_futures: list = field(default_factory=list, init=False, repr=False)
    # Strong references to in-flight end-of-event signal emissions
    _signal_tasks: Set["asyncio.Future[Any]"] = field(
        default_factory=set, init=False, repr=False
    )
    _init_lock: Any = field(default_factory=threading.RLock, init=False, repr=False)

    def __post_init__(self, enable_communication: bool = True) -> None:
        self.task_profiles = cast(Deque[TaskType], self.context.task_profiles)
        self._enable_communication = enable_communication
        # The bridge kind depends on the executor, which is only resolved in
        # run(); the bridge is therefore created in _initialize_executor().
        super().__post_init__()

    @staticmethod
    def _bridge_type_for(
        executor: Union[Type[BaseExecutor], BaseExecutor],
    ) -> Optional[str]:
        """Communication bridge kind matching an executor, or None for no bridge."""
        executor_class = executor if inspect.isclass(executor) else type(executor)
        if issubclass(executor_class, ProcessPoolExecutor):
            return "process"
        if executor_class.__name__ == "CeleryExecutor":
            return "celery"
        # grpc / tcp / rpc workers have no channel transport: no bridge
        if executor_class.__name__ in {
            "GRPCExecutor",
            "RemoteExecutor",
            "XMLRPCExecutor",
        }:
            return None
        return "local"  # in-process: default (async) and thread executors

    def _ensure_comm_bridge(
        self, executor: Union[Type[BaseExecutor], BaseExecutor]
    ) -> Optional[TaskCommunicationBridge]:
        """Return the task communication bridge matching ``executor``.

        The first bridge created is ``_comm_bridge``; if the flow later
        dispatches to an executor of a different kind (e.g. a thread pool, then
        a process pool) a bridge of that kind is created alongside it, so a task
        never registers its channel on the wrong transport. One bridge per kind.

        The bridge is the control plane (pause/resume/cancel/status). Failing to
        build it must never stop the task itself, so errors are only logged.
        """
        if not self._enable_communication:
            return self._comm_bridge
        bridge_type = self._bridge_type_for(executor)
        if bridge_type is None:
            logger.debug("No task communication bridge for executor %s", executor)
            return None
        if self._comm_bridge is not None and self._comm_bridge_kind in (
            None,
            bridge_type,
        ):
            return self._comm_bridge
        if bridge_type in self._extra_bridges:
            return self._extra_bridges[bridge_type]
        try:
            bridge = create_communication_bridge(self.context, bridge_type)
        except Exception as exc:
            logger.warning(
                "Could not create the %r communication bridge: %s",
                bridge_type,
                exc,
                exc_info=True,
            )
            return None
        if self._comm_bridge is None:
            self._comm_bridge = bridge
            self._comm_bridge_kind = bridge_type
        else:
            self._extra_bridges[bridge_type] = bridge
        return bridge

    def _all_bridges(self) -> list:
        bridges = [self._comm_bridge] if self._comm_bridge is not None else []
        bridges.extend(self._extra_bridges.values())
        return bridges

    def _bridge_for_task(self, task_id: str) -> Optional[TaskCommunicationBridge]:
        return self._task_bridges.get(task_id) or self._comm_bridge

    def add_task_profile(self, task_profile: TaskType) -> None:
        if self.task_profiles is None:
            self.task_profiles = deque([task_profile])
        else:
            self.task_profiles.append(task_profile)

    def get_initialized_event(
        self, task_profile: TaskType
    ) -> Tuple["Event", Dict[str, Any]]:
        """Return a cached, fully configured event instance for the given task.

        Events are cached by task ID so that sub-phase cache, resource state,
        and retry policy survive across multiple access points within a single
        flow execution.
        """
        task_id = task_profile.get_id()
        # Re-entrant lock: flows can be driven from several threads, and two
        # callers must not build (and cache) two different events for one task.
        with self._init_lock:
            return self._get_or_build_event(task_profile, task_id)

    def _get_or_build_event(
        self, task_profile: TaskType, task_id: str
    ) -> Tuple["Event", Dict[str, Any]]:
        if task_id in self._initialized_events:
            return self._initialized_events[task_id]

        event_klass = task_profile.get_event_class()
        logger.info("Initializing '%s' (task_id=%s)", task_profile.event, task_id)

        event_init_args, event_call_args = build_event_arguments_from_pipeline(
            event_klass, self.context.pipeline
        )
        event_init_args = event_init_args or {}
        event_call_args = event_call_args or {}

        # Core context injection
        event_init_args["execution_context"] = self.context
        event_init_args["task_id"] = task_id
        event_init_args["sequence_number"] = task_profile.sequence_number

        if task_profile.options:
            event_init_args["options"] = task_profile.options

        # Data flow routing based on pipe type
        pointer_type = self._resolve_pointer_type(task_profile)

        if pointer_type == PipeType.POINTER:
            event_init_args["previous_result"] = (
                self.context.previous_context.get_results()
                if self.context.previous_context
                else EMPTY
            )
        elif pointer_type == PipeType.PIPE_POINTER:
            # Stream integration for |-> operator
            stream_key = task_profile.get_stream_key()
            group_name = f"{self.context.workflow_id}:{task_id}"
            event_init_args["stream_key"] = stream_key
            event_init_args["consumer_group"] = group_name
            event_init_args["checkpoint_manager"] = self.context.checkpoint_manager

        event = event_klass(**event_init_args)
        self._configure_event(event, task_profile)

        result = (event, event_call_args)
        self._initialized_events[task_id] = result
        return result

    @staticmethod
    def _resolve_pointer_type(task_profile: TaskType) -> PipeType:
        if task_profile.is_parallel_execution_node:
            parent = task_profile.get_first_task_in_parallel_execution_mode()
            return parent.get_pointer_to_task()
        return task_profile.get_pointer_to_task()

    def _configure_event(self, event: "Event", task_profile: TaskProtocol) -> None:
        """Configure retry policy without mutating shared state."""
        if not task_profile.options:
            return

        total_retries = task_profile.options.retry_attempts or 0
        if total_retries <= 1:
            return

        event_retry_policy = event.init_retry()
        if event_retry_policy:
            # Deep-copy to prevent cross-task policy leakage
            new_policy = deepcopy(event_retry_policy)
            new_policy.max_attempts = total_retries
            event.retry_policy = new_policy
        else:
            event.config_retry_policy(max_attempts=total_retries)

    @staticmethod
    def get_task_executor_from_options(
        task_profile: Union[TaskProtocol, TaskGroupingProtocol],
    ) -> Optional[Union[Type[BaseExecutor], BaseExecutor]]:
        if task_profile.options and task_profile.options.executor:
            return _GLOBAL_EXECUTOR_REGISTRY.get(task_profile.options.executor)
        return None

    @staticmethod
    def parse_executor_initialisation_configuration(
        executor: Type[BaseExecutor], execution_config: ExecutorInitializerConfig
    ) -> Dict[str, Any]:
        return get_function_call_args(executor.__init__, execution_config.to_dict())

    async def get_flow_executor(self, *args: Any, **kwargs: Any) -> Type[BaseExecutor]:
        raise NotImplementedError(
            "get_flow_executor() must be implemented by subclasses"
        )

    async def get_flow_executor_config(
        self, task_profile: TaskType
    ) -> ExecutorInitializerConfig:
        options_config = (
            task_profile.options.executor_config if task_profile.options else None
        )

        if isinstance(task_profile, TaskProtocol):
            # Uses cached event — no duplicate initialization
            event, _ = self.get_initialized_event(task_profile)
            execution_config = event.get_executor_initializer_config()
            if options_config:
                return execution_config.update(options_config)  # type: ignore[union-attr]
            return execution_config

        return options_config or ExecutorInitializerConfig()

    def _initialize_executor(
        self,
        executor: Union[Type[BaseExecutor], BaseExecutor],
        executor_config: ExecutorInitializerConfig,
    ) -> BaseExecutor:
        """Instantiate ``executor`` (a class), or pass a shared instance through.

        Only instances created here are tagged and later shut down by ``close()``.
        """
        self._ensure_comm_bridge(executor)

        if inspect.isclass(executor):
            config = self.parse_executor_initialisation_configuration(
                executor, executor_config
            )
            instance = executor(**config)
            instance._created_by_flow = True  # type: ignore[attr-defined]
            self._flow_executors.append(instance)
            return instance

        # Shared executor instance (e.g., ProcessPoolExecutor singleton)
        return executor

    @staticmethod
    def _should_shutdown_executor(executor: BaseExecutor) -> bool:
        # Only executors this flow created are shut down; a shared instance
        # handed in by the registry is never tagged and is left alone. (Process
        # pools used to be excluded here, leaking a pool of worker processes
        # on every dispatch.)
        return getattr(executor, "_created_by_flow", False)

    @staticmethod
    async def shutdown_executor(executor: BaseExecutor) -> None:
        if not isinstance(executor, BaseExecutor):
            logger.warning("Executor is not a BaseExecutor instance: %s", executor)
            return
        try:
            logger.debug("Shutting down executor %s", executor.__class__.__name__)
            await to_thread(executor.shutdown, wait=True)
            logger.debug("Executor %s shut down", executor.__class__.__name__)
        except Exception as e:
            logger.warning(
                "Error shutting down executor %s: %s", executor.__class__.__name__, e
            )

    async def _submit_event_to_executor(
        self,
        executor: BaseExecutor,
        event: "Event",
        event_call_kwargs: Dict[str, Any],
    ) -> asyncio.Future:
        logger.info(
            "Submitting event %s to executor %s", event, executor.__class__.__name__
        )

        await event_execution_start.emit_async(
            sender=self.context.__class__,
            event=event,
            execution_context=self.context,
        )

        call_kwargs = dict(event_call_kwargs or {})
        # Signals and snapshots read the call arguments back from the event
        event._call_args = call_kwargs

        bridge = self._ensure_comm_bridge(executor)
        channel = None
        try:
            # The event reaches its channel through itself, so attach it first
            channel = await self._register_channel(event, bridge)
            if channel is not None:
                event._command_channel = channel

            future = self._dispatch_to_executor(executor, event, call_kwargs)
            self._track_future(event._task_id, future, bridge if channel else None)
            if channel is not None and bridge is not None:
                await bridge.start_monitoring()
        except BaseException:
            # Never leave a started-but-never-finished event behind: detach the
            # channel and close the start/end signal pair before re-raising.
            if channel is not None:
                event._command_channel = None
            self._task_bridges.pop(event._task_id, None)
            self._schedule_end_signal(event)
            raise

        # Async-safe signal emission on completion
        future.add_done_callback(lambda fut: self._schedule_end_signal(event))

        logger.debug("Event submitted successfully; future: %s", future)
        return future

    def _track_future(
        self,
        task_id: str,
        future: "asyncio.Future[Any]",
        bridge: Optional[TaskCommunicationBridge],
    ) -> None:
        """Remember ``future`` for cancel/pause, keyed by task id.

        Resubmitting a task id (a retry, or parallel nodes sharing an id) must
        not orphan the earlier run: if it is still pending it is kept in
        ``_superseded_futures`` so ``cancel()``/``close()`` still reach it.
        """
        previous = self._task_futures.get(task_id)
        if previous is not None and previous is not future and not previous.done():
            logger.warning(
                "Task %s resubmitted while a previous run is still pending", task_id
            )
            self._superseded_futures.append(previous)
        self._task_futures[task_id] = future
        if bridge is not None:
            self._task_bridges[task_id] = bridge
        else:
            self._task_bridges.pop(task_id, None)

    def _schedule_end_signal(self, event: "Event") -> None:
        """Emit ``event_execution_end``, keeping the emission task referenced so
        it is neither garbage collected nor outlives ``close()``."""
        task = _emit_signal_sync(
            signal=event_execution_end,
            sender=self.context.__class__,
            event=event,
            execution_context=self.context,
        )
        if task is not None:
            self._signal_tasks.add(task)
            task.add_done_callback(self._signal_tasks.discard)

    async def _register_channel(
        self, event: "Event", bridge: Optional[TaskCommunicationBridge] = None
    ) -> Optional[Any]:
        """Register ``event`` with ``bridge`` and return its command channel.

        The control plane is optional: any failure (no distributed backend for
        a process/Celery task, a transport error...) is logged and the task runs
        without a channel, exactly as it did before channels existed.
        """
        if bridge is None:
            return None
        handle = TaskExecutionHandle(
            task_def_id=event._task_id,
            execution_id=uuid.uuid4().hex[:12],
            event_name=event.__class__.__name__,
            context_state_id=getattr(self.context, "state_id", None),
        )
        try:
            return await bridge.register_task(handle)
        except ChannelUnavailable as exc:
            if not self._channel_warned:  # one warning per flow, not per task
                self._channel_warned = True
                logger.warning(
                    "Running without a control channel (pause/resume/status "
                    "unavailable): %s",
                    exc,
                )
        except Exception as exc:
            logger.warning(
                "Could not register %s with the communication bridge: %s",
                handle.event_name,
                exc,
                exc_info=True,
            )
        return None

    @staticmethod
    def _dispatch_to_executor(
        executor: BaseExecutor, event: "Event", call_kwargs: Dict[str, Any]
    ) -> asyncio.Future:
        """Start ``event`` on ``executor`` and return an awaitable ``Future``.

        * Async-capable executors (``DefaultExecutor``) await the coroutine on
          the engine's own loop.
        * Everything else (thread / process pools, remote executors) is handed
          the synchronous ``_run_event_sync`` wrapper, which drives the coroutine
          on a private loop inside the worker.
        """
        if getattr(executor, "supports_coroutines", False):
            return executor.submit_coroutine(event, **call_kwargs)  # type: ignore[attr-defined,no-any-return]

        loop = asyncio.get_running_loop()
        return loop.run_in_executor(executor, _run_event_sync, event, call_kwargs)

    async def _map_events_to_executor(
        self,
        executor: BaseExecutor,
        event_execution_config: Dict["Event", Any],
    ) -> list[asyncio.Future]:
        """Submit events and return individual futures for exception inspection.

        Callers MUST inspect each future for SubPhaseSuspension before
        treating it as a generic error.
        """
        futures: list[asyncio.Future] = []
        for event, event_call_kwargs in event_execution_config.items():
            future = await self._submit_event_to_executor(
                executor, event, event_call_kwargs
            )
            futures.append(future)
        return futures

    @staticmethod
    def validate_executor_class_and_config(
        executor_class: Type[BaseExecutor],
        executor_config: ExecutorInitializerConfig,
    ) -> None:
        if isinstance(executor_class, Exception):
            raise RuntimeError(
                f"Failed to get executor class: {executor_class.__class__.__name__}: {executor_class}"
            )
        if isinstance(executor_config, Exception):
            raise ValueError(
                f"Invalid executor config: {executor_config.__class__.__name__}: {executor_config}"
            )

    async def run(self) -> asyncio.Future:
        raise NotImplementedError("run() must be implemented by subclasses")

    async def close(self) -> None:
        """Clean up executors, communication bridge, and event cache."""
        # Shutdown flow-owned executors concurrently
        shutdown_tasks = [
            self.shutdown_executor(ex)
            for ex in self._flow_executors
            if self._should_shutdown_executor(ex)
        ]
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        self._flow_executors.clear()

        # Cancel tasks still pending, then let end-of-event signals finish so
        # listeners are not called after the flow is gone.
        pending = [f for f in self._task_futures.values() if not f.done()]
        pending.extend(f for f in self._superseded_futures if not f.done())
        for future in pending:
            future.cancel()
        self._task_futures.clear()
        self._superseded_futures.clear()
        self._task_bridges.clear()
        if self._signal_tasks:
            await asyncio.gather(*list(self._signal_tasks), return_exceptions=True)
        self._signal_tasks.clear()

        # Clear event cache to release references
        self._initialized_events.clear()

        # Shutdown communication bridges
        bridges = self._all_bridges()
        self._extra_bridges.clear()
        self._comm_bridge = None
        self._comm_bridge_kind = None
        for bridge in bridges:
            try:
                await bridge.shutdown()
            except Exception as e:
                logger.warning(
                    "Error shutting down communication bridge: %s", e, exc_info=True
                )

    async def _command_all(self, command_type: CommandType) -> int:
        """Send ``command_type`` to every task this flow submitted.

        Returns the number of tasks the command reached through a channel.
        """
        if not self._all_bridges():
            return 0
        delivered = 0
        for task_id in list(self._task_futures):
            bridge = self._bridge_for_task(task_id)
            if bridge is None:
                continue
            command = TaskCommand(task_id=task_id, command_type=command_type)
            try:
                if await bridge.send_command(task_id, command):
                    delivered += 1
            except Exception as e:
                logger.warning(
                    "Error sending %s to task %s: %s",
                    command_type.name,
                    task_id,
                    e,
                    exc_info=True,
                )
        return delivered

    async def cancel(self, *args: Any, **kwargs: Any) -> None:
        """Cancel every task. A task without a channel (remote executor, no
        distributed backend) is cancelled through its future instead.

        Cancelling a future cannot interrupt a thread or process that is already
        running the event: without a delivered channel command the work keeps
        running and only its result is discarded.
        """
        for future in list(self._superseded_futures):
            if not future.done():
                future.cancel()
        for task_id, future in list(self._task_futures.items()):
            if future.done():
                continue
            delivered = False
            bridge = self._bridge_for_task(task_id)
            if bridge:
                try:
                    delivered = await bridge.send_command(
                        task_id,
                        TaskCommand(task_id=task_id, command_type=CommandType.CANCEL),
                    )
                except Exception as e:
                    logger.warning(
                        "Error cancelling task %s: %s", task_id, e, exc_info=True
                    )
            if not delivered:
                future.cancel()

    async def pause(self) -> int:
        """Ask every running task to hold before its next step."""
        return await self._command_all(CommandType.PAUSE)

    async def resume(self) -> int:
        """Release tasks held by ``pause``."""
        return await self._command_all(CommandType.RESUME)

    async def get_task_status(self, task_id: str) -> Any:
        bridge = self._bridge_for_task(task_id)
        if not bridge:
            return None
        try:
            return await bridge.get_status(task_id)
        except Exception as e:
            logger.warning(
                "Error getting status for task %s: %s", task_id, e, exc_info=True
            )
            return None

    async def pause_task(self, task_id: str) -> bool:
        bridge = self._bridge_for_task(task_id)
        if not bridge:
            return False
        try:
            return await bridge.send_command(
                task_id, TaskCommand(task_id=task_id, command_type=CommandType.PAUSE)
            )
        except Exception as e:
            logger.warning("Error pausing task %s: %s", task_id, e, exc_info=True)
            return False

    async def resume_task(self, task_id: str) -> bool:
        bridge = self._bridge_for_task(task_id)
        if not bridge:
            return False
        try:
            return await bridge.send_command(
                task_id, TaskCommand(task_id=task_id, command_type=CommandType.RESUME)
            )
        except Exception as e:
            logger.warning("Error resuming task %s: %s", task_id, e, exc_info=True)
            return False
