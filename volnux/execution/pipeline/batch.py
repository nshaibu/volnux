import logging
import multiprocessing as mp
import queue as _queue
import threading
import time
import typing
import asyncio
from concurrent.futures import CancelledError, Future, ProcessPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from functools import partial

from .wrapper import PipelineWrapper
from ...config import VolnuxConfig
from ...exceptions import (
    ImproperlyConfigured,
    PipelineConfigurationError,
)
from ...typing import BatchProcessType
from ...utils import validate_batch_processor
from ...fields import InputDataField
from ...import_utils import import_string
from ...mixins import ObjectIdentityMixin
from ...signal.signals import (
    SoftSignal,
    batch_pipeline_finished,
    batch_pipeline_started,
    pipeline_execution_end,
    pipeline_execution_start,
    pipeline_metrics_updated,
)
from volnux.mixins.internal_metadata import InternalMetadataMixin

if typing.TYPE_CHECKING:
    from .pipeline import Pipeline

_ResultCallback = typing.Callable[
    ["_BatchResult"], typing.Union[None, typing.Awaitable[None]]
]

logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()


class _BatchResult(typing.NamedTuple):
    pipeline: typing.Any  # Pipeline instance (or None on failure)
    exception: typing.Optional[Exception]


class BatchPipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


@dataclass
class PipelineExecutionMetrics:
    """Execution metrics for a BatchPipeline run."""

    total_pipelines: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0
    active: int = 0
    start_time: typing.Optional[float] = None
    end_time: typing.Optional[float] = None
    execution_durations: typing.List[float] = field(default_factory=list)
    errors: typing.List[typing.Dict[str, typing.Any]] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        total = self.completed + self.failed
        return (self.completed / total * 100) if total > 0 else 0.0

    @property
    def average_duration(self) -> float:
        return (
            sum(self.execution_durations) / len(self.execution_durations)
            if self.execution_durations
            else 0.0
        )

    @property
    def total_duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return time.time() - self.start_time
        return 0.0

    @property
    def completion_rate(self) -> float:
        return (
            ((self.completed + self.failed) / self.total_pipelines * 100)
            if self.total_pipelines > 0
            else 0.0
        )


class _BatchProcessingMonitor(threading.Thread):
    """
    Background thread that consumes signals from the inter-process queue
    and re-emits them in the main process so signal handlers registered
    by the caller receive events from worker pipelines.

    Signal listener lifecycle
    -------------------------
    Listeners are connected in ``_setup_signal_listeners()`` and disconnected
    in ``_cleanup_signal_listeners()``. Both use ``sender=None`` (catch-all)
    for pipeline lifecycle signals whose sender is the worker class — we
    cannot predict which concrete Pipeline subclass the worker uses.

    Thread safety
    -------------
    ``active_pipelines``, ``pipeline_start_times``, and ``metrics`` are
    shared between signal handler calls which may arrive on any thread.
    All mutations are protected by ``_metrics_lock``.

    Shutdown
    --------
    ``shutdown()`` is idempotent — the ``_shutdown_called`` flag prevents
    double emission of ``batch_pipeline_finished`` and double-disconnect.
    ``run()``'s ``finally`` block calls ``shutdown()`` so cleanup always
    runs regardless of how the thread exits.
    """

    def __init__(
        self,
        batch_pipeline: "BatchPipeline",
        enable_metrics: bool = True,
    ):
        super().__init__(name=f"BatchMonitor-{batch_pipeline.id}", daemon=True)

        self.batch = batch_pipeline
        self.metrics = PipelineExecutionMetrics() if enable_metrics else None

        # Protected by _metrics_lock — written from signal handler threads
        self._metrics_lock = threading.Lock()
        self.active_pipelines: typing.Set[str] = set()
        self.pipeline_start_times: typing.Dict[str, float] = {}

        self._shutdown_flag = threading.Event()
        self._shutdown_called = False
        self._signals_connected = False
        self._batch_started_emitted = False

        self._setup_signal_listeners()

    def _setup_signal_listeners(self) -> None:
        """
        Connect signal listeners. Both connect and disconnect use sender=None
        so they match — the pipeline class running inside a worker process is
        not the same class reference as self.batch.__class__ in the main process.
        """
        try:
            pipeline_execution_start.connect(
                sender=None, listener=self._on_pipeline_started
            )
            pipeline_execution_end.connect(
                sender=None, listener=self._on_pipeline_ended
            )
            pipeline_metrics_updated.connect(
                sender=None, listener=self._on_metrics_updated
            )
            batch_pipeline_started.connect(sender=None, listener=self._on_batch_started)
            batch_pipeline_finished.connect(
                sender=None, listener=self._on_batch_finished
            )
            self._signals_connected = True
            logger.debug("BatchMonitor: signal listeners connected")
        except Exception as exc:
            logger.warning("BatchMonitor: failed to setup signal listeners: %s", exc)

    def _cleanup_signal_listeners(self) -> None:
        """
        Disconnect signal listeners. Uses sender=None to match the connection
        made in _setup_signal_listeners — mismatched senders cause silent
        no-ops that leave listeners permanently attached.
        """
        if not self._signals_connected:
            return
        try:
            pipeline_execution_start.disconnect(
                sender=None, listener=self._on_pipeline_started
            )
            pipeline_execution_end.disconnect(
                sender=None, listener=self._on_pipeline_ended
            )
            pipeline_metrics_updated.disconnect(
                sender=None, listener=self._on_metrics_updated
            )
            batch_pipeline_started.disconnect(
                sender=None, listener=self._on_batch_started
            )
            batch_pipeline_finished.disconnect(
                sender=None, listener=self._on_batch_finished
            )
            self._signals_connected = False
            logger.debug("BatchMonitor: signal listeners disconnected")
        except Exception as exc:
            logger.debug("BatchMonitor: error cleaning up signal listeners: %s", exc)

    def _emit_batch_started(self) -> None:
        if self._batch_started_emitted or self.metrics is None:
            return
        batch_pipeline_started.emit(
            sender=self.batch.__class__,
            batch=self.batch,
            total_pipelines=self.metrics.total_pipelines,
            timestamp=time.time(),
        )
        self._batch_started_emitted = True

    def _emit_batch_finished(self) -> None:
        if self.metrics is None:
            return
        batch_pipeline_finished.emit(
            sender=self.batch.__class__,
            batch=self.batch,
            metrics=self.metrics,
            success_rate=self.metrics.success_rate,
            total_duration=self.metrics.total_duration,
            timestamp=time.time(),
        )

    def _emit_metrics_updated(self) -> None:
        if self.metrics is None:
            return
        pipeline_metrics_updated.emit(
            sender=self.batch.__class__,
            batch_id=self.batch.id,
            metrics=self.metrics,
            active_count=self.metrics.active,
            completion_rate=self.metrics.completion_rate,
            timestamp=time.time(),
        )

    def _on_batch_started(self, sender, **kwargs) -> None:
        with self._metrics_lock:
            if self.metrics and not self.metrics.start_time:
                self.metrics.start_time = time.time()
                self.metrics.total_pipelines = kwargs.get("total_pipelines", 1)
        logger.info(
            "Batch started: %s — expected %d pipeline(s)",
            self._get_sender_name(sender),
            self.metrics.total_pipelines if self.metrics else 0,
            extra={"batch_id": self.batch.id},
        )

    def _on_batch_finished(self, sender, **kwargs) -> None:
        with self._metrics_lock:
            if self.metrics and not self.metrics.end_time:
                self.metrics.end_time = time.time()
        if self.metrics:
            logger.info(
                "Batch finished: %s — completed=%d/%d failed=%d duration=%.2fs",
                self._get_sender_name(sender),
                self.metrics.completed,
                self.metrics.total_pipelines,
                self.metrics.failed,
                self.metrics.total_duration,
                extra={"batch_id": self.batch.id},
            )

    def _on_pipeline_started(self, sender, **kwargs) -> None:
        pipeline = kwargs.get("pipeline")
        if pipeline is None:
            return

        # pipeline may be a Pipeline instance or a string ID — normalise
        pipeline_key = getattr(pipeline, "id", str(pipeline))

        with self._metrics_lock:
            self.active_pipelines.add(pipeline_key)
            self.pipeline_start_times[pipeline_key] = kwargs.get(
                "timestamp", time.time()
            )
            if self.metrics:
                self.metrics.started += 1
                self.metrics.active = len(self.active_pipelines)

        self._emit_metrics_updated()
        logger.info(
            "Pipeline started: %s from %s — active=%d",
            pipeline_key,
            self._get_sender_name(sender),
            len(self.active_pipelines),
            extra={"pipeline_id": pipeline_key},
        )

    def _on_pipeline_ended(self, sender, **kwargs) -> None:
        execution_context = kwargs.get("execution_context")
        if not execution_context:
            logger.warning("Pipeline ended signal received with no execution_context")
            return

        pipeline = getattr(execution_context, "pipeline", None)
        if pipeline is None:
            logger.warning(
                "execution_context.pipeline is None — cannot record completion"
            )
            return

        pipeline_key = getattr(pipeline, "id", str(pipeline))

        # Determine success before acquiring the lock — can call methods
        success = True
        if hasattr(execution_context, "get_latest_execution_context"):
            try:
                latest = execution_context.get_latest_execution_context()
                success = not latest.execution_failed()
            except Exception as exc:
                logger.debug("Could not get latest context: %s", exc)
                success = not kwargs.get("exception")
        else:
            success = not kwargs.get("exception")

        with self._metrics_lock:
            self.active_pipelines.discard(pipeline_key)
            start_time = self.pipeline_start_times.pop(pipeline_key, None)
            duration = None
            if start_time is not None:
                duration = kwargs.get("timestamp", time.time()) - start_time

            if self.metrics:
                if duration is not None:
                    self.metrics.execution_durations.append(duration)
                if success:
                    self.metrics.completed += 1
                else:
                    self.metrics.failed += 1
                self.metrics.active = len(self.active_pipelines)

        self._emit_metrics_updated()

        duration_str = f" — {duration:.2f}s" if duration is not None else ""
        logger.info(
            "Pipeline %s: %s from %s%s",
            "completed" if success else "failed",
            pipeline_key,
            self._get_sender_name(sender),
            duration_str,
            extra={
                "pipeline_id": pipeline_key,
                "success": success,
                "duration": duration,
            },
        )

    def _on_metrics_updated(self, sender, **kwargs) -> None:
        logger.debug(
            "Metrics updated — active=%s completion_rate=%s ts=%s",
            kwargs.get("active_count"),
            kwargs.get("completion_rate"),
            kwargs.get("timestamp"),
        )

    def shutdown(self, timeout: float = 5.0) -> None:
        """
        Emit batch_finished, disconnect listeners, set shutdown flag.

        Idempotent — safe to call multiple times. Prevents double-emission
        of batch_pipeline_finished and double-disconnect errors.
        """
        if self._shutdown_called:
            return
        self._shutdown_called = True

        self._emit_batch_finished()
        self._cleanup_signal_listeners()
        self._shutdown_flag.set()

    def run(self) -> None:
        """
        Consume signals from the inter-process queue and re-emit them.

        Queue protocol:
            None sentinel   → exit the loop
            dict            → signal or lifecycle message to process

        queue.Empty is the normal idle case — not an error. Only real
        processing exceptions are logged.

        task_done() is called after every successful get() so queue.join()
        works correctly if the caller uses it.
        """
        if self.metrics:
            with self._metrics_lock:
                self.metrics.start_time = time.time()
                self.metrics.total_pipelines = (
                    len(self.batch._field_batch_op_map)
                    if self.batch._field_batch_op_map
                    else 1
                )
            self._emit_batch_started()

        try:
            while not self._shutdown_flag.is_set():
                self.batch.check_memory_usage()

                try:
                    signal_data = self.batch.signals_queue.get(timeout=1.0)
                except _queue.Empty:
                    # Normal idle — no signal in this interval
                    continue

                # task_done() must be called once per successful get()
                # so queue.join() works correctly for callers that need it.
                try:
                    if signal_data is None:
                        # None is the stop sentinel — exit the loop
                        break

                    message_type = signal_data.get("message_type")

                    if message_type == "pipeline_signal":
                        self.construct_signal(signal_data)
                    elif message_type == "wrapper_lifecycle":
                        self._handle_wrapper_lifecycle_event(signal_data)
                    else:
                        # Legacy message format — attempt signal construction
                        self.construct_signal(signal_data)

                except Exception as exc:
                    # Processing errors are logged but do not stop the monitor —
                    # a bad signal message should not abort the batch
                    logger.warning(
                        "BatchMonitor: error processing signal %s: %s",
                        signal_data.get("message_type", "unknown"),
                        exc,
                        exc_info=True,
                    )
                finally:
                    if self.batch.signals_queue:
                        self.batch.signals_queue.task_done()

        finally:
            # Always shut down cleanly, regardless of how the loop exits.
            # shutdown() is idempotent — safe to call if already called externally.
            self.shutdown()

    @staticmethod
    def construct_signal(signal_data: typing.Dict) -> None:
        """Re-emit a signal that was serialised from a worker process."""
        try:
            data = signal_data.get("kwargs", {})
            sender = data.pop("sender", None)
            signal: SoftSignal = data.pop("signal", None)

            if sender and signal:
                parent_signal: SoftSignal = import_string(signal.__instance_import_str__)  # type: ignore
                parent_signal.emit(sender=sender, **data)
        except Exception:
            logger.exception(
                "BatchMonitor: failed to reconstruct signal from %s",
                signal_data,
            )

    def _handle_wrapper_lifecycle_event(self, message_data: typing.Dict) -> None:
        """Handle wrapper-specific lifecycle events from worker processes."""
        event_type = message_data.get("event_type")
        wrapper_id = message_data.get("wrapper_id")
        pipeline_id = message_data.get("pipeline_id")

        if event_type == "pipeline_execution_start":
            pipeline_execution_start.emit(
                sender=self.batch.__class__,
                pipeline=message_data.get("pipeline"),
            )
        elif event_type == "pipeline_execution_end":
            pipeline_execution_end.emit(
                sender=self.batch.__class__,
                execution_context=message_data.get("execution_context"),
            )
        elif event_type == "wrapper_finished":
            duration = message_data.get("execution_duration")
            logger.debug(
                "Wrapper %s finished%s",
                wrapper_id,
                f" in {duration:.2f}s" if duration else "",
                extra={"wrapper_id": wrapper_id, "pipeline_id": pipeline_id},
            )
        elif event_type == "wrapper_failed":
            error_message = message_data.get("error_message", "unknown error")
            logger.error(
                "Wrapper %s failed: %s",
                wrapper_id,
                error_message,
                extra={"wrapper_id": wrapper_id, "pipeline_id": pipeline_id},
            )
        else:
            logger.debug(
                "Wrapper lifecycle event: %s (wrapper=%s pipeline=%s)",
                event_type,
                wrapper_id,
                pipeline_id,
            )

    def get_current_metrics(self) -> typing.Optional[PipelineExecutionMetrics]:
        """Return a snapshot of current execution metrics. Thread-safe."""
        with self._metrics_lock:
            return self.metrics

    @staticmethod
    def _get_sender_name(sender) -> str:
        if sender is None:
            return "Unknown"
        if hasattr(sender, "__name__"):
            return sender.__name__
        if hasattr(sender, "__class__"):
            return sender.__class__.__name__
        return str(sender)


class BatchPipeline(ObjectIdentityMixin, InternalMetadataMixin):
    """
    Executes a Pipeline template across multiple input batches using
    a process pool, with inter-process signal monitoring.

    Class attributes
    ----------------
    pipeline_template:   Pipeline subclass to execute for each batch item.
    listen_to_signals:   Signal import strings are forwarded from worker processes.
    max_workers:         Process pool size (default: from config).
    memory_limit:        Absolute memory limit in bytes for the pool.
    max_memory_percent:  Trigger batch size reduction above this % usage.

    Architecture
    ------------
                 Main thread (caller)
                 ┌──────────────────────────────────────────┐
                 │ BatchPipeline.execute()                  │
                 │   ├─ _gather_and_init_field_batch_iterators()
                 │   ├─ mp.Manager().Queue()  → signals_queue
                 │   ├─ _BatchProcessingMonitor.start()     │
                 │   └─ ProcessPoolExecutor                 │
                 │        ├─ submit(pipeline_1) → future_1  │──► Worker process
                 │        ├─ submit(pipeline_2) → future_2  │──► Worker process
                 │        └─ ...done callback per future    │
                 └──────────────────────────────────────────┘

                 Monitor thread (_BatchProcessingMonitor)
                 ┌──────────────────────────────────────────┐
                 │ run()                                    │
                 │   └─ signals_queue.get() loop            │
                 │        └─ construct_signal() / lifecycle │
                 └──────────────────────────────────────────┘

                 Worker processes (_pipeline_executor)
                 ┌──────────────────────────────────────────┐
                 │ PipelineWrapper.run()                    │
                 │   └─ signals → signals_queue (via IPC)   │
                 └──────────────────────────────────────────┘

    Usage
    -----
    class MyBatch(BatchPipeline):
        pipeline_template = MyPipeline
        max_workers = 4

    batch = MyBatch(items=my_item_list)
    batch.execute()
    results = batch.results
    """

    pipeline_template: typing.Type["Pipeline"] = None
    listen_to_signals: typing.List[str] = SoftSignal.registered_signals()
    __signature__ = None
    max_workers: int = None
    memory_limit: int = None
    max_memory_percent: float = 90.0

    def __init__(self, *args, **kwargs):
        from .pipeline import Pipeline

        super().__init__(*args, **kwargs)

        # self.lock = mp.Lock()
        # self.results: typing.List[_BatchResult] = []
        self._async_lock = asyncio.Lock()
        self.status: BatchPipelineStatus = BatchPipelineStatus.PENDING

        pipeline_template = self.get_pipeline_template()

        if pipeline_template is None:
            raise ImproperlyConfigured(
                "No pipeline_template set. Subclass BatchPipeline and set "
                "pipeline_template to a Pipeline subclass."
            )
        if not issubclass(pipeline_template, Pipeline):
            raise ImproperlyConfigured(
                f"pipeline_template must be a Pipeline subclass, "
                f"got {pipeline_template!r}."
            )

        template_cls = pipeline_template.construct_call_signature()
        self.__signature__ = self.__signature__ or template_cls.__signature__
        bounded_args = self.__signature__.bind(*args, **kwargs)

        for field_name, value in bounded_args.arguments.items():
            setattr(self, field_name, value)

        # Batch operator maps — built lazily in execute()
        self._field_batch_op_map: typing.Dict[InputDataField, typing.Iterator] = {}
        self._configured_pipelines: typing.Set["Pipeline"] = set()
        self._configured_pipelines_count: int = 0

        self._signals_queue: typing.Optional[mp.Queue] = None
        self._monitor_thread: typing.Optional[_BatchProcessingMonitor] = None

        # Per-instance batch size overrides — avoids mutating class-level field objects
        self._instance_batch_sizes: typing.Dict[str, int] = {}

    def get_pipeline_template(self):
        return self.pipeline_template

    def get_fields(self):
        yield from self.get_pipeline_template().get_fields()

    @property
    def signals_queue(self) -> typing.Optional[mp.Queue]:
        return self._signals_queue

    def check_memory_usage(self) -> None:
        """Monitor memory usage and reduce batch sizes if above threshold."""
        import psutil

        if psutil.Process().memory_percent() > self.max_memory_percent:
            self._adjust_batch_size()

    def _adjust_batch_size(self) -> None:
        """
        Reduce effective batch sizes by 20% when memory pressure is detected.

        Uses per-instance overrides rather than mutating the class-level
        InputDataField objects — which are shared across all BatchPipeline
        instances of the same template class.
        """
        for field_obj in self._field_batch_op_map:
            fname = field_obj.name
            current_size = self._instance_batch_sizes.get(fname, field_obj.batch_size)
            new_size = max(1, int(current_size * 0.8))
            self._instance_batch_sizes[fname] = new_size
        logger.info("BatchPipeline: batch sizes reduced (instance=%s)", self.id)

    def _get_effective_batch_size(self, field_obj) -> int:
        """Return the effective batch size for a field, respecting instance overrides."""
        return self._instance_batch_sizes.get(field_obj.name, field_obj.batch_size)

    @staticmethod
    def _validate_batch_processor(batch_processor: BatchProcessType) -> None:
        is_iterable = validate_batch_processor(batch_processor)
        if not is_iterable:
            raise ImproperlyConfigured(
                "Batch processor must be iterable and produce generators."
            )

    def _initialise_template(self, **kwargs) -> "Pipeline":
        template = self.get_pipeline_template()
        if template is None:
            raise ImproperlyConfigured(
                "No pipeline template set. Subclass BatchPipeline and set 'pipeline_template'"
            )
        pipeline = template(**kwargs)
        for key, value in self.__dict__.get(self._INTERNAL_KEY, {}).items():
            try:
                pipeline.update_internal(key, value)
            except KeyError:
                continue

        return pipeline

    def _gather_field_batch_methods(
        self,
        field_obj: InputDataField,
        batch_processor: BatchProcessType,
    ) -> None:
        self._validate_batch_processor(batch_processor)
        batch_size = self._get_effective_batch_size(field_obj)
        if field_obj not in self._field_batch_op_map:
            self._field_batch_op_map[field_obj] = batch_processor(
                getattr(self, field_obj.name), batch_size
            )
        else:
            self._field_batch_op_map[field_obj] = batch_processor(
                self._field_batch_op_map[field_obj], batch_size
            )

    def _gather_and_init_field_batch_iterators(self) -> None:
        for field_name, field_obj in self.get_fields():
            batch_size = self._get_effective_batch_size(field_obj)
            if field_obj.has_batch_operation:
                self._validate_batch_processor(field_obj.batch_processor)
                self._field_batch_op_map[field_obj] = field_obj.batch_processor(
                    getattr(self, field_name, []), batch_size
                )
            method_name = f"{field_name}_batch"
            if hasattr(self, method_name):
                self._gather_field_batch_methods(field_obj, getattr(self, method_name))

    @staticmethod
    def _convert_value_to_field_data_type(field_obj: InputDataField, value: typing.Any):
        if value is None:
            return None
        value_dtype = type(value)
        if value_dtype in field_obj.data_type:
            return value
        return field_obj.data_type[0](value)

    def _execute_field_batch_processors(
        self,
        batch_processor_map: typing.Dict[InputDataField, typing.Iterator],
    ) -> typing.Iterator[typing.Dict[str, typing.Any]]:
        """
        Yield one kwargs dict per batch step, advancing each field iterator.

        Yields until ALL field iterators are exhausted. Fields that exhaust
        before others yield None for their remaining steps.

        Note: A fresh kwargs dict is yielded on each iteration — callers
        should not hold references across iterations.
        """
        while batch_processor_map:
            all_exhausted = True
            kwargs = {}
            consumed = []  # fields that are now exhausted

            for field_obj, batch_operation in batch_processor_map.items():
                try:
                    value = next(batch_operation)
                    all_exhausted = False  # at least one iterator still has items
                except StopIteration:
                    value = None
                    consumed.append(field_obj)

                value = self._convert_value_to_field_data_type(field_obj, value)
                kwargs[field_obj.name] = value

            # Yield a COPY of kwargs — callers must not hold references
            yield dict(kwargs)

            if all_exhausted:
                break

            # Remove exhausted iterators so we don't keep yielding None for them
            for field_obj in consumed:
                batch_processor_map.pop(field_obj, None)

    async def execute(self, on_result: _ResultCallback) -> typing.Dict[str, int]:
        """
        Configure and execute all pipeline batches.

        Args:
            on_result: Callback invoked for **each** ``_BatchResult`` as it
                       completes. Must be thread-safe — it is called from the
                       thread that resolves each process-pool future.

        Returns:
            Summary dict once all work is complete::

                {"completed": int, "failed": int, "total": int}
        """
        if not callable(on_result):
            raise TypeError("on_result must be a callable.")

        _completed = 0
        _failed = 0
        _summary_lock = asyncio.Lock()

        async def _tally_and_forward(batch_result: _BatchResult) -> None:
            nonlocal _completed, _failed
            async with _summary_lock:
                if batch_result.exception is None:
                    _completed += 1
                else:
                    _failed += 1
            on_result(batch_result)

        if not self._field_batch_op_map:
            self._gather_and_init_field_batch_iterators()

        non_batch_kwargs = self._prepare_args_for_non_batch_fields()
        self.status = BatchPipelineStatus.RUNNING

        try:
            if not self._field_batch_op_map:
                # Non-batch path — single pipeline, awaited directly
                self._configured_pipelines_count += 1
                pipeline = self._initialise_template(**non_batch_kwargs)
                try:
                    await pipeline.start(force_rerun=True)
                    await _tally_and_forward(_BatchResult(pipeline, exception=None))
                except Exception as exc:
                    logger.error(
                        "Non-batch pipeline execution failed: %s", exc, exc_info=True
                    )
                    await _tally_and_forward(_BatchResult(None, exception=exc))
            else:
                await self._execute_batch(non_batch_kwargs, _tally_and_forward)

            if self._configured_pipelines_count == 0:
                raise PipelineConfigurationError(
                    message=(
                        f"Batch execution of '{self.pipeline_template}' produced no pipelines. "
                        "Check that at least one batch iterator yields items."
                    ),
                    code="not_configured_pipeline",
                )

            self.status = BatchPipelineStatus.FINISHED

        except Exception as exc:
            self.status = BatchPipelineStatus.FAILED
            logger.error("BatchPipeline.execute() failed: %s", exc, exc_info=True)
            raise

        async with _summary_lock:
            return {
                "completed": _completed,
                "failed": _failed,
                "total": self._configured_pipelines_count,
            }

    async def _execute_batch(
        self,
        non_batch_kwargs: typing.Dict[str, typing.Any],
        on_result: typing.Callable[["_BatchResult"], typing.Awaitable[None]],
    ) -> None:
        manager = mp.Manager()
        mp_context = mp.get_context("spawn")

        self._signals_queue = manager.Queue()
        self._monitor_thread = _BatchProcessingMonitor(self)

        loop = asyncio.get_running_loop()

        try:
            self._monitor_thread.start()  # non-blocking — just spawns the OS thread

            pending: typing.List[asyncio.Future] = []

            with ProcessPoolExecutor(
                max_workers=self.max_workers or conf.MAX_BATCH_PROCESSING_WORKERS,
                mp_context=mp_context,
            ) as executor:
                for kwargs in self._execute_field_batch_processors(
                    self._field_batch_op_map
                ):
                    if not any(v is not None for v in kwargs.values()):
                        continue

                    merged_kwargs = {**kwargs, **non_batch_kwargs}
                    pipeline = self._initialise_template(**merged_kwargs)

                    cf_future = executor.submit(
                        self._pipeline_executor,
                        pipeline=pipeline,
                        focus_on_signals=self.listen_to_signals,
                        signals_queue=self._signals_queue,
                    )
                    self._configured_pipelines_count += 1
                    async_future = asyncio.wrap_future(cf_future, loop=loop)
                    pending.append(
                        asyncio.ensure_future(
                            self._handle_future(async_future, on_result)
                        )
                    )

            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        except Exception as exc:
            logger.error(
                "Error during batch pipeline execution: %s", exc, exc_info=True
            )
            raise
        finally:
            if self._signals_queue is not None:
                self._signals_queue.put(None)
            if self._monitor_thread is not None:
                # join() blocks until the monitor thread exits — offload it
                # so the event loop remains responsive during the wait.
                await asyncio.to_thread(self._monitor_thread.join, 30.0)
                if self._monitor_thread.is_alive():
                    logger.warning(
                        "BatchMonitor thread did not exit within 30s — "
                        "signals may have been dropped"
                    )

    @staticmethod
    async def _handle_future(
        async_future: "asyncio.Future[typing.Any]", on_result: _ResultCallback
    ) -> None:
        """
        Await a single process-pool future and forward its outcome to ``on_result``.
        """
        pipeline = None
        exception = None

        try:
            data = await async_future
            pipeline, exception = (
                data
                if isinstance(data, (tuple, list)) and len(data) == 2
                else (data, None)
            )
        except CancelledError as exc:
            logger.warning("Pipeline future was cancelled: %s", exc)
            exception = exc
        except TimeoutError as exc:
            logger.error("Pipeline future timed out: %s", exc)
            exception = exc
        except Exception as exc:
            logger.error("Pipeline future failed: %s", exc, exc_info=True)
            exception = exc

        result = on_result(_BatchResult(pipeline, exception=exception))
        if asyncio.iscoroutine(result):
            await result
        return result

    def _prepare_args_for_non_batch_fields(self) -> typing.Dict[str, typing.Any]:
        args = {}
        template = self.get_pipeline_template()
        for field_name, _ in template.get_non_batch_fields():
            args[field_name] = getattr(self, field_name, None)
        return args

    @staticmethod
    def _pipeline_executor(
        pipeline: "Pipeline",
        focus_on_signals: typing.List[str],
        signals_queue: mp.Queue,
    ):
        wrapper = PipelineWrapper(
            pipeline,
            focus_on_signals=focus_on_signals,
            signals_queue=signals_queue,
            import_string_fn=import_string,
            logger=logger,
        )
        return wrapper.run()

    async def close(self, timeout: float = 5.0) -> None:
        """Release resources. Safe to call multiple times."""
        try:
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.shutdown(timeout=timeout)
                await asyncio.to_thread(self._monitor_thread.join, timeout)
        except Exception as exc:
            logger.warning("BatchPipeline.close() error: %s", exc)

    async def __aenter__(self) -> "BatchPipeline":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # __del__ is intentionally removed.
    # Calling threading.Thread.join() from __del__ risks deadlocking during
    # garbage collection or interpreter shutdown. Use close() or the context
    # manager instead. If the process exits without close() being called,
    # daemon=True on the monitor thread ensures it does not prevent exit.
