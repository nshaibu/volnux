import asyncio
import os
import traceback
import time
import typing
import uuid
from enum import Enum


class PipelineExecutionState(Enum):
    INITIALIZED = "initialized"
    INITIALIZING = "initializing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    FINISHED = "finished"


# Protocol keys that additional_data must not overwrite in lifecycle events
_PROTOCOL_KEYS = frozenset(
    {
        "message_type",
        "event_type",
        "wrapper_id",
        "pipeline_id",
        "process_id",
        "execution_state",
        "version",
    }
)


class PipelineWrapper:
    """
    Executes a Pipeline in a worker process and forwards its signals
    to the parent process via a multiprocessing Queue.

    Architecture
    ------------
    Each worker process is a completely separate Python interpreter with no
    shared memory. Signal listeners registered in the parent process do not
    exist in the worker. The wrapper bridges this by:

        1. Connecting to the requested soft signals inside the worker process.
        2. Forwarding each signal as a serialised dict onto signals_queue.
        3. The parent's _BatchProcessingMonitor reads the queue and re-emits
           each signal in the parent process so registered handlers fire there.

    Signal forwarding protocol
    --------------------------
    Each forwarded signal is a dict with:

        {
            "message_type": "pipeline_signal",
            "kwargs": {
                "sender": <sender class>,
                "signal": <SoftSignal instance>, ← required by construct_signal()
                ...original signal kwargs...
            },
            "wrapper_id": str,
            "timestamp":  float,
            "version":    "1",
        }

    The SoftSignal instance is included per handler (via make_handler closure) so
    that construct_signal() can import and re-emit it in the parent process. A
    single shared handler without the signal reference cannot distinguish which
    signal fired, making construct_signal() a no-op.

    Lifecycle events (message_type="wrapper_lifecycle") use a protected-key
    merge so that additional_data cannot accidentally overwrite protocol fields.

    Parameters
    ----------
    pipeline:
        The Pipeline instance to execute.
    focus_on_signals:
        Import strings of SoftSignal objects to intercept and forward.
    signals_queue:
        Multiprocessing Queue shared with _BatchProcessingMonitor.
    import_string_fn:
        Function to resolve a dotted import path to a Python object.
    logger:
        Logger to use. Passed explicitly because logging configuration
        may differ between parent and worker processes.
    wrapper_id:
        Optional unique identifier for this wrapper instance. Defaults
        to a UUID4 string — avoids exposing the worker PID.
    """

    def __init__(
        self,
        pipeline,
        *,
        focus_on_signals: typing.List[str],
        signals_queue,
        import_string_fn: typing.Callable,
        logger,
        wrapper_id: typing.Optional[str] = None,
    ):
        self.pipeline = pipeline
        self.focus_on_signals = focus_on_signals or []
        self.signals_queue = signals_queue
        self._import_string = import_string_fn
        self._logger = logger
        self.wrapper_id = wrapper_id or f"wrapper-{uuid.uuid4().hex[:12]}"

        self.execution_state = PipelineExecutionState.INITIALIZED
        self.start_time: typing.Optional[float] = None
        self.end_time: typing.Optional[float] = None
        self.exception: typing.Optional[Exception] = None

        # List of (signal_module, handler) tuples for clean disconnection
        self._connected_signals: typing.List[
            typing.Tuple[typing.Any, typing.Callable]
        ] = []

    @staticmethod
    def _now() -> float:
        return time.time()

    def _connect_signals(self) -> None:
        """
        Connect per-signal handlers for each requested soft signal.

        Each signal gets its own handler closure that captures the signal
        module so construct_signal() in the parent process can import and
        re-emit the correct signal.

        A shared single handler (original approach) cannot include the
        signal reference — construct_signal() requires it and silently
        no-ops without it.
        """
        if not self.focus_on_signals:
            return

        pipeline_id = getattr(self.pipeline, "id", None)
        wrapper_id = self.wrapper_id

        for signal_str in self.focus_on_signals:
            try:
                signal_module = self._import_string(signal_str)
            except Exception as exc:
                if self._logger:
                    self._logger.warning(
                        "Signal import failed: %s — %s", signal_str, exc
                    )
                continue

            # Closure captures signal_module, so construct_signal() can
            # re-emit the correct signal in the parent process.
            def _make_handler(
                sig_module: typing.Any,
                pid_str: typing.Optional[str],
                wid: str,
            ) -> typing.Callable:
                def handler(*args, **kwargs) -> None:
                    enhanced_kwargs = {
                        **kwargs,
                        # signal key is required by construct_signal()
                        "signal": sig_module,
                        "pipeline_id": pid_str,
                        "process_id": os.getpid(),
                        "timestamp": time.time(),
                        "wrapper_id": wid,
                    }
                    signal_data = {
                        "message_type": "pipeline_signal",
                        "args": args,
                        "kwargs": enhanced_kwargs,
                        "wrapper_id": wid,
                        "timestamp": time.time(),
                        "version": "1",
                    }
                    try:
                        # put_nowait — never block the worker process waiting
                        # for the parent to drain the queue. A dropped signal
                        # is preferable to a stalled worker.
                        self.signals_queue.put_nowait(signal_data)
                    except Exception as put_exc:
                        if self._logger:
                            self._logger.debug(
                                "Signal queue full — dropped %s: %s",
                                type(sig_module).__name__,
                                put_exc,
                            )

                return handler

            specific_handler = _make_handler(signal_module, pipeline_id, wrapper_id)
            try:
                signal_module.connect(listener=specific_handler, sender=None)
                self._connected_signals.append((signal_module, specific_handler))
            except Exception as exc:
                if self._logger:
                    self._logger.warning(
                        "Failed to connect signal %s: %s", signal_str, exc
                    )

    def _disconnect_signals(self) -> None:
        """
        Disconnect every handler that was connected in _connect_signals.

        Each entry in _connected_signals is a (signal_module, handler) pair —
        the exact handler that was passed to connect(). The original code
        disconnected a placeholder method that was never connected, leaving
        all signal handlers permanently attached.
        """
        for signal_module, handler in self._connected_signals:
            try:
                signal_module.disconnect(sender=None, listener=handler)
            except Exception as exc:
                if self._logger:
                    self._logger.debug("Error disconnecting signal handler: %s", exc)
        self._connected_signals.clear()

    def _send_wrapper_lifecycle_event(
        self,
        event_type: str,
        additional_data: typing.Optional[typing.Dict] = None,
    ) -> None:
        """
        Send a wrapper lifecycle event to the parent's signals_queue.

        Protocol keys (message_type, event_type, wrapper_id, etc.) are set
        first and protected — additional_data cannot overwrite them. This
        prevents callers from accidentally corrupting the event protocol when
        passing payload data like "execution_context" or "execution_duration".

        Uses put_nowait to avoid blocking the worker process. A missed lifecycle
        event is acceptable; a stalled worker is not.
        """
        message_data: typing.Dict[str, typing.Any] = {
            "message_type": "wrapper_lifecycle",
            "event_type": event_type,
            "wrapper_id": self.wrapper_id,
            "pipeline_id": getattr(self.pipeline, "id", None),
            "process_id": os.getpid(),
            "timestamp": self._now(),
            "execution_state": self.execution_state.value,
            "version": "1.0",
        }

        if additional_data:
            # Merge only non-protocol keys — protocol keys are immutable
            for key, value in additional_data.items():
                if key not in _PROTOCOL_KEYS:
                    message_data[key] = value

        try:
            self.signals_queue.put_nowait(message_data)
        except Exception as exc:
            if self._logger:
                self._logger.error(
                    "Failed to send wrapper lifecycle event %r: %s", event_type, exc
                )

    def _handle_pipeline_execution(self) -> None:
        """
        Execute the pipeline and emit start/end lifecycle events.

        Pipeline.start() is async. This method runs inside a synchronous
        worker process (ProcessPoolExecutor / Celery) — asyncio.run() creates
        a fresh event loop per invocation, which is the correct pattern.
        """
        try:
            self.execution_state = PipelineExecutionState.RUNNING
            if self._logger:
                self._logger.info(
                    "Starting pipeline execution: %s",
                    self.pipeline.__class__.__name__,
                    extra={"pipeline_id": getattr(self.pipeline, "id", None)},
                )

            self._send_wrapper_lifecycle_event(
                "pipeline_execution_start",
                {"pipeline": self.pipeline},
            )

            execution_context = asyncio.run(self.pipeline.start(force_rerun=True))

            if execution_context:
                latest_context = execution_context.get_latest_execution_context()
                if latest_context.execution_failed():
                    error_node = self.pipeline.get_first_error_execution_node()
                    failed_task = (
                        error_node.task.id
                        if error_node and hasattr(error_node, "task")
                        else "unknown"
                    )
                    self.execution_state = PipelineExecutionState.FAILED
                    if self._logger:
                        self._logger.error(
                            "Pipeline execution failed at task: %s",
                            failed_task,
                            extra={
                                "pipeline_id": getattr(self.pipeline, "id", None),
                                "failed_task": failed_task,
                            },
                        )
                    return

            self.execution_state = PipelineExecutionState.COMPLETED
            self._send_wrapper_lifecycle_event(
                "pipeline_execution_end",
                {
                    "execution_context": execution_context,
                    "success": True,
                },
            )
            if self._logger:
                self._logger.info(
                    "Pipeline execution completed successfully",
                    extra={"pipeline_id": getattr(self.pipeline, "id", None)},
                )

        except Exception as exc:
            self.exception = exc
            self.execution_state = PipelineExecutionState.FAILED

            if self._logger:
                self._logger.error(
                    "Pipeline execution failed: %s",
                    exc,
                    exc_info=True,
                    extra={
                        "wrapper_id": self.wrapper_id,
                        "pipeline_id": getattr(self.pipeline, "id", None),
                        "exception_type": type(exc).__name__,
                    },
                )

            self._send_wrapper_lifecycle_event(
                "wrapper_failed",
                {
                    "error_message": f"Wrapper execution failed: {exc}",
                    "error_type": type(exc).__name__,
                    "traceback": self._format_exception_traceback(exc),
                    "pipeline_state": self._get_pipeline_state_info(),
                },
            )

    def run(self) -> typing.Tuple[typing.Any, typing.Optional[Exception]]:
        """
        Execute the pipeline, returning ``(pipeline, exception)``.

        The return value is consumed by BatchPipeline._process_future via:
            data = fut.result()
            _BatchResult(*data)

        Execution flow:
            1. Send "wrapper_started" lifecycle event.
            2. Connect signal interceptors.
            3. Execute pipeline via _handle_pipeline_execution().
            4. Send "wrapper_finished" lifecycle event (always).
            5. Disconnect signal interceptors (always).

        _handle_pipeline_execution() has its own try/except and never
        re-raises. The outer try/except here guards only steps 1 and 2 —
        the connect phase — which can theoretically fail on import errors.
        Both layers do not duplicate each other: the outer layer does not
        call _handle_pipeline_execution() if signal setup fails.
        """
        self.start_time = self._now()

        try:
            self.execution_state = PipelineExecutionState.INITIALIZING
            self._send_wrapper_lifecycle_event("wrapper_started")

            if self._logger:
                self._logger.info(
                    "Pipeline wrapper started: %s",
                    self.wrapper_id,
                    extra={"pipeline_id": getattr(self.pipeline, "id", None)},
                )

            self._connect_signals()
            self._handle_pipeline_execution()

        except Exception as exc:
            # Only reached if _send_wrapper_lifecycle_event or _connect_signals
            # raises — _handle_pipeline_execution() is self-contained.
            if self.exception is None:
                self.exception = exc
            if self.execution_state not in (
                PipelineExecutionState.FAILED,
                PipelineExecutionState.COMPLETED,
            ):
                self.execution_state = PipelineExecutionState.FAILED

            if self._logger:
                self._logger.error("Wrapper setup failed: %s", exc, exc_info=True)
            self._send_wrapper_lifecycle_event(
                "wrapper_failed",
                {
                    "error_message": f"Wrapper setup failed: {exc}",
                    "error_type": type(exc).__name__,
                    "traceback": self._format_exception_traceback(exc),
                },
            )

        finally:
            self.end_time = self._now()
            execution_duration = (
                self.end_time - self.start_time if self.start_time else 0.0
            )

            # Preserve FAILED state — do not overwrite it with FINISHED.
            if self.execution_state != PipelineExecutionState.FAILED:
                self.execution_state = PipelineExecutionState.FINISHED

            self._send_wrapper_lifecycle_event(
                "wrapper_finished",
                {
                    "execution_duration": execution_duration,
                    "final_state": self.execution_state.value,
                    "had_exception": self.exception is not None,
                },
            )

            self._disconnect_signals()

            if self._logger:
                self._logger.info(
                    "Pipeline wrapper finished: %s (duration=%.2fs state=%s)",
                    self.wrapper_id,
                    execution_duration,
                    self.execution_state.value,
                    extra={"pipeline_id": getattr(self.pipeline, "id", None)},
                )

        return self.pipeline, self.exception

    @staticmethod
    def _format_exception_traceback(exception: Exception) -> str:
        """Serialise an exception with its full traceback as a string."""
        try:
            return "".join(
                traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                )
            )
        except Exception:
            return f"Failed to format traceback for: {exception}"

    def _get_pipeline_state_info(self) -> typing.Dict[str, typing.Any]:
        """Collect diagnostic pipeline state for error reporting."""
        try:
            return {
                "pipeline_id": getattr(self.pipeline, "id", None),
                "pipeline_class": self.pipeline.__class__.__name__,
                "execution_context_exists": self.pipeline.execution_context is not None,
                "has_cache": bool(getattr(self.pipeline, "_state", None)),
            }
        except Exception:
            return {"error": "Failed to gather pipeline state info"}
