import asyncio
import inspect
import logging
import threading
import typing
import weakref
from inspect import Parameter, Signature

from volnux.mixins import ObjectIdentityMixin
from volnux.concurrency.async_utils import as_coroutine

logger = logging.getLogger(__name__)

#: Any callable accepting the signal's keyword arguments. May be sync or async.
Listener = typing.Callable[..., typing.Any]
#: A list of ``(listener, response)`` pairs returned by ``emit``.
Responses = typing.List[typing.Tuple[typing.Any, typing.Any]]

# Strong references to fire-and-forget tasks scheduled from a sync ``emit``
# running inside an event loop (the loop only keeps weak references to tasks).
_background_tasks: typing.Set["asyncio.Future[typing.Any]"] = set()


class GenericSender:
    """Wildcard sender key: listeners connected with ``sender=None``."""


def get_signal(
    signal_name: str, module_path: str = "volnux.signal.signals"
) -> typing.Optional["SoftSignal"]:
    """
    Retrieve a registered soft signal by its fully qualified name.

    Args:
        signal_name (str): signal name, e.g. "pipeline_pre_init" or "pipeline_post_init".
        module_path (str, optional): The module path where the signal is expected to be found. Defaults to "volnux.signal.signals".

    Returns:
        typing.Optional[SoftSignal]: The registered signal if found, otherwise None.
    """

    return SoftSignal._registered_signal.get(f"{module_path}.{signal_name}")


def _normalise_sender(sender: typing.Any) -> typing.Any:
    """Map ``None`` to the wildcard sender and require a hashable sender."""
    if sender is None:
        return GenericSender
    try:
        hash(sender)
    except TypeError:
        raise TypeError(
            "Signal senders must be hashable, got %r." % type(sender).__name__
        ) from None
    return sender


class SoftSignal(ObjectIdentityMixin):
    """A lightweight signal/dispatcher holding *weak* references to listeners.

    Listeners are dropped when garbage collected, so lambdas, closures and
    ``functools.partial`` objects must be kept alive by the caller.
    Listener failures never propagate out of ``emit``: the exception object is
    returned as that listener's response.
    """

    _registered_signal: typing.ClassVar[
        "weakref.WeakValueDictionary[str, SoftSignal]"
    ] = weakref.WeakValueDictionary()

    def __init__(
        self, name: str, provide_args: typing.Optional[typing.Sequence[str]] = None
    ) -> None:
        super().__init__()

        self.name = name
        signal_import_str = f"{self.__module__}.{self.name}"
        self._registered_signal[signal_import_str] = self

        if provide_args is None:
            provide_args = []
        elif isinstance(provide_args, str):
            raise TypeError(
                "provide_args must be a sequence of names, not a string: %r"
                % (provide_args,)
            )

        self._provide_args = set(provide_args)
        if "sender" not in self._provide_args:
            self._provide_args.add("sender")

        if "signal" not in self._provide_args:
            self._provide_args.add("signal")

        # Re-entrant: weakref finalizers may run (via GC) on a thread that
        # already holds the lock.
        self.lock = threading.RLock()

        self._emit_signature = self._construct_listener_arguments()

        # sender -> {weak listener ref -> finalizer removing it on GC}
        self._listeners: typing.Dict[
            typing.Any, typing.Dict[weakref.ReferenceType, weakref.finalize]
        ] = {}

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.name!r}>"

    @property
    def __instance_import_str__(self) -> str:
        return f"{self.__module__}.{self.name}"

    def __getstate__(self) -> typing.Dict[str, typing.Any]:
        state = self.__dict__.copy()
        state.pop("lock", None)
        state.pop("_listeners", None)
        return state

    def __setstate__(self, state: typing.Dict[str, typing.Any]) -> None:
        state["lock"] = threading.RLock()
        state["_listeners"] = {}
        self.__dict__.update(state)

    def _construct_listener_arguments(self) -> Signature:
        params = [
            Parameter(
                name=name,
                annotation=typing.Any,
                kind=Parameter.KEYWORD_ONLY,
            )
            for name in self._provide_args
        ]

        default_field_names = ["pipeline_id", "process_id"]
        params.extend(
            [
                Parameter(
                    name=name,
                    annotation=typing.Union[str, int],
                    kind=Parameter.KEYWORD_ONLY,
                    default=None,
                )
                for name in default_field_names
            ]
        )
        return Signature(params)

    def _snapshot(self, sender: typing.Any) -> typing.List[weakref.ReferenceType]:
        """Copy the weak refs to call for ``sender`` (specific, then wildcard)."""
        sender = _normalise_sender(sender)
        with self.lock:
            refs = list(self._listeners.get(sender, ()))
            if sender is not GenericSender:
                refs.extend(self._listeners.get(GenericSender, ()))
        return refs

    def _prepare_call(
        self, listener_ref: weakref.ReferenceType, sender: typing.Any, kwargs: dict
    ) -> typing.Tuple[typing.Optional[Listener], typing.Dict[str, typing.Any]]:
        listener = listener_ref()
        if listener is None:
            return None, {}
        bound = self._emit_signature.bind(signal=self, sender=sender, **kwargs)
        return listener, bound.kwargs

    def _log_failure(self, listener: typing.Any, exc: BaseException) -> None:
        logger.error(
            "Signal '%s' listener %r raised %s: %s",
            self.name,
            listener,
            type(exc).__name__,
            exc,
            exc_info=exc,
        )

    def _execute_listener(
        self,
        listener_ref: weakref.ReferenceType,
        sender: typing.Any,
        **kwargs: typing.Any,
    ) -> Responses:
        listener = listener_ref()
        if listener is None:
            return []
        try:
            listener, call_kwargs = self._prepare_call(listener_ref, sender, kwargs)
            if listener is None:
                return []
            response = listener(**call_kwargs)
            if inspect.isawaitable(response):
                response = self._run_awaitable(response, listener)
        except Exception as e:
            self._log_failure(listener, e)
            response = e
        return [(listener, response)]

    def _run_awaitable(
        self, awaitable: typing.Awaitable[typing.Any], listener: typing.Any
    ) -> typing.Any:
        """Resolve an async listener result from synchronous ``emit``."""

        async def _await() -> typing.Any:
            return await awaitable

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(_await())

        # Cannot block a running loop: schedule it and hand back the task.
        logger.warning(
            "Signal '%s' async listener %r called via sync emit() inside a running "
            "event loop; scheduled as a task. Use emit_async() to await it.",
            self.name,
            listener,
        )
        task = asyncio.ensure_future(_await())
        _background_tasks.add(task)

        def _done(t: "asyncio.Future[typing.Any]") -> None:
            _background_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                self._log_failure(listener, t.exception())

        task.add_done_callback(_done)
        return task

    def emit(self, sender: typing.Any, **kwargs: typing.Any) -> Responses:
        """
        Emit a signal to all connected listeners for a given sender.

        Listeners for ``sender`` run first, then wildcard (``None``) listeners.
        A listener that raises does not stop the others; its exception is
        returned as its response.

        Args:
            sender: The object sending the signal.
            **kwargs: Additional keyword arguments to pass to listeners.
        Return:
            A list of tuples containing (listener, response).
        """
        responses: Responses = []
        for weak_listener in self._snapshot(sender):
            responses.extend(self._execute_listener(weak_listener, sender, **kwargs))
        return responses

    async def emit_async(self, sender: typing.Any, **kwargs: typing.Any) -> Responses:
        """
        Asynchronously emit a signal to all connected listeners for a given sender.

        Coroutine listeners are awaited on the running loop; sync listeners run
        in a worker thread. Listeners run one after another, in emit order.

        Args:
            sender: The object sending the signal.
            **kwargs: Additional keyword arguments to pass to listeners.
        Return:
            A list of tuples containing (listener, response).
        """
        responses: Responses = []
        for weak_listener in self._snapshot(sender):
            listener = weak_listener()
            if listener is None:
                continue
            try:
                listener, call_kwargs = self._prepare_call(
                    weak_listener, sender, kwargs
                )
                if listener is None:
                    continue
                response = await as_coroutine(listener, **call_kwargs)
            except Exception as e:
                self._log_failure(listener, e)
                response = e
            responses.append((listener, response))
        return responses

    def clean(self, sender: typing.Any) -> None:
        """
        Clean up all listeners associated with the sender.

        Args:
            sender: The object whose listeners should be removed.
        """
        sender = _normalise_sender(sender)
        with self.lock:
            bucket = self._listeners.pop(sender, None)
            if bucket:
                for finalizer in bucket.values():
                    finalizer.detach()

    def connect(self, sender: typing.Any, listener: Listener) -> None:
        """
        Connect a listener to a sender (``None`` means any sender).

        Only a weak reference is kept. Connecting the same listener to the
        same sender twice is a no-op.

        Args:
            sender: The object sending the signal.
            listener: The function to be called when the signal is emitted.
        """
        sender = _normalise_sender(sender)
        if not callable(listener):
            raise TypeError(
                "listener must be callable, got %r." % type(listener).__name__
            )

        ref: typing.Callable[..., weakref.ReferenceType] = weakref.ref
        listener_obj = listener
        if hasattr(listener, "__self__") and hasattr(listener, "__func__"):
            ref = weakref.WeakMethod
            listener_obj = listener.__self__

        ref_listener = ref(listener)

        with self.lock:
            bucket = self._listeners.setdefault(sender, {})
            if ref_listener in bucket:
                return
            # remove the reference when the referent is garbage collected
            finalizer = weakref.finalize(
                listener_obj,
                self._remove_dead_listener,
                sender=sender,
                listener=ref_listener,
            )
            finalizer.atexit = False
            bucket[ref_listener] = finalizer

    def disconnect(self, sender: typing.Any, listener: Listener) -> None:
        """
        Disconnect a listener from a sender (``None`` means the wildcard sender).

        Args:
            sender: The object sending the signal.
            listener: The function to be removed from the signal's listeners.
        """
        sender = _normalise_sender(sender)
        with self.lock:
            bucket = self._listeners.get(sender)
            if bucket is None:
                return
            for weak_listener in list(bucket):
                target = weak_listener()
                if target is None or target == listener:
                    bucket.pop(weak_listener).detach()
            # Clean up the entry if it is empty
            if not bucket:
                del self._listeners[sender]

    def _remove_dead_listener(
        self,
        sender: typing.Any,
        listener: typing.Optional[weakref.ReferenceType] = None,
    ) -> None:
        with self.lock:
            bucket = self._listeners.get(sender)
            if bucket is None:
                return
            bucket.pop(listener, None)
            if not bucket:
                del self._listeners[sender]

    @classmethod
    def registered_signals(cls) -> typing.List[str]:
        return list(cls._registered_signal.keys())


pipeline_pre_init = SoftSignal("pipeline_pre_init", provide_args=["args", "kwargs"])
pipeline_post_init = SoftSignal("pipeline_post_init", provide_args=["pipeline"])


pipeline_shutdown = SoftSignal(
    "pipeline_shutdown", provide_args=["pipeline", "execution_context"]
)
pipeline_stop = SoftSignal(
    "pipeline_stop", provide_args=["pipeline", "execution_context"]
)


pipeline_execution_start = SoftSignal(
    "pipeline_execution_start", provide_args=["pipeline"]
)
pipeline_execution_end = SoftSignal(
    "pipeline_execution_end", provide_args=["execution_context"]
)


event_init = SoftSignal("event_init", provide_args=["event", "init_kwargs"])
event_called = SoftSignal(
    "event_called",
    provide_args=["event", "init_kwargs", "call_kwargs", "hook_type", "result"],
)
event_phase_changed = SoftSignal("event_phase_changed", provide_args=["event", "phase"])

event_execution_init = SoftSignal(
    "event_execution_init",
    provide_args=["event", "execution_context", "executor", "call_kwargs"],
)
event_execution_start = SoftSignal(
    "event_execution_start", provide_args=["event", "execution_context"]
)
event_execution_end = SoftSignal(
    "event_execution_end", provide_args=["event", "execution_context"]
)
event_execution_retry = SoftSignal(
    "event_execution_retry",
    provide_args=[
        "event",
        "execution_context",
        "task_id",
        "backoff",
        "retry_count",
        "max_attempts",
    ],
)
event_execution_retry_done = SoftSignal(
    "event_execution_retry_done",
    provide_args=["event", "execution_context", "task_id", "max_attempts"],
)
event_execution_cancelled = SoftSignal(
    "event_execution_cancelled",
    provide_args=["task_profiles", "execution_context", "state"],
)
event_execution_aborted = SoftSignal(
    "event_execution_aborted",
    provide_args=["task_profiles", "execution_context", "state"],
)
event_execution_failed = SoftSignal(
    "event_execution_failed",
    provide_args=["task_profiles", "execution_context", "state"],
)
event_execution_paused = SoftSignal(
    "event_execution_paused",
    provide_args=["task_profiles", "execution_context", "state"],
)
event_execution_resumed = SoftSignal(
    "event_execution_resumed",
    provide_args=["task_profiles", "execution_context", "state"],
)

# Fired when a task suspends for human input (HITL). Lets an out-of-process
# governance layer learn a request is pending without the engine knowing about it.
hitl_requested = SoftSignal(
    "hitl_requested",
    provide_args=["execution_context", "request"],
)

# Batch Pipeline signals
batch_pipeline_started = SoftSignal(
    name="batch_pipeline_started",
    provide_args=["batch", "total_pipelines", "timestamp"],
)
batch_pipeline_finished = SoftSignal(
    name="batch_pipeline_finished",
    provide_args=["batch", "metrics", "success_rate", "total_duration", "timestamp"],
)
pipeline_metrics_updated = SoftSignal(
    name="pipeline_metrics_updated",
    provide_args=[
        "batch_id",
        "metrics",
        "active_count",
        "completion_rate",
        "timestamp",
    ],
)
