import asyncio
import logging
import threading
import typing
import weakref
from inspect import Parameter, Signature

from volnux.mixins import ObjectIdentityMixin

logger = logging.getLogger(__name__)


class GenericSender:
    pass


class SoftSignal(ObjectIdentityMixin):
    _registered_signal: typing.ClassVar[
        typing.Dict[str, typing.Dict[str, typing.Any]]
    ] = {}

    def __init__(self, name: str, provide_args: typing.List[str] = None):
        super().__init__()

        self.name = name
        signal_import_str = f"{self.__module__}.{self.name}"
        self._registered_signal[signal_import_str] = {}

        if provide_args is None:
            provide_args = []
        elif not isinstance(provide_args, (list, tuple)):
            provide_args = tuple(provide_args)

        self._provide_args = set(provide_args)
        if "sender" not in self._provide_args:
            self._provide_args.add("sender")

        if "signal" not in self._provide_args:
            self._provide_args.add("signal")

        self.lock = threading.Lock()

        self._emit_signature = self._construct_listener_arguments()

        # Initialize a dict to hold connected listeners as weak references
        self._listeners: typing.Dict[typing.Any, typing.Set[weakref.ReferenceType]] = {}

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.name!r}>"

    @property
    def __instance_import_str__(self):
        return f"{self.__module__}.{self.name}"

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("lock", None)
        state.pop("_listeners", None)
        return state

    def __setstate__(self, state):
        state["lock"] = threading.Lock()
        state["_listeners"] = {}
        self.__dict__.update(state)

    def _construct_listener_arguments(self):
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

    def _execute_listeners(self, listener_ref, sender, **kwargs):
        responses = []
        listener = listener_ref()  # Get the listener from the weak reference
        if listener:  # Check if the listener is still alive
            bounded_args = self._emit_signature.bind(
                signal=self, sender=sender, **kwargs
            )
            try:
                response = listener(**bounded_args.kwargs)
            except Exception as e:
                logger.exception(str(e), exc_info=e)
                response = e
            responses.append((listener, response))
        return responses

    def emit(
        self, sender: typing.Any, **kwargs
    ) -> typing.List[typing.Tuple[typing.Any, typing.Any]]:
        """
        Emit a signal to all connected listeners for a given sender.

        Args:
            sender: The object sending the signal.
            **kwargs: Additional keyword arguments to pass to listeners.
        Return:
            A list of tuples containing (listener, response).
        """
        responses = []
        if sender in self._listeners:
            for weak_listener in self._listeners[sender]:
                responses.extend(
                    self._execute_listeners(weak_listener, sender, **kwargs)
                )

        if GenericSender in self._listeners:
            for weak_listener in self._listeners[GenericSender]:
                responses.extend(
                    self._execute_listeners(weak_listener, sender, **kwargs)
                )

        return responses

    async def emit_async(
        self, sender: typing.Any, **kwargs
    ) -> typing.List[typing.Tuple[typing.Any, typing.Any]]:
        """
        Asynchronously emit a signal to all connected listeners for a given sender.

        Args:
            sender: The object sending the signal.
            **kwargs: Additional keyword arguments to pass to listeners.
        Return:
            A list of tuples containing (listener, response).
        """
        return await asyncio.to_thread(self.emit, sender, **kwargs)

    def clean(self, sender: typing.Any) -> None:
        """
        Clean up all listeners associated with the sender.

        Args:
            sender: The object whose listeners should be removed.
        """
        with self.lock:
            if sender in self._listeners:
                del self._listeners[sender]

    def connect(self, sender: typing.Any, listener) -> None:
        """
        Connect a listener to a sender.

        Args:
            sender: The object sending the signal.
            listener: The function to be called when the signal is emitted.
        """
        if sender is None:
            sender = GenericSender

        if sender not in self._listeners:
            self._listeners[sender] = set()

        ref = weakref.ref
        listener_obj = listener
        if hasattr(listener, "__self__") and hasattr(listener, "__func__"):
            ref = weakref.WeakMethod
            listener_obj = listener.__self__

        ref_listener = ref(listener)

        with self.lock:
            self._listeners[sender].add(ref_listener)

        # remove the reference when the referent is garbage collected
        weakref.finalize(
            listener_obj,
            self._remove_dead_listener,
            sender=sender,
            listener=ref_listener,
        )

    def disconnect(self, sender: typing.Any, listener: typing.Callable) -> None:
        """
        Disconnect a listener from a sender.

        Args:
            sender: The object sending the signal.
            listener: The function to be removed from the signal's listeners.
        """
        with self.lock:
            if sender in self._listeners:
                self._listeners[sender] = set(
                    [
                        weak_listener
                        for weak_listener in self._listeners[sender]
                        if weak_listener() != listener
                    ]
                )
                # Clean up the list if it is empty
                if not self._listeners[sender]:
                    del self._listeners[sender]

    def _remove_dead_listener(self, sender, listener: typing.Any = None) -> None:
        if sender:
            with self.lock:
                if sender in self._listeners:
                    try:
                        self._listeners[sender].remove(listener)
                    except KeyError:
                        pass

    @classmethod
    def registered_signals(cls):
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
