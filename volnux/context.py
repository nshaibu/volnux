import inspect
import functools
import logging
from contextvars import ContextVar, Token
from dataclasses import dataclass, asdict
from typing import Any, ContextManager, Dict, Optional, Generator, TypeVar, Callable
from contextlib import ExitStack, contextmanager

from volnux import config as _config_module
from volnux.config import VolnuxConfig


logger = logging.getLogger(__name__)

_WORKFLOW_ID: ContextVar[Optional[str]] = ContextVar("volnux_workflow_id", default=None)
_WORKFLOW_VERSION: ContextVar[Optional[str]] = ContextVar(
    "volnux_workflow_version", default=None
)
_CORRELATION_ID: ContextVar[Optional[str]] = ContextVar(
    "volnux_correlation_id", default=None
)
_EXECUTION_ID: ContextVar[Optional[str]] = ContextVar(
    "volnux_execution_id", default=None
)
_PARENT_EXECUTION_ID: ContextVar[Optional[str]] = ContextVar(
    "volnux_parent_execution_id", default=None
)
_TRACE_ID: ContextVar[Optional[str]] = ContextVar("volnux_trace_id", default=None)
_IS_SPECULATIVE: ContextVar[bool] = ContextVar("volnux_is_speculative", default=False)
_PROJECT_ID: ContextVar[Optional[str]] = ContextVar("volnux_project_id", default=None)
_NODE_ID: ContextVar[Optional[str]] = ContextVar("volnux_node_id", default=None)
# No default: the process-wide VolnuxConfig is looked up when it is needed. (A
# default would build it, and read the configuration files, as a side effect of
# importing this module, and would keep that object even after it is replaced.)
_CONFIG: ContextVar[Optional[VolnuxConfig]] = ContextVar("volnux_config", default=None)

CARRIER_KEY = "__volnux_context__"


class TracePropagation:
    """How the active distributed trace travels in a carrier.

    ``volnux.otel`` registers one (:func:`register_trace_propagation`); this module
    does not import OpenTelemetry. ``inject`` returns the trace context to put in
    the snapshot (a JSON-compatible dict, ``None`` when no trace is active);
    ``attach`` is a context manager that makes a carried trace context the parent
    of what runs inside it.
    """

    def __init__(
        self,
        inject: Callable[[], Optional[Dict[str, str]]],
        attach: Callable[[Dict[str, str]], ContextManager[Any]],
    ) -> None:
        self.inject = inject
        self.attach = attach


_trace_propagation: Optional[TracePropagation] = None


def register_trace_propagation(propagation: Optional[TracePropagation]) -> None:
    """Install (or, with ``None``, remove) the trace propagation hook."""
    global _trace_propagation
    _trace_propagation = propagation


# Let ``volnux.config.setting()`` read the configuration this context carries.
_config_module._active_context_config = _CONFIG.get


@dataclass(frozen=True)
class ExecutionContextSnapshot:
    """Immutable snapshot of the active Volnux execution context."""

    workflow_id: Optional[str] = None
    workflow_version: Optional[str] = None
    correlation_id: Optional[str] = None
    execution_id: Optional[str] = None
    parent_execution_id: Optional[str] = None
    trace_id: Optional[str] = None
    is_speculative: bool = False
    project_id: Optional[str] = None
    node_id: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    # The distributed trace (W3C trace context) this was taken in; see
    # TracePropagation. Lets a worker or pod continue the dispatcher's trace.
    trace_context: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionContextSnapshot":
        valid_keys = cls.__dataclass_fields__.keys()
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered_data)

    def get_config_instance(self) -> VolnuxConfig:
        """Reconstructs the VolnuxConfig instance from the snapshot dict."""
        if self.config is None:
            return VolnuxConfig.get_instance()
        return VolnuxConfig.from_dict(self.config)


def get_current_workflow_id() -> Optional[str]:
    return _WORKFLOW_ID.get()


def get_current_workflow_version() -> Optional[str]:
    return _WORKFLOW_VERSION.get()


def get_current_correlation_id() -> Optional[str]:
    return _CORRELATION_ID.get()


def get_current_execution_id() -> Optional[str]:
    return _EXECUTION_ID.get()


def get_current_parent_execution_id() -> Optional[str]:
    return _PARENT_EXECUTION_ID.get()


def get_current_trace_id() -> Optional[str]:
    return _TRACE_ID.get()


def is_speculative_pass() -> bool:
    return _IS_SPECULATIVE.get()


def get_current_project_id() -> Optional[str]:
    return _PROJECT_ID.get()


def get_current_node_id() -> Optional[str]:
    return _NODE_ID.get()


def get_current_config() -> VolnuxConfig:
    """The configuration of the active context, else the process-wide one."""
    cfg = _CONFIG.get()
    return cfg if cfg is not None else VolnuxConfig.get_instance()


def snapshot_context() -> ExecutionContextSnapshot:
    cfg = get_current_config()
    return ExecutionContextSnapshot(
        workflow_id=_WORKFLOW_ID.get(),
        workflow_version=_WORKFLOW_VERSION.get(),
        correlation_id=_CORRELATION_ID.get(),
        execution_id=_EXECUTION_ID.get(),
        parent_execution_id=_PARENT_EXECUTION_ID.get(),
        trace_id=_TRACE_ID.get(),
        is_speculative=_IS_SPECULATIVE.get(),
        project_id=_PROJECT_ID.get(),
        node_id=_NODE_ID.get(),
        config=cfg.to_dict(),
        trace_context=_current_trace_context(),
    )


def _current_trace_context() -> Optional[Dict[str, str]]:
    propagation = _trace_propagation
    if propagation is None:
        return None
    try:
        return propagation.inject()
    except Exception:  # tracing must never break execution
        logger.debug("Could not inject the trace context", exc_info=True)
        return None


def inject_carrier(
    carrier: Dict[str, Any],
    node_id_override: Optional[str] = None,
    config_override: Optional[VolnuxConfig] = None,
) -> Dict[str, Any]:
    """
    Injects context + configuration dictionary into task payload prior to dispatch.
    """
    snapshot = snapshot_context()
    ctx_dict = snapshot.to_dict()

    if config_override is not None:
        ctx_dict["config"] = config_override.to_dict()

    if node_id_override is not None:
        ctx_dict["node_id"] = node_id_override
        # Ensure config's node_id also reflects target node
        if ctx_dict.get("config"):
            ctx_dict["config"]["node_id"] = node_id_override

    carrier[CARRIER_KEY] = ctx_dict
    return carrier


def extract_carrier(carrier: Dict[str, Any]) -> ExecutionContextSnapshot:
    """Extracts context + configuration snapshot from a remote payload."""
    raw_ctx = carrier.get(CARRIER_KEY, {}) if isinstance(carrier, dict) else None
    if isinstance(raw_ctx, dict):
        return ExecutionContextSnapshot.from_dict(raw_ctx)
    return ExecutionContextSnapshot()


@contextmanager
def volnux_context(
    workflow_id: Optional[str] = None,
    workflow_version: Optional[str] = None,
    correlation_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    parent_execution_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    is_speculative: Optional[bool] = None,
    project_id: Optional[str] = None,
    node_id: Optional[str] = None,
    config: Optional[VolnuxConfig] = None,
    snapshot: Optional[ExecutionContextSnapshot] = None,
) -> Generator[ExecutionContextSnapshot, None, None]:
    """
    Scoped Context Manager that activates context variables + VolnuxConfig
    and restores tokens on exit.
    """
    tokens: Dict[ContextVar, Token] = {}
    stack = ExitStack()

    if snapshot:
        workflow_id = snapshot.workflow_id if workflow_id is None else workflow_id
        workflow_version = (
            snapshot.workflow_version if workflow_version is None else workflow_version
        )
        correlation_id = (
            snapshot.correlation_id if correlation_id is None else correlation_id
        )
        execution_id = snapshot.execution_id if execution_id is None else execution_id
        parent_execution_id = (
            snapshot.parent_execution_id
            if parent_execution_id is None
            else parent_execution_id
        )
        trace_id = snapshot.trace_id if trace_id is None else trace_id
        is_speculative = (
            snapshot.is_speculative if is_speculative is None else is_speculative
        )
        project_id = snapshot.project_id if project_id is None else project_id
        node_id = snapshot.node_id if node_id is None else node_id
        if config is None and snapshot.config:
            config = snapshot.get_config_instance()
        if snapshot.trace_context and _trace_propagation is not None:
            try:
                stack.enter_context(_trace_propagation.attach(snapshot.trace_context))
            except Exception:  # tracing must never break execution
                logger.debug("Could not attach the trace context", exc_info=True)

    def _set_var(var: ContextVar, val: Any) -> None:
        if val is not None:
            tokens[var] = var.set(val)

    try:
        _set_var(_WORKFLOW_ID, workflow_id)
        _set_var(_WORKFLOW_VERSION, workflow_version)
        _set_var(_CORRELATION_ID, correlation_id)
        _set_var(_EXECUTION_ID, execution_id)
        _set_var(_PARENT_EXECUTION_ID, parent_execution_id)
        _set_var(_TRACE_ID, trace_id)
        _set_var(_IS_SPECULATIVE, is_speculative)
        _set_var(_PROJECT_ID, project_id)
        _set_var(_NODE_ID, node_id)
        _set_var(_CONFIG, config)
        yield snapshot_context()
    finally:
        # whatever was set is put back, last set first
        for var, token in reversed(list(tokens.items())):
            var.reset(token)
        stack.close()


F = TypeVar("F", bound=Callable[..., Any])


def _find_carrier(args: tuple, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """The payload that carries the context: the ``payload`` keyword if it is a
    dict, else the keyword arguments themselves if they carry it, else the first
    positional argument that does. ``{}`` (no context) if none."""
    payload = kwargs.get("payload")
    if isinstance(payload, dict):
        return payload
    if CARRIER_KEY in kwargs:
        return kwargs
    for arg in args:
        if isinstance(arg, dict) and CARRIER_KEY in arg:
            return arg
    return {}


def dispatch_worker_task(func: F) -> F:
    """
    Decorator for remote worker entrypoints (e.g., @celery.task or K8s task runner).
    Automatically extracts __volnux_context__ from kwargs/payload and activates it.
    """

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        snapshot = extract_carrier(_find_carrier(args, kwargs))

        with volnux_context(snapshot=snapshot):
            return await func(*args, **kwargs)

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        snapshot = extract_carrier(_find_carrier(args, kwargs))

        with volnux_context(snapshot=snapshot):
            return func(*args, **kwargs)

    return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper  # type: ignore
