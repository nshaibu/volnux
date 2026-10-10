import inspect
import functools
from contextvars import ContextVar, Token
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Generator, TypeVar, Callable
from contextlib import contextmanager

from volnux.config import VolnuxConfig


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
_CONFIG: ContextVar[VolnuxConfig] = ContextVar(
    "volnux_config", default=VolnuxConfig.get_instance()
)

CARRIER_KEY = "__volnux_context__"


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
    return _CONFIG.get()


def snapshot_context() -> ExecutionContextSnapshot:
    cfg = _CONFIG.get()
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
    )


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

    if config_override:
        ctx_dict["config"] = config_override.to_dict()

    if node_id_override:
        ctx_dict["node_id"] = node_id_override
        # Ensure config's node_id also reflects target node
        if ctx_dict.get("config"):
            ctx_dict["config"]["node_id"] = node_id_override

    carrier[CARRIER_KEY] = ctx_dict
    return carrier


def extract_carrier(carrier: Dict[str, Any]) -> ExecutionContextSnapshot:
    """Extracts context + configuration snapshot from a remote payload."""
    raw_ctx = carrier.get(CARRIER_KEY, {})
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

    def _set_var(var: ContextVar, val: Any) -> None:
        if val is not None:
            tokens[var] = var.set(val)

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

    try:
        yield snapshot_context()
    finally:
        for var, token in tokens.items():
            var.reset(token)


F = TypeVar("F", bound=Callable[..., Any])


def dispatch_worker_task(func: F) -> F:
    """
    Decorator for remote worker entrypoints (e.g., @celery.task or K8s task runner).
    Automatically extracts __volnux_context__ from kwargs/payload and activates it.
    """

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        # Extract carrier from kwargs or first arg if dict
        carrier = kwargs.get("payload", kwargs)
        snapshot = extract_carrier(carrier)

        with volnux_context(snapshot=snapshot):
            return await func(*args, **kwargs)

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        carrier = kwargs.get("payload", kwargs)
        snapshot = extract_carrier(carrier)

        with volnux_context(snapshot=snapshot):
            return func(*args, **kwargs)

    return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper  # type: ignore
