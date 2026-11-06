import logging
import time
from contextlib import asynccontextmanager

from volnux.config import setting
from volnux.app.utils import get_jwt_secret
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from volnux.models import (
    Organization,
    User,
    Team,
    TeamMember,
    Role,
    RoleAssignment,
    Workflow,
    WorkflowVersion,
    WorkflowVariable,
    WorkflowDescriptor,
    ApprovalChain,
    ApprovalStep,
    Execution,
    ExecutionTrace,
    HITLRequest,
    AuditEntry,
    Delegation,
    DelegationAction,
    BreakGlassAccess,
    BreakGlassAction,
    Event,
    EventVersion,
    EventDependency,
    Namespace,
    MeshNode,
    NodeHeartbeat,
    TriggerConfig,
    NotificationConfig,
)

from volnux.app.formax_utils import register_formax_model_constraints

logger = logging.getLogger(__name__)


def _execution_listener():
    from volnux.app.v1.executions import on_execution_finished

    return on_execution_finished


def _connect_engine():
    """Have the engine's executor report the end of every run to the API (so an
    ``Execution`` leaves RUNNING). Returns the executor, or None when there is no
    engine in this process (a standalone API)."""
    try:
        from volnux.engine.workflows.trigger import get_trigger_engine

        engine = get_trigger_engine()
        if not engine.has_workflow_executor():
            return None
        executor = engine.workflow_executor
        executor.add_listener(_execution_listener())
        return executor
    except Exception:
        logger.exception("The API could not connect to the workflow engine")
        return None


async def _restore_triggers() -> int:
    """Register the triggers created through the API with the engine again."""
    try:
        from volnux.app.trigger_factory import restore_triggers

        return await restore_triggers()
    except Exception:
        logger.exception("The API's triggers could not be restored")
        return 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Record start time on startup
    app.state.start_time = time.time()
    # Fail now, not at the first login, if production has no SECRET_KEY
    get_jwt_secret()
    listening = _connect_engine()
    restored = 0
    if listening is not None:
        restored = await _restore_triggers()
    try:
        yield
    finally:
        if listening is not None:
            listening.remove_listener(_execution_listener())


# The interactive documentation is served only with the API_DOCS setting.
_docs = bool(setting("API_DOCS", False))


def _cors_origins() -> list:
    """The CORS_ALLOWED_ORIGINS setting; an environment value may also be a plain
    comma-separated list (``https://a.example,https://b.example``)."""
    origins = setting("CORS_ALLOWED_ORIGINS", [])
    if isinstance(origins, str):
        origins = [o.strip() for o in origins.split(",") if o.strip()]
    return list(origins)


_app = FastAPI(
    title="Volnux Platform API",
    description="REST API for the Volnux governed workflow platform",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if _docs else None,
    redoc_url="/redoc" if _docs else None,
)

# Register all Formax model constraints in the OpenAPI schema
register_formax_model_constraints(
    _app,
    Workflow,
    User,
    Team,
    Role,
    Organization,
    ApprovalChain,
    ApprovalStep,
    Execution,
    ExecutionTrace,
    HITLRequest,
    AuditEntry,
    Delegation,
    DelegationAction,
    MeshNode,
    NodeHeartbeat,
    TriggerConfig,
    BreakGlassAccess,
    BreakGlassAction,
    Event,
    EventVersion,
    EventDependency,
    Namespace,
    NotificationConfig,
    WorkflowVersion,
    WorkflowVariable,
    WorkflowDescriptor,
    RoleAssignment,
    TeamMember,
)


# Observability (OpenTelemetry, Prometheus) is set up by the server lifecycle
# (volnux/adapters/lifecycle), which knows the engine and the run mode.

# Middleware (last added = first executed)
_app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    # (a wildcard origin and credentials must not be combined: browsers would send
    # cookies and tokens to any site)
    allow_credentials="*" not in _cors_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_current_app() -> FastAPI:
    return _app
