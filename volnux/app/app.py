import time
from contextlib import asynccontextmanager
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Record start time on startup
    app.state.start_time = time.time()
    yield


_app = FastAPI(
    title="Volnux Platform API",
    description="REST API for the Volnux governed workflow platform",
    version="1.0.0",
    lifespan=lifespan,
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


# Observability
if getattr(config, "OTEL_ENABLED", True):
    setup_otel(app, engine)

if getattr(config, "METRICS_ENABLED", True):
    setup_metrics(app, engine, port=metrics_port)

# Middleware (last added = first executed)
app.add_middleware(RequestLoggingMiddleware)

cors_origins = ["*"] if dev_mode else getattr(config, "API_CORS_ORIGINS", [])

_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if getattr(config, "API_RATE_LIMIT_ENABLED", True) and not dev_mode:
    app.add_middleware(
        RateLimitMiddleware,
        requests_per_window=getattr(config, "API_RATE_LIMIT_REQUESTS", 1000),
        window_seconds=getattr(config, "API_RATE_LIMIT_WINDOW", 60),
    )


def get_current_app() -> FastAPI:
    return _app
