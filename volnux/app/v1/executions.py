import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Path, Query

from volnux.models import Execution, ExecutionTrace
from volnux.models.enums import (
    AuditEventType,
    ExecutionState,
    TriggerType,
    WorkflowStatus,
)
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import ExecuteRequest
from ..utils import (
    RequestContext,
    audit,
    audit_log,
    get_current_user,
    require_permission,
)
from .workflows import load_workflow, versions_of

logger = logging.getLogger(__name__)
app = get_current_app()


async def _load_execution(ctx: RequestContext, execution_id: str) -> Execution:
    execution = await Execution.get_async(execution_id)
    ensure_same_org(ctx, execution.organization.id, "Execution")
    return execution


@app.post("/api/v1/workflows/{workflow_id}/execute", status_code=202)
async def execute_workflow(
    data: ExecuteRequest,
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_EXECUTE)),
):
    """Run a production workflow (workflow-consumer or operator)."""
    workflow = await load_workflow(ctx, workflow_id)
    if workflow.status != WorkflowStatus.PRODUCTION:
        raise HTTPException(409, "Only production workflows can be executed")
    versions = await versions_of(workflow.id)
    if not versions:
        raise HTTPException(409, "The workflow has no published version")
    version = next((v for v in versions if v.is_active), versions[0])

    execution = Execution(
        workflow=workflow,
        workflow_version=version,
        organization=ctx.organization,
        team=workflow.team,
        triggered_by=ctx.user,
        trigger_type=TriggerType.MANUAL,
        trigger_params={},
        execution_params=data.inputs,
        parent_execution=None,
    )
    await execution.save_async()
    await audit(
        ctx,
        AuditEventType.EXECUTION_STARTED,
        "execution",
        execution.id,
        f"started {workflow.name}",
        {"workflow": workflow.id, "version": version.version},
    )

    from volnux.engine.workflows.trigger import get_trigger_engine
    from volnux.engine.workflows.trigger.triggers import TriggerType as EngineTrigger

    try:
        await get_trigger_engine().fire_trigger(
            workflow.name,
            dict(data.inputs),
            EngineTrigger.MANUAL,
            execution_id=execution.id,
        )
    except (RuntimeError, ValueError) as exc:
        logger.warning("Could not hand %s to the engine: %s", workflow.name, exc)
        execution.status = ExecutionState.FAILED
        execution.ended_at = datetime.now(timezone.utc).timestamp()
        await execution.save_async()
        await audit(
            ctx,
            AuditEventType.EXECUTION_FAILED,
            "execution",
            execution.id,
            f"engine refused the run: {exc}",
        )
        raise HTTPException(503, f"The workflow could not be started: {exc}")
    return {"status": "success", "data": _serialize_model(execution)}


@app.get("/api/v1/executions")
async def list_executions(
    pagination: dict = Depends(Pagination),
    workflow_id: Optional[str] = Query(None),
    status: Optional[ExecutionState] = Query(None),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_READ)),
):
    filters = {"organization__object_id": ctx.organization.id}
    if workflow_id:
        filters["workflow__object_id"] = workflow_id
    if status:
        filters["status"] = status
    executions = await Execution.filter_async(
        **page_args(pagination), order_by="-created_at", **filters
    )
    return {
        "status": "success",
        "data": [_serialize_model(e) for e in executions],
        "meta": meta(pagination, await Execution.count_async(**filters)),
    }


@app.get("/api/v1/executions/{execution_id}")
async def get_execution(
    execution_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_READ)),
):
    execution = await _load_execution(ctx, execution_id)
    return {"status": "success", "data": _serialize_model(execution)}


@app.get("/api/v1/executions/{execution_id}/traces")
async def get_execution_traces(
    execution_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_TRACE)),
):
    await _load_execution(ctx, execution_id)
    traces = await ExecutionTrace.filter_async(
        execution__object_id=execution_id, order_by="step_order"
    )
    return {"status": "success", "data": [_serialize_model(t) for t in traces]}


def _controllable_executor():
    """The engine's executor, if it can control runs: 503 when there is none,
    501 when it hands workflows to another process (Celery, Kubernetes)."""
    from volnux.engine.workflows.trigger import get_trigger_engine

    engine = get_trigger_engine()
    executor = getattr(engine, "workflow_executor", None)
    if executor is None:
        raise HTTPException(503, "The workflow engine is not running")
    if not getattr(executor, "supports_control", False):
        raise HTTPException(
            501,
            f"{type(executor).__name__} runs workflows elsewhere and cannot "
            "pause, resume or stop them from the API",
        )
    return executor


async def _transition(
    ctx: RequestContext,
    execution_id: str,
    allowed: tuple,
    new_state: ExecutionState,
    event: AuditEventType,
    verb: str,
    control: str,
) -> dict:
    execution = await _load_execution(ctx, execution_id)
    if execution.status not in allowed:
        raise HTTPException(
            409,
            f"Cannot {verb} an execution that is {getattr(execution.status, 'value', execution.status)}",
        )
    executor = _controllable_executor()
    if not await getattr(executor, control)(execution_id):
        raise HTTPException(
            409,
            "The execution is not running in this process (it has not started "
            "yet, has ended, or runs on another replica)",
        )
    execution.status = new_state
    if new_state == ExecutionState.STOPPED:
        execution.ended_at = datetime.now(timezone.utc).timestamp()
    await execution.save_async()
    await audit(ctx, event, "execution", execution.id, verb)
    return {"status": "success", "data": _serialize_model(execution)}


async def on_execution_finished(
    execution_id: str, state: str, error: Optional[str] = None
) -> None:
    """Listener of the engine's executor: the run ended, so the record says so."""
    from volnux.models import Organization, User

    try:
        execution = await Execution.get_or_none_async(execution_id)
        if execution is None:
            return
        final = {
            "completed": ExecutionState.COMPLETED,
            "failed": ExecutionState.FAILED,
            "stopped": ExecutionState.STOPPED,
        }.get(state)
        if final is None:
            return
        already_ended = execution.ended_at is not None
        execution.status = final
        if not already_ended:
            execution.ended_at = datetime.now(timezone.utc).timestamp()
        await execution.save_async()
        if execution.triggered_by is None or final == ExecutionState.STOPPED:
            return  # (a stop was audited by the request that made it)
        await audit_log(
            organization=await Organization.get_async(execution.organization.id),
            event_type=(
                AuditEventType.EXECUTION_COMPLETED
                if final == ExecutionState.COMPLETED
                else AuditEventType.EXECUTION_FAILED
            ),
            actor=await User.get_async(execution.triggered_by.id),
            actor_role="system",
            target_type="execution",
            target_id=execution.id,
            action=state if error is None else f"{state}: {error}",
        )
    except Exception:
        logger.exception("Could not record the end of execution %s", execution_id)


@app.post("/api/v1/executions/{execution_id}/pause")
async def pause_execution(
    execution_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_CONTROL)),
):
    return await _transition(
        ctx,
        execution_id,
        (ExecutionState.RUNNING,),
        ExecutionState.PAUSED,
        AuditEventType.EXECUTION_PAUSED,
        "pause",
        "pause_execution",
    )


@app.post("/api/v1/executions/{execution_id}/resume")
async def resume_execution(
    execution_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_CONTROL)),
):
    return await _transition(
        ctx,
        execution_id,
        (ExecutionState.PAUSED,),
        ExecutionState.RUNNING,
        AuditEventType.EXECUTION_RESUMED,
        "resume",
        "resume_execution",
    )


@app.post("/api/v1/executions/{execution_id}/stop")
async def stop_execution(
    execution_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_CONTROL)),
):
    return await _transition(
        ctx,
        execution_id,
        (ExecutionState.RUNNING, ExecutionState.PAUSED),
        ExecutionState.STOPPED,
        AuditEventType.EXECUTION_STOPPED,
        "stop",
        "stop_execution",
    )
