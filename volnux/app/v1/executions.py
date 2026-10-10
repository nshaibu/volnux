from typing import Optional
from datetime import datetime, timezone
from fastapi import Depends, Query, Path, Body, HTTPException

from volnux.models import Workflow, Execution, ExecutionTrace
from volnux.models.enums import WorkflowStatus, ExecutionState, TriggerType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.post("/api/v1/workflows/{workflow_id}/execute", status_code=202)
async def execute_workflow(
    workflow_id: str = Path(...),
    data: dict = Body(default={}),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_EXECUTE)),
):
    """Trigger a workflow execution. Requires workflow-consumer or operator role."""
    workflow = await Workflow.get_async(workflow_id)
    if workflow.status != WorkflowStatus.PRODUCTION:
        raise HTTPException(400, "Only production workflows can be executed")

    execution = Execution(
        workflow_id=workflow_id,
        workflow_version_id=data.get("workflow_version_id", ""),
        organization_id=org_id,
        team_id=workflow.team,
        triggered_by=user["user_id"],
        trigger_type=TriggerType.MANUAL,
        execution_params=data.get("params", {}),
    )
    await execution.save_async()

    return {"status": "success", "data": _serialize_model(execution)}


@app.get("/api/v1/executions")
async def list_executions(
    org_id: str = Depends(get_org_id),
    pagination: dict = Depends(Pagination),
    workflow_id: Optional[str] = Query(None),
    status: Optional[ExecutionState] = Query(None),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_READ)),
):
    """List executions with optional filters. Requires execution-read permission."""
    filters = {"organization_id": org_id}
    if workflow_id:
        filters["workflow_id"] = workflow_id
    if status:
        filters["status"] = status

    executions = await Execution.filter_async(
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
        order_by="-started_at",
        **filters,
    )
    total = await Execution.count_async(**filters)

    return {
        "status": "success",
        "data": [_serialize_model(e) for e in executions],
        "meta": {
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "total": total,
        },
    }


@app.get("/api/v1/executions/{execution_id}")
async def get_execution(
    execution_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_READ)),
):
    """Get execution by ID."""
    execution = await Execution.get_async(execution_id)
    return {"status": "success", "data": _serialize_model(execution)}


@app.get("/api/v1/executions/{execution_id}/traces")
async def get_execution_traces(
    execution_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_TRACE)),
):
    """Get the fractal execution tree for an execution. Requires trace permission."""
    traces = ExecutionTrace.filter_async(
        execution_id=execution_id, order_by="step_order"
    )
    return {"status": "success", "data": [_serialize_model(t) for t in traces]}


@app.post("/api/v1/executions/{execution_id}/pause")
async def pause_execution(
    execution_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_CONTROL)),
):
    """Pause a running execution. Requires operator role."""
    execution = Execution.get(execution_id)
    if execution.status != ExecutionState.RUNNING:
        raise HTTPException(400, "Can only pause running executions")
    execution.status = ExecutionState.PAUSED
    execution.save()
    return {"status": "success", "data": _serialize_model(execution)}


@app.post("/api/v1/executions/{execution_id}/resume")
async def resume_execution(
    execution_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_CONTROL)),
):
    """Resume a paused execution. Requires operator role."""
    execution = await Execution.get_async(execution_id)
    if execution.status != ExecutionState.PAUSED:
        raise HTTPException(400, "Can only resume paused executions")
    execution.status = ExecutionState.RUNNING
    await execution.save_async()
    return {"status": "success", "data": _serialize_model(execution)}


@app.post("/api/v1/executions/{execution_id}/stop")
async def stop_execution(
    execution_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EXECUTION_CONTROL)),
):
    """Stop a running execution. Requires an operator role."""
    execution = await Execution.get_async(execution_id)
    if execution.status not in (ExecutionState.RUNNING, ExecutionState.PAUSED):
        raise HTTPException(400, "Can only stop running or paused executions")
    execution.status = ExecutionState.STOPPED
    execution.ended_at = datetime.now(timezone.utc).timestamp()
    await execution.save_async()
    return {"status": "success", "data": _serialize_model(execution)}
