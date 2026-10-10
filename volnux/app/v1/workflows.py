from typing import Optional
from datetime import datetime, timezone
from fastapi import Depends, Query, Body, Path, HTTPException

from volnux.models import Workflow, WorkflowVersion
from volnux.models.enums import WorkflowStatus, WorkflowCategory
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.post("/api/v1/workflows", status_code=201)
async def create_workflow(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_CREATE)),
):
    """Create a new workflow. Requires a workflow-author role."""
    data["organization_id"] = org_id
    data["created_by"] = user["user_id"]
    workflow = Workflow(**data)
    await workflow.save_async()
    return {"status": "success", "data": _serialize_model(workflow)}


@app.get("/api/v1/workflows")
async def list_workflows(
    org_id: str = Depends(get_org_id),
    pagination: dict = Depends(Pagination),
    status: Optional[WorkflowStatus] = Query(None),
    category: Optional[WorkflowCategory] = Query(None),
    team_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    """List workflows with optional filters. Requires workflow-read permission."""
    filters = {"organization_id": org_id}
    if status:
        filters["status"] = status
    if category:
        filters["category"] = category
    if team_id:
        filters["team_id"] = team_id

    workflows: list[Workflow] = await Workflow.filter_async(
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
        order_by="-creation_time",
        **filters,
    )
    total = await Workflow.count_async(**filters)

    return {
        "status": "success",
        "data": [_serialize_model(w) for w in workflows],
        "meta": {
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "total": total,
        },
    }


@app.get("/api/v1/workflows/{workflow_id}")
async def get_workflow(
    workflow_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    """Get workflow by ID."""
    workflow: Workflow = await Workflow.get_async(workflow_id)
    if workflow.organization.id != user["organization_id"]:
        raise HTTPException(403, "Access denied")
    return {"status": "success", "data": _serialize_model(workflow)}


@app.get("/api/v1/workflows/{workflow_id}/source")
async def get_workflow_source(
    workflow_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    """Get the Pointy-Lang source of a workflow."""
    workflow: Workflow = await Workflow.get_async(workflow_id)
    return {"status": "success", "data": {"source": workflow.pointy_lang_source}}


@app.put("/api/v1/workflows/{workflow_id}")
async def update_workflow(
    workflow_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_UPDATE)),
):
    """Update a workflow. Requires a workflow-author role."""
    workflow: Workflow = await Workflow.get_async(workflow_id)
    immutable = ("id", "organization_id", "creation_time", "created_by")
    for key, value in data.items():
        if hasattr(workflow, key) and key not in immutable:
            setattr(workflow, key, value)
    workflow.updated_by = user["user_id"]
    workflow.touch()
    await workflow.save_async()
    return {"status": "success", "data": _serialize_model(workflow)}


@app.delete("/api/v1/workflows/{workflow_id}")
async def delete_workflow(
    workflow_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_DELETE)),
):
    """Delete a workflow. Requires workflow-author role and draft status."""
    workflow: Workflow = await Workflow.get_async(workflow_id)
    if workflow.status != WorkflowStatus.DRAFT:
        raise HTTPException(400, "Only draft workflows can be deleted")
    await workflow.delete_async()
    return {"status": "success", "data": None}


@app.post("/api/v1/workflows/{workflow_id}/submit")
async def submit_workflow(
    workflow_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_SUBMIT)),
):
    """Submit a workflow for review."""
    workflow: Workflow = await Workflow.get_async(workflow_id)
    workflow.status = WorkflowStatus.PENDING_REVIEW
    workflow.touch()
    await workflow.save_async()
    return {"status": "success", "data": _serialize_model(workflow)}


@app.post("/api/v1/workflows/{workflow_id}/publish")
async def publish_workflow(
    workflow_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_PUBLISH)),
):
    """Publish a workflow to the registry."""
    workflow: Workflow = await Workflow.get_async(workflow_id)
    workflow.status = WorkflowStatus.PRODUCTION
    workflow.published_at = datetime.now(timezone.utc).timestamp()
    workflow.touch()
    await workflow.save_async()
    return {"status": "success", "data": _serialize_model(workflow)}


@app.get("/api/v1/workflows/{workflow_id}/versions")
async def list_workflow_versions(
    workflow_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    """List versions of a workflow."""
    versions = await WorkflowVersion.filter_async(
        workflow_id=workflow_id, order_by="-published_at"
    )
    return {"status": "success", "data": [_serialize_model(v) for v in versions]}
