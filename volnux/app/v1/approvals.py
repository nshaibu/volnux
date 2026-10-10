from datetime import datetime, timezone
from fastapi import Depends, Path, Body, HTTPException

from volnux.models import ApprovalStep, ApprovalChain, Workflow
from volnux.models.enums import ApprovalStatus, WorkflowStatus
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user, RequestContext

app = get_current_app()


@app.get("/api/v1/approvals/pending", tags=["Approvals"])
async def list_pending_approvals(
    org_id: str = Depends(get_org_id),
    request: RequestContext = Depends(get_current_user),
    pagination: dict = Depends(Pagination),
    _: None = Depends(require_permission(Permission.APPROVAL_REVIEW)),
):
    """List workflows pending the current user's review. Requires reviewer or compliance approver."""
    steps = await ApprovalStep.filter_async(
        status=ApprovalStatus.PENDING,
        individual_id=request.user.id,
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
    )
    return {"status": "success", "data": [_serialize_model(s) for s in steps]}


@app.post("/api/v1/approvals/{workflow_id}/approve", tags=["Approvals"])
async def approve_workflow(
    workflow_id: str = Path(...),
    data: dict = Body(default={}),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_APPROVE)),
):
    """Approve a workflow at the current step. Requires reviewer or compliance approver."""
    steps = await ApprovalStep.filter_async(
        workflow_id=workflow_id,
        individual_id=user["user_id"],
        status=ApprovalStatus.PENDING,
    )
    if not steps:
        raise HTTPException(404, "No pending approval step found for this user")

    step = steps[0]
    step.status = ApprovalStatus.APPROVED
    step.decided_by = user["user_id"]
    step.decision = "approved"
    step.comments = data.get("comments", "")
    step.decided_at = datetime.now(timezone.utc).timestamp()
    await step.save_async()

    return {"status": "success", "data": _serialize_model(step)}


@app.post("/api/v1/approvals/{workflow_id}/reject", tags=["Approvals"])
async def reject_workflow(
    workflow_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_REJECT)),
):
    """Reject a workflow with reason. Requires reviewer or compliance approver."""
    steps = await ApprovalStep.filter_async(
        workflow_id=workflow_id,
        individual_id=user["user_id"],
        status=ApprovalStatus.PENDING,
    )
    if not steps:
        raise HTTPException(404, "No pending approval step found for this user")

    step = steps[0]
    step.status = ApprovalStatus.REJECTED
    step.decided_by = user["user_id"]
    step.decision = "rejected"
    step.comments = data.get("reason", "")
    step.decided_at = datetime.now(timezone.utc).timestamp()
    await step.save_async()

    workflow = await Workflow.get_async(workflow_id)
    workflow.status = WorkflowStatus.REJECTED
    await workflow.save_async()

    return {"status": "success", "data": _serialize_model(step)}


@app.get("/api/v1/approvals/chains", tags=["Approvals"])
async def list_approval_chains(
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
):
    """List configured approval chains."""
    chains = await ApprovalChain.filter_async(organization_id=org_id)
    return {"status": "success", "data": [_serialize_model(c) for c in chains]}


@app.post("/api/v1/approvals/chains", status_code=201, tags=["Approvals"])
async def create_approval_chain(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_MANAGE)),
):
    """Create an approval chain. Requires org-admin."""
    data["organization_id"] = org_id
    data["created_by"] = user["user_id"]
    chain = ApprovalChain(**data)
    await chain.save_async()
    return {"status": "success", "data": _serialize_model(chain)}
