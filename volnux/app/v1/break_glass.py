from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Path

from volnux.models import BreakGlassAccess, Execution, Workflow
from volnux.models.enums import AuditEventType, BreakGlassReviewStatus
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import BreakGlassInitiate, BreakGlassReview
from ..utils import RequestContext, audit, get_current_user, require_permission

app = get_current_app()


async def _load_access(ctx: RequestContext, access_id: str) -> BreakGlassAccess:
    access = await BreakGlassAccess.get_async(access_id)
    ensure_same_org(ctx, access.organization.id, "Break-glass access")
    return access


@app.post("/api/v1/break-glass", status_code=201)
async def initiate_break_glass(
    data: BreakGlassInitiate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.BREAK_GLASS_INITIATE)),
):
    """Emergency access, recorded with a justification for later review."""
    workflow = execution = None
    if data.workflow_id:
        workflow = await Workflow.get_async(data.workflow_id)
        ensure_same_org(ctx, workflow.organization.id, "Workflow")
    if data.execution_id:
        execution = await Execution.get_async(data.execution_id)
        ensure_same_org(ctx, execution.organization.id, "Execution")
    access = BreakGlassAccess(
        super_admin=ctx.user,
        organization=ctx.organization,
        workflow=workflow,
        execution=execution,
        justification=data.justification,
        reviewed_by=None,
    )
    await access.save_async()
    await audit(
        ctx,
        AuditEventType.BREAK_GLASS_INITIATED,
        "break_glass",
        access.id,
        data.justification,
        {"workflow": data.workflow_id, "execution": data.execution_id},
    )
    return {"status": "success", "data": _serialize_model(access)}


@app.get("/api/v1/break-glass")
async def list_break_glass(
    pagination: dict = Depends(Pagination),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.BREAK_GLASS_REVIEW)),
):
    scope = {"organization__object_id": ctx.organization.id}
    items = await BreakGlassAccess.filter_async(
        **page_args(pagination), order_by="-created_at", **scope
    )
    return {
        "status": "success",
        "data": [_serialize_model(a) for a in items],
        "meta": meta(pagination, await BreakGlassAccess.count_async(**scope)),
    }


@app.get("/api/v1/break-glass/{access_id}")
async def get_break_glass(
    access_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.BREAK_GLASS_REVIEW)),
):
    return {
        "status": "success",
        "data": _serialize_model(await _load_access(ctx, access_id)),
    }


@app.post("/api/v1/break-glass/{access_id}/review")
async def review_break_glass(
    data: BreakGlassReview,
    access_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.BREAK_GLASS_REVIEW)),
):
    access = await _load_access(ctx, access_id)
    if access.review_status != BreakGlassReviewStatus.PENDING:
        raise HTTPException(409, "This access has already been reviewed")
    if access.super_admin.id == ctx.user.id:
        raise HTTPException(403, "You cannot review your own break-glass access")
    access.review_status = data.status
    access.review_findings = data.findings
    access.review_remediation = data.remediation
    access.reviewed_by = ctx.user
    access.reviewed_at = datetime.now(timezone.utc).timestamp()
    await access.save_async()
    await audit(
        ctx,
        AuditEventType.BREAK_GLASS_REVIEWED,
        "break_glass",
        access.id,
        f"reviewed: {data.status.value}",
    )
    return {"status": "success", "data": _serialize_model(access)}
