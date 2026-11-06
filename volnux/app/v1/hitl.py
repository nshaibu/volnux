from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Path, Query

from volnux.models import HITLRequest, TeamMember, Workflow
from volnux.models.enums import AuditEventType, HITLStatus
from ..app import get_current_app
from ..dependencies import Pagination, _serialize_model, ensure_same_org, meta
from ..permission import Permission
from ..schemas import HITLRespond
from ..utils import (
    RequestContext,
    audit,
    get_current_user,
    is_super_admin,
    require_permission,
)

app = get_current_app()

_OPEN = (
    HITLStatus.PENDING,
    HITLStatus.ASSIGNED,
    HITLStatus.ESCALATED,
    HITLStatus.REROUTED,
)


async def _load_request(ctx: RequestContext, request_id: str) -> HITLRequest:
    """A request of the caller's organization (scoped through its workflow)."""
    hitl = await HITLRequest.get_async(request_id)
    workflow = await Workflow.get_async(hitl.workflow.id)
    ensure_same_org(ctx, workflow.organization.id, "HITL request")
    return hitl


async def _may_respond(ctx: RequestContext, hitl: HITLRequest) -> bool:
    if is_super_admin(ctx):
        return True
    if hitl.assigned_user is not None:
        return hitl.assigned_user.id == ctx.user.id
    if ctx.user.id in (hitl.eligible_users or []):
        return True
    if hitl.assigned_role and hitl.assigned_role in ctx.roles:
        return True
    if hitl.assigned_team is not None:
        return bool(
            await TeamMember.filter_async(
                team__object_id=hitl.assigned_team.id, user__object_id=ctx.user.id
            )
        )
    # nobody in particular: any operator of the organization
    return not (hitl.assigned_role or hitl.eligible_users)


@app.get("/api/v1/hitl/requests")
async def list_hitl_requests(
    pagination: dict = Depends(Pagination),
    status: Optional[HITLStatus] = Query(None),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.HITL_READ)),
):
    org_workflows = {
        w.id
        for w in await Workflow.filter_async(
            organization__object_id=ctx.organization.id
        )
    }
    filters = {"status": status} if status else {}
    requests = [
        r
        for r in await HITLRequest.filter_async(order_by="-created_at", **filters)
        if r.workflow.id in org_workflows
    ]
    start = (pagination["page"] - 1) * pagination["page_size"]
    window = requests[start : start + pagination["page_size"]]
    return {
        "status": "success",
        "data": [_serialize_model(r) for r in window],
        "meta": meta(pagination, len(requests)),
    }


@app.get("/api/v1/hitl/requests/{request_id}")
async def get_hitl_request(
    request_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.HITL_READ)),
):
    hitl = await _load_request(ctx, request_id)
    return {"status": "success", "data": _serialize_model(hitl)}


@app.post("/api/v1/hitl/requests/{request_id}/respond")
async def respond_hitl(
    data: HITLRespond,
    request_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.HITL_RESPOND)),
):
    hitl = await _load_request(ctx, request_id)
    if hitl.status not in _OPEN:
        raise HTTPException(
            409, f"The request is already {getattr(hitl.status, 'value', hitl.status)}"
        )
    now = datetime.now(timezone.utc).timestamp()
    if hitl.sla_deadline is not None and now > hitl.sla_deadline:
        hitl.status = HITLStatus.EXPIRED
        await hitl.save_async()
        await audit(
            ctx,
            AuditEventType.HITL_SLA_BREACHED,
            "hitl_request",
            hitl.id,
            "responded after the SLA deadline",
        )
        raise HTTPException(409, "The SLA deadline has passed")
    if not await _may_respond(ctx, hitl):
        raise HTTPException(403, "This request is not assigned to you")
    if hitl.available_actions and data.decision not in hitl.available_actions:
        raise HTTPException(
            422, f"decision must be one of {list(hitl.available_actions)}"
        )
    missing = [
        i.get("name")
        for i in hitl.required_inputs
        if i.get("required", True) and i.get("name") not in data.response_data
    ]
    if missing:
        raise HTTPException(422, f"Missing required inputs: {missing}")

    hitl.decision = data.decision
    hitl.decision_metadata = {
        **data.response_data,
        **({"comments": data.comments} if data.comments else {}),
    }
    hitl.status = HITLStatus.RESPONDED
    hitl.resolved_by = ctx.user
    hitl.responded_at = now
    await hitl.save_async()
    await audit(
        ctx,
        AuditEventType.HITL_RESPONDED,
        "hitl_request",
        hitl.id,
        f"responded: {data.decision}",
    )
    return {"status": "success", "data": _serialize_model(hitl)}
