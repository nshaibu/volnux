from datetime import datetime
from typing import Optional

from fastapi import Depends, Query

from volnux.models import AuditEntry
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import Pagination, _serialize_model, meta
from ..permission import Permission
from ..utils import (
    RequestContext,
    get_current_user,
    require_permission,
    verify_audit_chain,
)

app = get_current_app()


@app.get("/api/v1/audit/entries")
async def list_audit_entries(
    pagination: dict = Depends(Pagination),
    event_type: Optional[AuditEventType] = Query(None),
    actor_id: Optional[str] = Query(None),
    target_type: Optional[str] = Query(None),
    target_id: Optional[str] = Query(None),
    since: Optional[datetime] = Query(None),
    until: Optional[datetime] = Query(None),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.AUDIT_READ)),
):
    """The organization's audit trail (read-only; entries can never be changed here)."""
    filters = {"organization__object_id": ctx.organization.id}
    if event_type:
        filters["event_type"] = event_type
    if actor_id:
        filters["actor__object_id"] = actor_id
    if target_type:
        filters["target_type"] = target_type
    if target_id:
        filters["target_id"] = target_id

    entries = await AuditEntry.filter_async(order_by="-created_at", **filters)
    if since:
        entries = [e for e in entries if e.created_at >= since]
    if until:
        entries = [e for e in entries if e.created_at <= until]
    start = (pagination["page"] - 1) * pagination["page_size"]
    window = entries[start : start + pagination["page_size"]]
    return {
        "status": "success",
        "data": [_serialize_model(e) for e in window],
        "meta": meta(pagination, len(entries)),
    }


@app.get("/api/v1/audit/verify")
async def verify_audit_trail(
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.AUDIT_READ)),
):
    """Whether the organization's audit hash chain is intact."""
    return {
        "status": "success",
        "data": {"intact": await verify_audit_chain(ctx.organization.id)},
    }
