from typing import Optional
from fastapi import Depends, Query

from volnux.models import AuditEntry
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.get("/api/v1/audit/entries")
async def list_audit_entries(
    org_id: str = Depends(get_org_id),
    pagination: dict = Depends(Pagination),
    event_type: Optional[AuditEventType] = Query(None),
    actor_id: Optional[str] = Query(None),
    target_type: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.AUDIT_READ)),
):
    """Query audit log entries. Requires auditor role."""
    filters = {"organization_id": org_id}
    if event_type:
        filters["event_type"] = event_type
    if actor_id:
        filters["actor_id"] = actor_id
    if target_type:
        filters["target_type"] = target_type

    entries = await AuditEntry.filter_async(
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
        order_by="-creation_time",
        **filters,
    )
    total = await AuditEntry.count_async(**filters)

    return {
        "status": "success",
        "data": [_serialize_model(e) for e in entries],
        "meta": {
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "total": total,
        },
    }
