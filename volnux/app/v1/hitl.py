from typing import Optional
from datetime import datetime, timezone
from fastapi import Depends, Query, Path, Body

from volnux.models import HITLRequest
from volnux.models.enums import HITLStatus
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.get("/api/v1/hitl/requests")
async def list_hitl_requests(
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    pagination: dict = Depends(Pagination),
    status: Optional[HITLStatus] = Query(None),
    _: None = Depends(require_permission(Permission.HITL_READ)),
):
    """List HITL requests. Requires operator role."""
    filters = {}
    if status:
        filters["status"] = status

    requests = await HITLRequest.filter_async(
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
        order_by="-creation_time",
        **filters,
    )

    return {
        "status": "success",
        "data": [_serialize_model(r) for r in requests],
    }


@app.get("/api/v1/hitl/requests/{request_id}")
async def get_hitl_request(
    request_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.HITL_READ)),
):
    """Get HITL request details. Requires operator role."""
    hitl = await HITLRequest.get_async(request_id)
    return {"status": "success", "data": _serialize_model(hitl)}


@app.post("/api/v1/hitl/requests/{request_id}/respond")
async def respond_hitl(
    request_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.HITL_RESPOND)),
):
    """Respond to a HITL request. Requires operator role."""
    hitl = await HITLRequest.get_async(request_id)
    hitl.status = HITLStatus.RESPONDED
    hitl.decision = data.get("decision")
    hitl.decision_metadata = data.get("metadata", {})
    hitl.resolved_by = user["user_id"]
    hitl.responded_at = datetime.now(timezone.utc).timestamp()
    await hitl.save_async()

    return {"status": "success", "data": _serialize_model(hitl)}
