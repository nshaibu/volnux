from typing import Optional
from fastapi import Depends, Query, Body, Path

from volnux.models import Event, EventVersion
from volnux.models.enums import NamespaceType, EventStatus
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.get("/api/v1/eventhub/components")
async def list_events(
    org_id: str = Depends(get_org_id),
    pagination: dict = Depends(Pagination),
    namespace: Optional[NamespaceType] = Query(None),
    status: Optional[EventStatus] = Query(None),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EVENT_READ)),
):
    """Browse the EventHub component registry. Requires eventhub-consumer or publisher."""
    filters = {"organization_id": org_id}
    if namespace:
        filters["namespace"] = namespace
    if status:
        filters["status"] = status

    events = await Event.filter_async(
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
        **filters,
    )
    total = await Event.count_async(**filters)

    return {
        "status": "success",
        "data": [_serialize_model(e) for e in events],
        "meta": {
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "total": total,
        },
    }


@app.post("/api/v1/eventhub/components", status_code=201)
async def publish_event(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EVENT_PUBLISH)),
):
    """Publish a new EventHub component. Requires eventhub-publisher role."""
    data["organization_id"] = org_id
    data["publisher_id"] = user["user_id"]
    event = Event(**data)
    await event.save_async()
    return {"status": "success", "data": _serialize_model(event)}


@app.get("/api/v1/eventhub/components/{component_id}")
async def get_event(
    component_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EVENT_READ)),
):
    """Get EventHub component details."""
    event = await Event.get_async(component_id)
    return {"status": "success", "data": _serialize_model(event)}


@app.get("/api/v1/eventhub/components/{component_id}/versions")
async def list_event_versions(
    component_id: str = Path(...),
    user: dict = Depends(get_current_user),
):
    """List versions of an EventHub component."""
    versions = await EventVersion.filter_async(
        event_id=component_id, order_by="-published_at"
    )
    return {"status": "success", "data": [_serialize_model(v) for v in versions]}
