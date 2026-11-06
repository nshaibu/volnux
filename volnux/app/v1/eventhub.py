from typing import Optional

from fastapi import Depends, HTTPException, Path, Query

from volnux.models import Event, EventVersion, Team
from volnux.models.enums import AuditEventType, EventStatus
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import EventPublish
from ..utils import RequestContext, audit, get_current_user, require_permission

app = get_current_app()


async def _load_event(ctx: RequestContext, component_id: str) -> Event:
    event = await Event.get_async(component_id)
    ensure_same_org(ctx, event.organization.id, "Event")
    return event


@app.get("/api/v1/eventhub/components")
async def list_events(
    pagination: dict = Depends(Pagination),
    status: Optional[EventStatus] = Query(None),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EVENT_READ)),
):
    filters = {"organization__object_id": ctx.organization.id}
    if status:
        filters["status"] = status
    events = await Event.filter_async(**page_args(pagination), **filters)
    return {
        "status": "success",
        "data": [_serialize_model(e) for e in events],
        "meta": meta(pagination, await Event.count_async(**filters)),
    }


@app.post("/api/v1/eventhub/components", status_code=201)
async def publish_event(
    data: EventPublish,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EVENT_PUBLISH)),
):
    if await Event.filter_async(
        organization__object_id=ctx.organization.id,
        name=data.name,
        version=data.version,
    ):
        raise HTTPException(409, "This version of the event is already published")
    team = None
    if data.team_id:
        team = await Team.get_async(data.team_id)
        ensure_same_org(ctx, team.organization.id, "Team")
    event = Event(
        name=data.name,
        namespace=data.namespace,
        version=data.version,
        manifest=data.manifest,
        organization=ctx.organization,
        team=team,
        publisher=ctx.user,
    )
    await event.save_async()
    await EventVersion(
        event=event,
        version_number=data.version,
        manifest=data.manifest,
        changelog=data.changelog,
        published_by=ctx.user,
    ).save_async()
    await audit(
        ctx,
        AuditEventType.EVENT_PUBLISHED,
        "event",
        event.id,
        f"published {data.name} {data.version}",
    )
    return {"status": "success", "data": _serialize_model(event)}


@app.get("/api/v1/eventhub/components/{component_id}")
async def get_event(
    component_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EVENT_READ)),
):
    return {
        "status": "success",
        "data": _serialize_model(await _load_event(ctx, component_id)),
    }


@app.get("/api/v1/eventhub/components/{component_id}/versions")
async def list_event_versions(
    component_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.EVENT_READ)),
):
    await _load_event(ctx, component_id)
    versions = await EventVersion.filter_async(
        event__object_id=component_id, order_by="-published_at"
    )
    return {"status": "success", "data": [_serialize_model(v) for v in versions]}
