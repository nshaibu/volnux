from typing import Optional
from fastapi import Depends, Query, Body, Path

from volnux.models import TriggerConfig
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.get("/api/v1/triggers")
async def list_triggers(
    org_id: str = Depends(get_org_id),
    workflow_id: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    """List trigger configurations."""
    filters = {"organization_id": org_id}
    if workflow_id:
        filters["workflow_id"] = workflow_id

    triggers: list[TriggerConfig] = await TriggerConfig.filter_async(**filters)
    return {"status": "success", "data": [_serialize_model(t) for t in triggers]}


@app.post("/api/v1/triggers", status_code=201)
async def create_trigger(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TRIGGER_CREATE)),
):
    """Create a trigger configuration. Requires org-admin."""
    data["organization_id"] = org_id
    data["created_by"] = user["user_id"]
    trigger = TriggerConfig(**data)
    await trigger.save_async()
    return {"status": "success", "data": _serialize_model(trigger)}


@app.delete("/api/v1/triggers/{trigger_id}")
async def delete_trigger(
    trigger_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TRIGGER_DELETE)),
):
    """Delete a trigger configuration. Requires org-admin."""
    trigger = await TriggerConfig.get_async(trigger_id)
    await trigger.delete_async()
    return {"status": "success", "data": None}
