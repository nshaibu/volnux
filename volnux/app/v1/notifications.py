from typing import Optional
from fastapi import Depends, Path, Body, HTTPException

from volnux.models import NotificationConfig
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.get("/api/v1/notifications")
async def list_notification_configs(
    user: dict = Depends(get_current_user),
):
    """List notification configurations for the current user."""
    configs = await NotificationConfig.filter_async(user_id=user["user_id"])
    return {"status": "success", "data": [_serialize_model(c) for c in configs]}


@app.post("/api/v1/notifications", status_code=201)
async def create_notification_config(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
):
    """Create a notification configuration."""
    data["organization_id"] = org_id
    data["user_id"] = user["user_id"]
    config = NotificationConfig(**data)
    config.save()
    return {"status": "success", "data": _serialize_model(config)}


@app.put("/api/v1/notifications/{config_id}")
async def update_notification_config(
    config_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
):
    """Update a notification configuration."""
    config = await NotificationConfig.get_async(config_id)
    if config.user.id != user["user_id"]:
        raise HTTPException(403, "Can only update your own notification configs")

    immutable = ("id", "user_id", "organization_id", "creation_time")
    for key, value in data.items():
        if hasattr(config, key) and key not in immutable:
            setattr(config, key, value)
    await config.save_async()
    return {"status": "success", "data": _serialize_model(config)}


@app.delete("/api/v1/notifications/{config_id}")
async def delete_notification_config(
    config_id: str = Path(...),
    user: dict = Depends(get_current_user),
):
    """Delete a notification configuration."""
    config = await NotificationConfig.get_async(config_id)
    if config.user.id != user["user_id"]:
        raise HTTPException(403, "Can only delete your own notification configs")
    await config.delete_async()
    return {"status": "success", "data": None}
