from fastapi import Depends, Path

from volnux.models import NotificationConfig
from ..app import get_current_app
from ..dependencies import _serialize_model, not_found
from ..schemas import NotificationConfigCreate, NotificationConfigUpdate
from ..utils import RequestContext, get_current_user

app = get_current_app()

# Notification settings are personal: every authenticated user manages their own, and
# only their own (the owner check below), so no role permission is needed.


async def _own(ctx: RequestContext, config_id: str) -> NotificationConfig:
    config = await NotificationConfig.get_async(config_id)
    if config.user.id != ctx.user.id:
        raise not_found("Notification config")
    return config


@app.get("/api/v1/notifications")
async def list_notification_configs(
    ctx: RequestContext = Depends(get_current_user),
):
    configs = await NotificationConfig.filter_async(user__object_id=ctx.user.id)
    return {"status": "success", "data": [_serialize_model(c) for c in configs]}


@app.post("/api/v1/notifications", status_code=201)
async def create_notification_config(
    data: NotificationConfigCreate,
    ctx: RequestContext = Depends(get_current_user),
):
    config = NotificationConfig(
        user=ctx.user, organization=ctx.organization, **data.model_dump()
    )
    await config.save_async()
    return {"status": "success", "data": _serialize_model(config)}


@app.put("/api/v1/notifications/{config_id}")
async def update_notification_config(
    data: NotificationConfigUpdate,
    config_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
):
    config = await _own(ctx, config_id)
    for name, value in data.model_dump(exclude_unset=True).items():
        setattr(config, name, value)
    await config.save_async()
    return {"status": "success", "data": _serialize_model(config)}


@app.delete("/api/v1/notifications/{config_id}")
async def delete_notification_config(
    config_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
):
    config = await _own(ctx, config_id)
    await config.delete_async()
    return {"status": "success", "data": None}
