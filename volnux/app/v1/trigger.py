import logging
from typing import Optional

from fastapi import Depends, HTTPException, Path, Query, Request

from volnux.models import TriggerConfig
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import TriggerCreate, TriggerUpdate
from ..trigger_factory import (
    TriggerConfigError,
    register_trigger,
    unregister_trigger,
)
from ..utils import RequestContext, audit, get_current_user, require_permission
from .workflows import load_workflow

logger = logging.getLogger(__name__)
app = get_current_app()

MAX_HOOK_BODY = 1024 * 1024  # 1 MiB


async def _load_trigger(ctx: RequestContext, trigger_id: str) -> TriggerConfig:
    trigger = await TriggerConfig.get_async(trigger_id)
    ensure_same_org(ctx, trigger.organization.id, "Trigger")
    return trigger


def _engine_trigger(trigger: TriggerConfig):
    from volnux.engine.workflows.trigger import get_trigger_engine

    if not trigger.engine_trigger_id:
        return None
    return get_trigger_engine().triggers.get(trigger.engine_trigger_id)


@app.get("/api/v1/triggers")
async def list_triggers(
    pagination: dict = Depends(Pagination),
    workflow_id: Optional[str] = Query(None),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    filters = {"organization__object_id": ctx.organization.id}
    if workflow_id:
        filters["workflow__object_id"] = workflow_id
    triggers = await TriggerConfig.filter_async(**page_args(pagination), **filters)
    return {
        "status": "success",
        "data": [_serialize_model(t) for t in triggers],
        "meta": meta(pagination, await TriggerConfig.count_async(**filters)),
    }


@app.post("/api/v1/triggers", status_code=201)
async def create_trigger(
    data: TriggerCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TRIGGER_CREATE)),
):
    """Store a trigger and register it with the engine, so the workflow runs when it fires."""
    from volnux.engine.workflows.workflow import WorkflowNotFound

    workflow = await load_workflow(ctx, data.workflow_id)
    try:
        live = await register_trigger(
            data.trigger_type, workflow.name, data.config, enabled=data.is_active
        )
    except TriggerConfigError as exc:
        raise HTTPException(422, str(exc))
    except WorkflowNotFound:
        raise HTTPException(409, "The workflow is not loaded in the engine")
    except ValueError as exc:
        raise HTTPException(422, str(exc))

    trigger = TriggerConfig(
        workflow=workflow,
        organization=ctx.organization,
        trigger_type=data.trigger_type,
        config=data.config,
        is_active=data.is_active,
        created_by=ctx.user,
        engine_trigger_id=live.trigger_id,
    )
    try:
        await trigger.save_async()
    except Exception:
        await unregister_trigger(live.trigger_id)
        raise
    await audit(
        ctx,
        AuditEventType.TRIGGER_CREATED,
        "trigger",
        trigger.id,
        f"created a {data.trigger_type.value} trigger for {workflow.name}",
        {"workflow": workflow.id, "engine_trigger": live.trigger_id},
    )
    return {"status": "success", "data": _serialize_model(trigger)}


@app.put("/api/v1/triggers/{trigger_id}")
async def update_trigger(
    data: TriggerUpdate,
    trigger_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TRIGGER_CREATE)),
):
    """Enable or disable a trigger (a disabled trigger stays registered but never fires)."""
    trigger = await _load_trigger(ctx, trigger_id)
    live = _engine_trigger(trigger)
    if live is not None:
        live.state.enabled = data.is_active
        await live.state.save_async()
    trigger.is_active = data.is_active
    await trigger.save_async()
    await audit(
        ctx,
        AuditEventType.TRIGGER_UPDATED,
        "trigger",
        trigger.id,
        "enabled" if data.is_active else "disabled",
    )
    return {"status": "success", "data": _serialize_model(trigger)}


@app.delete("/api/v1/triggers/{trigger_id}")
async def delete_trigger(
    trigger_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TRIGGER_DELETE)),
):
    trigger = await _load_trigger(ctx, trigger_id)
    await unregister_trigger(trigger.engine_trigger_id)
    await trigger.delete_async()
    await audit(
        ctx,
        AuditEventType.TRIGGER_DELETED,
        "trigger",
        trigger_id,
        "deleted a trigger",
    )
    return {"status": "success", "data": None}


@app.post("/api/v1/hooks/{engine_trigger_id}", tags=["Webhooks"])
async def receive_webhook(request: Request, engine_trigger_id: str = Path(...)):
    """Where a webhook trigger receives calls. Not behind a login: the sender proves
    itself with the HMAC signature of the trigger's secret over the raw body."""
    from volnux.engine.workflows.trigger import get_trigger_engine
    from volnux.engine.workflows.trigger.triggers import WebhookTrigger

    trigger = get_trigger_engine().triggers.get(engine_trigger_id)
    if not isinstance(trigger, WebhookTrigger):
        raise HTTPException(404, "Unknown webhook")
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > MAX_HOOK_BODY:
        raise HTTPException(413, "The request body is too large")
    body = await request.body()
    if len(body) > MAX_HOOK_BODY:
        raise HTTPException(413, "The request body is too large")
    try:
        accepted = await trigger.handle_webhook(body, headers=dict(request.headers))
    except PermissionError:
        raise HTTPException(401, "Invalid signature")
    return {"status": "accepted" if accepted else "ignored"}
