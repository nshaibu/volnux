from datetime import datetime, timezone

from fastapi import Depends, Path

from volnux.models import MeshNode, NodeHeartbeat
from volnux.models.enums import AuditEventType, NodeStatus
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import NodeRegister
from ..utils import RequestContext, audit, get_current_user, require_permission

app = get_current_app()


async def _load_node(ctx: RequestContext, node_id: str) -> MeshNode:
    node = await MeshNode.get_async(node_id)
    ensure_same_org(ctx, node.organization.id, "Node")
    return node


@app.get("/api/v1/nodes")
async def list_nodes(
    pagination: dict = Depends(Pagination),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_READ)),
):
    scope = {"organization__object_id": ctx.organization.id}
    nodes = await MeshNode.filter_async(**page_args(pagination), **scope)
    return {
        "status": "success",
        "data": [_serialize_model(n) for n in nodes],
        "meta": meta(pagination, await MeshNode.count_async(**scope)),
    }


@app.post("/api/v1/nodes", status_code=201)
async def register_node(
    data: NodeRegister,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_REGISTER)),
):
    """Register a node. It starts OFFLINE: only its heartbeats make it healthy."""
    node = MeshNode(
        **data.model_dump(),
        organization=ctx.organization,
        registered_by=ctx.user,
        decommissioned_by=None,
    )
    await node.save_async()
    await audit(
        ctx,
        AuditEventType.NODE_REGISTERED,
        "node",
        node.id,
        f"registered {node.name}",
    )
    return {"status": "success", "data": _serialize_model(node)}


@app.get("/api/v1/nodes/{node_id}")
async def get_node(
    node_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_READ)),
):
    return {
        "status": "success",
        "data": _serialize_model(await _load_node(ctx, node_id)),
    }


@app.get("/api/v1/nodes/{node_id}/health")
async def get_node_health(
    node_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_READ)),
):
    await _load_node(ctx, node_id)
    beats = await NodeHeartbeat.filter_async(
        node__object_id=node_id, order_by="-recorded_at", limit=100
    )
    return {"status": "success", "data": [_serialize_model(b) for b in beats]}


@app.post("/api/v1/nodes/{node_id}/decommission")
async def decommission_node(
    node_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_DECOMMISSION)),
):
    node = await _load_node(ctx, node_id)
    if node.status != NodeStatus.DECOMMISSIONED:
        node.status = NodeStatus.DECOMMISSIONED
        node.decommissioned_by = ctx.user
        node.decommissioned_at = datetime.now(timezone.utc).timestamp()
        await node.save_async()
        await audit(
            ctx,
            AuditEventType.NODE_DECOMMISSIONED,
            "node",
            node.id,
            f"decommissioned {node.name}",
        )
    return {"status": "success", "data": _serialize_model(node)}
