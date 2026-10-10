from typing import Optional
from datetime import datetime, timezone
from fastapi import Depends, Query, Body, Path

from volnux.models import MeshNode, NodeHeartbeat
from volnux.models.enums import NodeStatus
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.get("/api/v1/nodes")
async def list_nodes(
    org_id: str = Depends(get_org_id),
    status: Optional[NodeStatus] = Query(None),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_READ)),
):
    """List mesh nodes. Requires mesh-node-admin."""
    filters = {"organization_id": org_id}
    if status:
        filters["status"] = status

    nodes = await MeshNode.filter_async(**filters)
    return {"status": "success", "data": [_serialize_model(n) for n in nodes]}


@app.post("/api/v1/nodes", status_code=201)
async def register_node(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_REGISTER)),
):
    """Register a new mesh node. Requires mesh-node-admin."""
    data["organization_id"] = org_id
    data["registered_by"] = user["user_id"]
    node = MeshNode(**data)
    await node.save_async()
    return {"status": "success", "data": _serialize_model(node)}


@app.get("/api/v1/nodes/{node_id}")
async def get_node(
    node_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_READ)),
):
    """Get node details. Requires mesh-node-admin."""
    node = await MeshNode.get_async(node_id)
    return {"status": "success", "data": _serialize_model(node)}


@app.get("/api/v1/nodes/{node_id}/health")
async def get_node_health(
    node_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_READ)),
):
    """Get node health metrics. Requires mesh-node-admin."""
    heartbeats = await NodeHeartbeat.filter_async(
        node_id=node_id,
        order_by="-recorded_at",
        limit=1,
    )
    latest = _serialize_model(heartbeats[0]) if heartbeats else None
    return {"status": "success", "data": latest}


@app.post("/api/v1/nodes/{node_id}/decommission")
async def decommission_node(
    node_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.NODE_DECOMMISSION)),
):
    """Decommission a mesh node. Requires mesh-node-admin."""
    node = await MeshNode.get_async(node_id)
    node.status = NodeStatus.DECOMMISSIONED
    node.decommissioned_by = user["user_id"]
    node.decommissioned_at = datetime.now(timezone.utc).timestamp()
    await node.save_async()
    return {"status": "success", "data": _serialize_model(node)}
