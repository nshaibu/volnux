from typing import Optional
from fastapi import Depends, Body, Path, HTTPException

from volnux.models import Organization
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.post("/api/v1/organizations", status_code=201)
async def create_organization(
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.SUPER_ADMIN)),
):
    """Create a new organization. Requires super-admin."""
    org = Organization(**data)
    await org.save_async()
    return {"status": "success", "data": _serialize_model(org)}


@app.get("/api/v1/organizations/{org_id}")
async def get_organization(
    org_id: str = Path(...),
    user: dict = Depends(get_current_user),
):
    """Get organization by ID. User must belong to the organization."""
    org = await Organization.get_async(org_id)
    if org_id != user["organization_id"] and "super-admin" not in user["roles"]:
        raise HTTPException(403, "Access denied to this organization")
    return {"status": "success", "data": _serialize_model(org)}


@app.put("/api/v1/organizations/{org_id}")
async def update_organization(
    org_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.ORG_MANAGE)),
):
    """Update an organization. Requires org-admin."""
    org = await Organization.get_async(org_id)
    if org_id != user["organization_id"]:
        raise HTTPException(403, "Access denied to this organization")

    for key, value in data.items():
        if hasattr(org, key) and key not in ("id", "creation_time"):
            setattr(org, key, value)
    org.touch()
    await org.save_async()
    return {"status": "success", "data": _serialize_model(org)}


@app.delete("/api/v1/organizations/{org_id}")
async def delete_organization(
    org_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.SUPER_ADMIN)),
):
    """Delete an organization. Requires super-admin."""
    org = await Organization.get_async(org_id)
    await org.delete_async()
    return {"status": "success", "data": None}
