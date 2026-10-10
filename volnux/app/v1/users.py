from typing import Optional
from fastapi import Depends, Query, Body, Path, HTTPException

from volnux.models import User, RoleAssignment
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.post("/api/v1/users", status_code=201)
async def create_user(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_CREATE)),
):
    """Create a new user. Requires user-create permission."""
    data["organization_id"] = org_id
    new_user = User(**data)
    new_user.save()
    return {"status": "success", "data": _serialize_model(new_user)}


@app.get("/api/v1/users")
async def list_users(
    org_id: str = Depends(get_org_id),
    pagination: dict = Depends(Pagination),
    is_active: Optional[bool] = Query(None),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_READ)),
):
    """List users in the organization. Requires user-read permission."""
    filters = {"organization_id": org_id}
    if is_active is not None:
        filters["is_active"] = is_active

    users = User.filter(
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
        **filters,
    )
    total = User.count(**filters)

    return {
        "status": "success",
        "data": [_serialize_model(u) for u in users],
        "meta": {
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "total": total,
        },
    }


@app.get("/api/v1/users/{user_id}")
async def get_user(
    user_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_READ)),
):
    """Get user by ID. Requires user-read permission."""
    target = User.get(user_id)
    if (
        target.organization_id != user["organization_id"]
        and "super-admin" not in user["roles"]
    ):
        raise HTTPException(403, "Access denied")
    return {"status": "success", "data": _serialize_model(target)}


@app.put("/api/v1/users/{user_id}")
async def update_user(
    user_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_UPDATE)),
):
    """Update a user. Requires user-update permission."""
    target = User.get(user_id)
    if target.organization_id != user["organization_id"]:
        raise HTTPException(403, "Access denied")

    immutable = ("id", "organization_id", "creation_time")
    for key, value in data.items():
        if hasattr(target, key) and key not in immutable:
            setattr(target, key, value)
    target.touch()
    target.save()
    return {"status": "success", "data": _serialize_model(target)}


@app.delete("/api/v1/users/{user_id}")
async def delete_user(
    user_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_DELETE)),
):
    """Delete a user. Requires user-delete permission."""
    target = User.get(user_id)
    if target.organization_id != user["organization_id"]:
        raise HTTPException(403, "Access denied")
    target.delete()
    return {"status": "success", "data": None}


@app.get("/api/v1/users/{user_id}/roles")
async def get_user_roles(
    user_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_READ)),
):
    """Get role assignments for a user."""
    target = User.get(user_id)
    if target.organization_id != user["organization_id"]:
        raise HTTPException(403, "Access denied")

    assignments = RoleAssignment.filter(user_id=user_id)
    return {"status": "success", "data": [_serialize_model(a) for a in assignments]}
