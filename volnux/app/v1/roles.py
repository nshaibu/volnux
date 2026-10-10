from datetime import datetime, timezone
from fastapi import Depends, Body, Path, HTTPException

from volnux.models import Role, RoleAssignment, User
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.post("/api/v1/roles", status_code=201)
async def create_role(
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.SUPER_ADMIN)),
):
    """Create a new role. Requires super-admin."""
    role = Role(**data)
    await role.save_async()
    return {"status": "success", "data": _serialize_model(role)}


@app.get("/api/v1/roles")
async def list_roles(
    pagination: dict = Depends(Pagination),
    user: dict = Depends(get_current_user),
):
    """List all roles."""
    roles = Role.filter(
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
    )
    total = Role.count()
    return {
        "status": "success",
        "data": [_serialize_model(r) for r in roles],
        "meta": {
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "total": total,
        },
    }


@app.post("/api/v1/roles/assign", status_code=201)
async def assign_role(
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.ROLE_ASSIGN)),
):
    """Assign a role to a user. Requires role-assign permission."""
    # Verify the target user is in the same organization
    target_user: User = await User.get_async(data["user_id"])
    if target_user.organization.id != user["organization_id"]:
        raise HTTPException(403, "Cannot assign roles across organizations")

    data["granted_by"] = user["user_id"]
    assignment = RoleAssignment(**data)
    await assignment.save_async()
    return {"status": "success", "data": _serialize_model(assignment)}


@app.delete("/api/v1/roles/assign/{assignment_id}")
async def revoke_role(
    assignment_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.ROLE_REVOKE)),
):
    """Revoke a role assignment. Requires role-revoke permission."""
    assignment: RoleAssignment = await RoleAssignment.get_async(assignment_id)
    assignment.revoked_at = datetime.now(timezone.utc).timestamp()
    await assignment.save_async()
    return {"status": "success", "data": None}
