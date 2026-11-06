from datetime import datetime, timezone
from fastapi import Depends, Path, HTTPException

from volnux.models import Role, RoleAssignment, Team, User
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
from ..schemas import RoleAssign, RoleCreate
from ..utils import (
    RequestContext,
    audit,
    get_current_user,
    is_super_admin,
    require_permission,
)

app = get_current_app()


@app.post("/api/v1/roles", status_code=201)
async def create_role(
    data: RoleCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.SUPER_ADMIN)),
):
    """Create a new role. Requires super-admin."""
    if await Role.filter_async(slug=data.slug):
        raise HTTPException(409, "A role with this slug already exists")
    role = Role(**data.model_dump(mode="json"))
    await role.save_async()
    return {"status": "success", "data": _serialize_model(role)}


@app.get("/api/v1/roles")
async def list_roles(
    pagination: dict = Depends(Pagination),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.ROLE_ASSIGN)),
):
    """List all roles."""
    roles = await Role.filter_async(**page_args(pagination))
    return {
        "status": "success",
        "data": [_serialize_model(r) for r in roles],
        "meta": meta(pagination, await Role.count_async()),
    }


@app.post("/api/v1/roles/assign", status_code=201)
async def assign_role(
    data: RoleAssign,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.ROLE_ASSIGN)),
):
    """Assign a role to a user in the caller's organization."""
    target = await User.get_async(data.user_id)
    ensure_same_org(ctx, target.organization.id, "User")
    role = await Role.get_async(data.role_id)
    if role.slug == "super-admin" and not is_super_admin(ctx):
        raise HTTPException(403, "Only a super-admin can grant super-admin")
    team = None
    if data.team_id:
        team = await Team.get_async(data.team_id)
        ensure_same_org(ctx, team.organization.id, "Team")
    assignment = RoleAssignment(
        user=target,
        role=role,
        scope_type=data.scope_type,
        team=team,
        environment=data.environment,
        granted_by=ctx.user,
        revoked_at=None,
    )
    await assignment.save_async()
    await audit(
        ctx,
        AuditEventType.ROLE_ASSIGNED,
        "user",
        target.id,
        f"assigned role {role.slug}",
        {"role": role.slug, "assignment": assignment.id},
    )
    return {"status": "success", "data": _serialize_model(assignment)}


@app.delete("/api/v1/roles/assign/{assignment_id}")
async def revoke_role(
    assignment_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.ROLE_REVOKE)),
):
    """Revoke a role assignment in the caller's organization."""
    assignment = await RoleAssignment.get_async(assignment_id)
    target = await User.get_async(assignment.user.id)
    ensure_same_org(ctx, target.organization.id, "Role assignment")
    if assignment.revoked_at is None:
        assignment.revoked_at = datetime.now(timezone.utc)
        await assignment.save_async()
        await audit(
            ctx,
            AuditEventType.ROLE_REVOKED,
            "user",
            target.id,
            "revoked a role assignment",
            {"assignment": assignment.id},
        )
    return {"status": "success", "data": None}
