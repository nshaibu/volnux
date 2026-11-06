from datetime import datetime, timezone
from fastapi import Depends, Path, HTTPException

from volnux.models import Delegation, Role, Team, User
from volnux.models.enums import AuditEventType, DelegationStatus
from ..app import get_current_app
from ..dependencies import _serialize_model, ensure_same_org
from ..permission import Permission
from ..schemas import DelegationCreate
from ..utils import (
    RequestContext,
    audit,
    get_current_user,
    has_permission,
    require_permission,
)

app = get_current_app()


@app.post("/api/v1/delegations", status_code=201)
async def create_delegation(
    data: DelegationCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.DELEGATION_CREATE)),
):
    """Delegate one of your own roles to another user for a bounded time."""
    if data.end_time <= data.start_time:
        raise HTTPException(422, "end_time must be after start_time")
    if data.end_time <= datetime.now(timezone.utc).timestamp():
        raise HTTPException(422, "end_time is in the past")
    delegate = await User.get_async(data.delegate_id)
    ensure_same_org(ctx, delegate.organization.id, "User")
    if delegate.id == ctx.user.id:
        raise HTTPException(422, "You cannot delegate to yourself")
    role = await Role.get_async(data.role_id)
    if role.slug == "super-admin":
        raise HTTPException(403, "super-admin cannot be delegated")
    if role.slug not in ctx.roles:
        raise HTTPException(403, "You can only delegate a role you hold")
    scope_team = None
    if data.scope_team_id:
        scope_team = await Team.get_async(data.scope_team_id)
        ensure_same_org(ctx, scope_team.organization.id, "Team")
    delegation = Delegation(
        organization=ctx.organization,
        delegator=ctx.user,
        delegate=delegate,
        role=role,
        start_time=data.start_time,
        end_time=data.end_time,
        scope_team=scope_team,
        scope_environment=data.scope_environment,
        reason=data.reason,
        revoked_by=None,
    )
    await delegation.save_async()
    await audit(
        ctx,
        AuditEventType.DELEGATION_CREATED,
        "delegation",
        delegation.id,
        f"delegated {role.slug} to {delegate.id}",
    )
    return {"status": "success", "data": _serialize_model(delegation)}


@app.get("/api/v1/delegations")
async def list_delegations(
    ctx: RequestContext = Depends(get_current_user),
):
    """Active delegations made and received by the current user."""
    made = await Delegation.filter_async(
        delegator__object_id=ctx.user.id, status=DelegationStatus.ACTIVE
    )
    received = await Delegation.filter_async(
        delegate__object_id=ctx.user.id, status=DelegationStatus.ACTIVE
    )
    return {
        "status": "success",
        "data": {
            "made": [_serialize_model(d) for d in made],
            "received": [_serialize_model(d) for d in received],
        },
    }


@app.delete("/api/v1/delegations/{delegation_id}")
async def revoke_delegation(
    delegation_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.DELEGATION_REVOKE)),
):
    """Revoke a delegation (the delegator, or an org admin)."""
    delegation = await Delegation.get_async(delegation_id)
    ensure_same_org(ctx, delegation.organization.id, "Delegation")
    if delegation.delegator.id != ctx.user.id and not has_permission(
        ctx.roles, Permission.ORG_MANAGE, ctx.permissions
    ):
        raise HTTPException(403, "Only the delegator or an org admin can revoke")
    if delegation.status == DelegationStatus.ACTIVE:
        delegation.status = DelegationStatus.REVOKED
        delegation.revoked_by = ctx.user
        delegation.revoked_at = datetime.now(timezone.utc).timestamp()
        await delegation.save_async()
        await audit(
            ctx,
            AuditEventType.DELEGATION_REVOKED,
            "delegation",
            delegation.id,
            "revoked",
        )
    return {"status": "success", "data": None}
