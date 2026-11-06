from fastapi import Depends, Path, HTTPException

from volnux.models import RoleAssignment, User
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import UserCreate, UserUpdate
from ..security import hash_password
from ..utils import RequestContext, get_current_user, require_permission

app = get_current_app()


async def _load_user(ctx: RequestContext, user_id: str) -> User:
    user = await User.get_async(user_id)
    ensure_same_org(ctx, user.organization.id, "User")
    return user


@app.post("/api/v1/users", status_code=201)
async def create_user(
    data: UserCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_CREATE)),
):
    """Create a user in the caller's organization."""
    if await User.filter_async(
        email=data.email, organization__object_id=ctx.organization.id
    ):
        raise HTTPException(409, "A user with this email already exists")
    user = User(
        name=data.name,
        email=data.email,
        sso_identifier=data.sso_identifier,
        password_hash=hash_password(data.password),
        organization=ctx.organization,
    )
    await user.save_async()
    return {"status": "success", "data": _serialize_model(user)}


@app.get("/api/v1/users")
async def list_users(
    pagination: dict = Depends(Pagination),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_READ)),
):
    """List users in the caller's organization."""
    scope = {"organization__object_id": ctx.organization.id}
    users = await User.filter_async(**page_args(pagination), **scope)
    total = await User.count_async(**scope)
    return {
        "status": "success",
        "data": [_serialize_model(u) for u in users],
        "meta": meta(pagination, total),
    }


@app.get("/api/v1/users/{user_id}")
async def get_user(
    user_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_READ)),
):
    return {
        "status": "success",
        "data": _serialize_model(await _load_user(ctx, user_id)),
    }


@app.put("/api/v1/users/{user_id}")
async def update_user(
    data: UserUpdate,
    user_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_UPDATE)),
):
    target = await _load_user(ctx, user_id)
    changes = data.model_dump(exclude_unset=True)
    if "password" in changes:
        target.password_hash = hash_password(changes.pop("password"))
    for name, value in changes.items():
        setattr(target, name, value)
    await target.save_async()
    return {"status": "success", "data": _serialize_model(target)}


@app.delete("/api/v1/users/{user_id}")
async def delete_user(
    user_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_DELETE)),
):
    target = await _load_user(ctx, user_id)
    if target.id == ctx.user.id:
        raise HTTPException(400, "You cannot delete your own account")
    await target.delete_async()
    return {"status": "success", "data": None}


@app.get("/api/v1/users/{user_id}/roles")
async def get_user_roles(
    user_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_READ)),
):
    target = await _load_user(ctx, user_id)
    assignments = await RoleAssignment.filter_async(user__object_id=target.id)
    return {
        "status": "success",
        "data": [_serialize_model(a) for a in assignments if a.revoked_at is None],
    }
