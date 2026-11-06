from fastapi import Depends, Path, HTTPException

from volnux.models import Team, TeamMember, User
from volnux.exceptions import ObjectDoesNotExist
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import TeamCreate, TeamMemberAdd, TeamUpdate
from ..utils import (
    RequestContext,
    get_current_user,
    require_any_permission,
    require_permission,
)

app = get_current_app()


async def _load_team(ctx: RequestContext, team_id: str) -> Team:
    team = await Team.get_async(team_id)
    ensure_same_org(ctx, team.organization.id, "Team")
    return team


@app.post("/api/v1/teams", status_code=201)
async def create_team(
    data: TeamCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_CREATE)),
):
    if await Team.filter_async(
        slug=data.slug, organization__object_id=ctx.organization.id
    ):
        raise HTTPException(409, "A team with this slug already exists")
    team = Team(**data.model_dump(), organization=ctx.organization)
    await team.save_async()
    return {"status": "success", "data": _serialize_model(team)}


@app.get("/api/v1/teams")
async def list_teams(
    pagination: dict = Depends(Pagination),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(
        require_any_permission(Permission.TEAM_MANAGE, Permission.USER_READ)
    ),
):
    scope = {"organization__object_id": ctx.organization.id}
    teams = await Team.filter_async(**page_args(pagination), **scope)
    return {
        "status": "success",
        "data": [_serialize_model(t) for t in teams],
        "meta": meta(pagination, await Team.count_async(**scope)),
    }


@app.get("/api/v1/teams/{team_id}")
async def get_team(
    team_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(
        require_any_permission(Permission.TEAM_MANAGE, Permission.USER_READ)
    ),
):
    return {
        "status": "success",
        "data": _serialize_model(await _load_team(ctx, team_id)),
    }


@app.put("/api/v1/teams/{team_id}")
async def update_team(
    data: TeamUpdate,
    team_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    team = await _load_team(ctx, team_id)
    for name, value in data.model_dump(exclude_unset=True).items():
        setattr(team, name, value)
    await team.save_async()
    return {"status": "success", "data": _serialize_model(team)}


@app.delete("/api/v1/teams/{team_id}")
async def delete_team(
    team_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    team = await _load_team(ctx, team_id)
    await team.delete_async()
    return {"status": "success", "data": None}


@app.get("/api/v1/teams/{team_id}/members")
async def list_team_members(
    team_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(
        require_any_permission(Permission.TEAM_MANAGE, Permission.USER_READ)
    ),
):
    await _load_team(ctx, team_id)
    members = []
    for m in await TeamMember.filter_async(team__object_id=team_id):
        try:
            members.append(_serialize_model(await User.get_async(m.user.id)))
        except ObjectDoesNotExist:
            pass
    return {"status": "success", "data": members}


@app.post("/api/v1/teams/{team_id}/members", status_code=201)
async def add_team_member(
    data: TeamMemberAdd,
    team_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    team = await _load_team(ctx, team_id)
    member = await User.get_async(data.user_id)
    ensure_same_org(ctx, member.organization.id, "User")
    if await TeamMember.filter_async(
        team__object_id=team_id, user__object_id=member.id
    ):
        raise HTTPException(409, "The user is already a member of this team")
    membership = TeamMember(team=team, user=member)
    await membership.save_async()
    return {"status": "success", "data": _serialize_model(membership)}


@app.delete("/api/v1/teams/{team_id}/members/{user_id}")
async def remove_team_member(
    team_id: str = Path(...),
    user_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    await _load_team(ctx, team_id)
    for membership in await TeamMember.filter_async(
        team__object_id=team_id, user__object_id=user_id
    ):
        await membership.delete_async()
    return {"status": "success", "data": None}
