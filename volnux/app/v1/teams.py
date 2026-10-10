from typing import Optional
from fastapi import Depends, Body, Path

from volnux.models import Team, TeamMember, User
from volnux.models.enums import AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, Pagination, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user
from volnux.exceptions import ObjectDoesNotExist

app = get_current_app()


@app.post("/api/v1/teams", status_code=201)
async def create_team(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_CREATE)),
):
    """Create a new team. Requires team-create permission."""
    data["organization_id"] = org_id
    team = Team(**data)
    await team.save_async()
    return {"status": "success", "data": _serialize_model(team)}


@app.get("/api/v1/teams")
async def list_teams(
    org_id: str = Depends(get_org_id),
    pagination: dict = Depends(Pagination),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.USER_READ)),
):
    """List teams in the organization."""
    teams = await Team.filter_async(
        organization_id=org_id,
        limit=pagination["page_size"],
        offset=(pagination["page"] - 1) * pagination["page_size"],
    )
    total = await Team.count_async(organization_id=org_id)

    return {
        "status": "success",
        "data": [_serialize_model(t) for t in teams],
        "meta": {
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "total": total,
        },
    }


@app.get("/api/v1/teams/{team_id}")
async def get_team(
    team_id: str = Path(...),
    user: dict = Depends(get_current_user),
):
    """Get team by ID."""
    team: Team = await Team.get_async(team_id)
    return {"status": "success", "data": _serialize_model(team)}


@app.put("/api/v1/teams/{team_id}")
async def update_team(
    team_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    """Update a team. Requires team-manage permission."""
    team = await Team.get_async(team_id)
    immutable = ("id", "organization_id", "creation_time")
    for key, value in data.items():
        if hasattr(team, key) and key not in immutable:
            setattr(team, key, value)
    team.touch()
    await team.save_async()
    return {"status": "success", "data": _serialize_model(team)}


@app.delete("/api/v1/teams/{team_id}")
async def delete_team(
    team_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    """Delete a team. Requires team-manage permission."""
    team: Team = await Team.get_async(team_id)
    await team.delete_async()
    return {"status": "success", "data": None}


@app.get("/api/v1/teams/{team_id}/members")
async def list_team_members(
    team_id: str = Path(...),
    user: dict = Depends(get_current_user),
):
    """List members of a team."""
    memberships: list[TeamMember] = await TeamMember.filter_async(team_id=team_id)
    member_data = []
    for m in memberships:
        try:
            member: User = await User.get_async(m.user.id)
            member_data.append(_serialize_model(member))
        except ObjectDoesNotExist:
            pass
    return {"status": "success", "data": member_data}


@app.post("/api/v1/teams/{team_id}/members", status_code=201)
async def add_team_member(
    team_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    """Add a member to a team. Requires team-manage permission."""
    membership = TeamMember(team_id=team_id, user_id=data["user_id"])
    await membership.save_async()
    return {"status": "success", "data": _serialize_model(membership)}


@app.delete("/api/v1/teams/{team_id}/members/{user_id}")
async def remove_team_member(
    team_id: str = Path(...),
    user_id: str = Path(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.TEAM_MANAGE)),
):
    """Remove a member from a team. Requires team-manage permission."""
    memberships: list[TeamMember] = await TeamMember.filter_async(
        team_id=team_id, user_id=user_id
    )
    for membership in memberships:
        await membership.delete_async()
    return {"status": "success", "data": None}
