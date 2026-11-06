from fastapi import Depends, Path

from volnux.models import Organization
from ..app import get_current_app
from ..dependencies import _serialize_model, not_found
from ..permission import Permission
from ..schemas import OrganizationCreate, OrganizationUpdate
from ..utils import RequestContext, get_current_user, is_super_admin, require_permission

app = get_current_app()


def _visible(ctx: RequestContext, org_id: str) -> None:
    if org_id != ctx.organization.id and not is_super_admin(ctx):
        raise not_found("Organization")


@app.post("/api/v1/organizations", status_code=201)
async def create_organization(
    data: OrganizationCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.SUPER_ADMIN)),
):
    """Create a new organization. Requires super-admin."""
    org = Organization(**data.model_dump())
    await org.save_async()
    return {"status": "success", "data": _serialize_model(org, redact=("sso_config",))}


@app.get("/api/v1/organizations/{org_id}")
async def get_organization(
    org_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
):
    """Get an organization. The caller must belong to it (or be a super-admin)."""
    _visible(ctx, org_id)
    org = await Organization.get_async(org_id)
    return {"status": "success", "data": _serialize_model(org, redact=("sso_config",))}


@app.put("/api/v1/organizations/{org_id}")
async def update_organization(
    data: OrganizationUpdate,
    org_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.ORG_MANAGE)),
):
    """Update the caller's organization."""
    _visible(ctx, org_id)
    org = await Organization.get_async(org_id)
    for name, value in data.model_dump(exclude_unset=True).items():
        setattr(org, name, value)
    await org.save_async()
    return {"status": "success", "data": _serialize_model(org, redact=("sso_config",))}


@app.delete("/api/v1/organizations/{org_id}")
async def delete_organization(
    org_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.SUPER_ADMIN)),
):
    """Delete an organization. Requires super-admin."""
    org = await Organization.get_async(org_id)
    await org.delete_async()
    return {"status": "success", "data": None}
