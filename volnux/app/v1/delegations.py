from datetime import datetime, timezone
from fastapi import Depends, Body, Path, HTTPException

from volnux.models import Delegation
from volnux.models.enums import DelegationStatus
from ..app import get_current_app
from ..dependencies import get_org_id, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user

app = get_current_app()


@app.post("/api/v1/delegations", status_code=201)
async def create_delegation(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.DELEGATION_CREATE)),
):
    """Create a time-bounded delegation. Requires operator role."""
    data["organization_id"] = org_id
    data["delegator_id"] = user["user_id"]
    delegation = Delegation(**data)
    await delegation.save_async()
    return {"status": "success", "data": _serialize_model(delegation)}


@app.get("/api/v1/delegations")
async def list_delegations(
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
):
    """List active delegations for the current user."""
    made = await Delegation.filter_async(
        delegator_id=user["user_id"], status=DelegationStatus.ACTIVE
    )
    received = await Delegation.filter_async(
        delegate_id=user["user_id"], status=DelegationStatus.ACTIVE
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
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.DELEGATION_REVOKE)),
):
    """Revoke a delegation."""
    delegation = await Delegation.get_async(delegation_id)
    if delegation.delegator.id != user["user_id"] and "org-admin" not in user["roles"]:
        raise HTTPException(403, "Only the delegator or an org admin can revoke")

    delegation.status = DelegationStatus.REVOKED
    delegation.revoked_by = user["user_id"]
    delegation.revoked_at = datetime.now(timezone.utc).timestamp()
    await delegation.save_async()
    return {"status": "success", "data": None}
