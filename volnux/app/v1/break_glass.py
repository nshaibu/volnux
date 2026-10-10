from datetime import datetime, timezone
from fastapi import Depends, Body, Path

from volnux.models import BreakGlassAccess
from volnux.models.enums import BreakGlassReviewStatus, AuditEventType
from ..app import get_current_app
from ..dependencies import get_org_id, _serialize_model
from ..permission import Permission
from ..utils import require_permission, get_current_user, audit_log

app = get_current_app()


@app.post("/api/v1/break-glass", status_code=201)
async def initiate_break_glass(
    data: dict = Body(...),
    org_id: str = Depends(get_org_id),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.BREAK_GLASS_INITIATE)),
):
    """Initiate break-glass emergency access. Requires super-admin."""
    access = BreakGlassAccess(
        super_admin_id=user["user_id"],
        organization_id=org_id,
        workflow_id=data.get("workflow_id"),
        execution_id=data.get("execution_id"),
        justification=data["justification"],
    )
    await access.save_async()

    # Log to audit
    audit_log(
        org_id=org_id,
        event_type=AuditEventType.BREAK_GLASS_INITIATED,
        actor_id=user["user_id"],
        actor_role="super-admin",
        target_type="break_glass",
        target_id=access.id,
        action="initiate",
        metadata={"justification": data["justification"]},
    )

    return {"status": "success", "data": _serialize_model(access)}


@app.post("/api/v1/break-glass/{access_id}/review")
async def review_break_glass(
    access_id: str = Path(...),
    data: dict = Body(...),
    user: dict = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.BREAK_GLASS_REVIEW)),
):
    """Submit a post-incident review for break-glass access. Requires auditor."""
    access = await BreakGlassAccess.get_async(access_id)
    access.review_status = BreakGlassReviewStatus(data["status"])
    access.review_findings = data.get("findings")
    access.review_remediation = data.get("remediation")
    access.reviewed_by = user["user_id"]
    access.reviewed_at = datetime.now(timezone.utc).timestamp()
    await access.save_async()
    return {"status": "success", "data": _serialize_model(access)}
