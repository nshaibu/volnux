import hashlib
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import Depends, HTTPException, Path, Query

from volnux.models import (
    ApprovalChain,
    ApprovalStep,
    Role,
    Team,
    Workflow,
    WorkflowVersion,
)
from volnux.models.workflows import make_delta
from volnux.models.enums import AuditEventType, WorkflowStatus
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import WorkflowCreate, WorkflowUpdate
from ..utils import RequestContext, audit, get_current_user, require_permission

app = get_current_app()

_EDITABLE = (
    WorkflowStatus.DRAFT,
    WorkflowStatus.CHANGES_REQUESTED,
    WorkflowStatus.REJECTED,
)
_SUBMITTABLE = (WorkflowStatus.DRAFT, WorkflowStatus.CHANGES_REQUESTED)


async def load_workflow(ctx: RequestContext, workflow_id: str) -> Workflow:
    """The workflow, or 404 if it is missing or belongs to another organization."""
    workflow = await Workflow.get_async(workflow_id)
    ensure_same_org(ctx, workflow.organization.id, "Workflow")
    return workflow


async def versions_of(workflow_id: str) -> List[WorkflowVersion]:
    versions = await WorkflowVersion.filter_async(
        workflow__object_id=workflow_id, order_by="-created_at"
    )
    return versions


def _next_version(latest: Optional[WorkflowVersion]) -> str:
    if latest is None:
        return "0.1.0"
    try:
        major, minor, patch = (int(p) for p in latest.version.split("."))
    except ValueError:
        return "0.1.0"
    return f"{major}.{minor}.{patch + 1}"


_KEYFRAME_EVERY = 10  # a full copy every N versions keeps rebuild chains short


async def add_version(
    ctx: RequestContext, workflow: Workflow, source: str, changelog: Optional[str]
) -> WorkflowVersion:
    """Store ``source`` as the next version: a diff from the previous one, or the
    whole text for the first version, every 10th, and when a diff is not safe."""
    existing = await versions_of(workflow.id)
    latest = existing[0] if existing else None
    pty_source, delta = source, None
    if latest is not None and len(existing) % _KEYFRAME_EVERY:
        delta = make_delta(await latest.get_full_source_async(), source)
        if delta is not None:
            pty_source = None
    for old in existing:
        if old.is_active:
            old.is_active = False
            await old.save_async()
    version = WorkflowVersion(
        workflow=workflow,
        version=_next_version(latest),
        ast_hash=hashlib.sha256(source.encode()).hexdigest(),
        parent_version=latest,
        pty_source=pty_source,
        delta_patch=delta,
        is_active=True,
        changelog=changelog,
        published_by=ctx.user,
    )
    await version.save_async()
    return version


@app.post("/api/v1/workflows", status_code=201)
async def create_workflow(
    data: WorkflowCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_CREATE)),
):
    team = await Team.get_async(data.team_id)
    ensure_same_org(ctx, team.organization.id, "Team")
    chain = None
    if data.approval_chain_id:
        chain = await ApprovalChain.get_async(data.approval_chain_id)
        ensure_same_org(ctx, chain.organization.id, "Approval chain")
    workflow = Workflow(
        name=data.name,
        description=data.description,
        category=data.category,
        organization=ctx.organization,
        team=team,
        created_by=ctx.user,
        updated_by=None,
        approval_chain=chain,
        published_at=None,
    )
    await workflow.save_async()
    if data.pty_source:
        await add_version(ctx, workflow, data.pty_source, "initial version")
    await audit(
        ctx,
        AuditEventType.WORKFLOW_CREATED,
        "workflow",
        workflow.id,
        f"created workflow {workflow.name}",
    )
    return {"status": "success", "data": _serialize_model(workflow)}


@app.get("/api/v1/workflows")
async def list_workflows(
    pagination: dict = Depends(Pagination),
    status: Optional[WorkflowStatus] = Query(None),
    team_id: Optional[str] = Query(None),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    filters = {"organization__object_id": ctx.organization.id}
    if status:
        filters["status"] = status
    if team_id:
        filters["team__object_id"] = team_id
    workflows = await Workflow.filter_async(
        **page_args(pagination), order_by="-created_at", **filters
    )
    return {
        "status": "success",
        "data": [_serialize_model(w) for w in workflows],
        "meta": meta(pagination, await Workflow.count_async(**filters)),
    }


@app.get("/api/v1/workflows/{workflow_id}")
async def get_workflow(
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    workflow = await load_workflow(ctx, workflow_id)
    return {"status": "success", "data": _serialize_model(workflow)}


@app.get("/api/v1/workflows/{workflow_id}/source")
async def get_workflow_source(
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    """The source of the active version."""
    await load_workflow(ctx, workflow_id)
    versions = await versions_of(workflow_id)
    if not versions:
        raise HTTPException(404, "Workflow has no source yet")
    active = next((v for v in versions if v.is_active), versions[0])
    return {
        "status": "success",
        "data": {
            "version": active.version,
            "pty_source": await active.get_full_source_async(),
        },
    }


@app.put("/api/v1/workflows/{workflow_id}")
async def update_workflow(
    data: WorkflowUpdate,
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_UPDATE)),
):
    """Edit a workflow that is still a draft (status changes go through submit/approve/publish)."""
    workflow = await load_workflow(ctx, workflow_id)
    if workflow.status not in _EDITABLE:
        raise HTTPException(
            409,
            f"A workflow in status {getattr(workflow.status, 'value', workflow.status)} cannot be edited",
        )
    changes = data.model_dump(exclude_unset=True)
    source = changes.pop("pty_source", None)
    chain_id = changes.pop("approval_chain_id", None)
    if chain_id:
        chain = await ApprovalChain.get_async(chain_id)
        ensure_same_org(ctx, chain.organization.id, "Approval chain")
        workflow.approval_chain = chain
    for name, value in changes.items():
        setattr(workflow, name, value)
    workflow.updated_by = ctx.user
    await workflow.save_async()
    if source:
        await add_version(ctx, workflow, source, "updated")
    return {"status": "success", "data": _serialize_model(workflow)}


@app.delete("/api/v1/workflows/{workflow_id}")
async def delete_workflow(
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_DELETE)),
):
    workflow = await load_workflow(ctx, workflow_id)
    if workflow.status != WorkflowStatus.DRAFT:
        raise HTTPException(409, "Only draft workflows can be deleted")
    await workflow.delete_async()
    await audit(
        ctx,
        AuditEventType.WORKFLOW_DEPRECATED,
        "workflow",
        workflow_id,
        f"deleted draft workflow {workflow.name}",
    )
    return {"status": "success", "data": None}


async def _find_chain(
    ctx: RequestContext, workflow: Workflow
) -> Optional[ApprovalChain]:
    if workflow.approval_chain is not None:
        return await ApprovalChain.get_async(workflow.approval_chain.id)
    for chain in await ApprovalChain.filter_async(
        organization__object_id=ctx.organization.id, is_active=True
    ):
        if workflow.category in chain.applicable_categories:
            return chain
    return None


@app.post("/api/v1/workflows/{workflow_id}/submit")
async def submit_workflow(
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_SUBMIT)),
):
    """Submit a draft for review: the chain's steps are created for this workflow."""
    workflow = await load_workflow(ctx, workflow_id)
    if workflow.status not in _SUBMITTABLE:
        raise HTTPException(
            409,
            f"A workflow in status {getattr(workflow.status, 'value', workflow.status)} cannot be submitted",
        )
    if not await versions_of(workflow.id):
        raise HTTPException(422, "The workflow has no source to review")
    chain = await _find_chain(ctx, workflow)
    if chain is None or not chain.is_active or not chain.steps_template:
        raise HTTPException(422, "No active approval chain applies to this workflow")

    for old in await ApprovalStep.filter_async(workflow__object_id=workflow.id):
        await old.delete_async()
    for spec in sorted(chain.steps_template, key=lambda s: s["step_order"]):
        role = await Role.get_async(spec["role_id"]) if spec.get("role_id") else None
        individual = None
        if spec.get("individual_id"):
            from volnux.models import User

            individual = await User.get_async(spec["individual_id"])
        team = await Team.get_async(spec["team_id"]) if spec.get("team_id") else None
        # One row per required approver: a step with a quorum of 3 has 3 rows, each
        # decided by a different person.
        for _n in range(spec.get("required_count", 1)):
            await ApprovalStep(
                chain=chain,
                workflow=workflow,
                step_order=spec["step_order"],
                approver_type=spec["approver_type"],
                role=role,
                individual=individual,
                team=team,
                required_count=spec.get("required_count", 1),
                is_mandatory=spec.get("is_mandatory", True),
                timeout_hours=spec.get("timeout_hours"),
                description=spec.get("description"),
                decided_by=None,
            ).save_async()

    workflow.approval_chain = chain
    workflow.status = WorkflowStatus.PENDING_REVIEW
    workflow.updated_by = ctx.user
    await workflow.save_async()
    await audit(
        ctx,
        AuditEventType.WORKFLOW_SUBMITTED,
        "workflow",
        workflow.id,
        "submitted for review",
        {"chain": chain.id},
    )
    return {"status": "success", "data": _serialize_model(workflow)}


@app.post("/api/v1/workflows/{workflow_id}/publish")
async def publish_workflow(
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_PUBLISH)),
):
    """Promote an approved workflow to production."""
    workflow = await load_workflow(ctx, workflow_id)
    if workflow.status != WorkflowStatus.APPROVED:
        raise HTTPException(409, "Only an approved workflow can be published")
    workflow.status = WorkflowStatus.PRODUCTION
    workflow.published_at = datetime.now(timezone.utc)
    workflow.updated_by = ctx.user
    await workflow.save_async()
    await audit(
        ctx,
        AuditEventType.WORKFLOW_PROMOTED,
        "workflow",
        workflow.id,
        "published to production",
    )
    return {"status": "success", "data": _serialize_model(workflow)}


@app.get("/api/v1/workflows/{workflow_id}/versions")
async def list_workflow_versions(
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    await load_workflow(ctx, workflow_id)
    versions = await versions_of(workflow_id)
    return {"status": "success", "data": [_serialize_model(v) for v in versions]}
