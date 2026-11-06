from datetime import datetime, timezone
from typing import List, Optional

from fastapi import Depends, HTTPException, Path

from volnux.models import (
    ApprovalChain,
    ApprovalStep,
    Role,
    Team,
    TeamMember,
    User,
    Workflow,
)
from volnux.models.enums import (
    ApprovalStatus,
    ApproverType,
    AuditEventType,
    WorkflowStatus,
)
from ..app import get_current_app
from ..dependencies import (
    Pagination,
    _serialize_model,
    ensure_same_org,
    meta,
    page_args,
)
from ..permission import Permission
from ..schemas import ApprovalChainCreate, ApprovalDecision, RejectDecision
from ..utils import (
    RequestContext,
    audit,
    get_current_user,
    is_super_admin,
    require_permission,
)
from .workflows import load_workflow

app = get_current_app()

_REVIEW_STATES = (
    WorkflowStatus.PENDING_REVIEW,
    WorkflowStatus.PENDING_COMPLIANCE,
    WorkflowStatus.PENDING_BUSINESS,
)


async def _can_decide(ctx: RequestContext, step: ApprovalStep) -> bool:
    """Whether the caller is one of the people this step is waiting for."""
    if is_super_admin(ctx):
        return True
    if step.approver_type == ApproverType.NAMED_INDIVIDUAL:
        return step.individual is not None and step.individual.id == ctx.user.id
    if step.approver_type in (ApproverType.ROLE_BASED, ApproverType.COMPLIANCE):
        if step.role is None:
            return False
        role = await Role.get_async(step.role.id)
        return role.slug in ctx.roles
    if step.approver_type == ApproverType.TEAM_QUORUM:
        if step.team is None:
            return False
        return bool(
            await TeamMember.filter_async(
                team__object_id=step.team.id, user__object_id=ctx.user.id
            )
        )
    return False


def _expired(step: ApprovalStep) -> bool:
    if not step.timeout_hours:
        return False
    started = step.created_at.timestamp()
    return datetime.now(timezone.utc).timestamp() > started + step.timeout_hours * 3600


def _open_steps(steps: List[ApprovalStep]) -> List[ApprovalStep]:
    return [s for s in steps if s.status == ApprovalStatus.PENDING]


def _current_order(steps: List[ApprovalStep], ordered: bool) -> Optional[int]:
    """The step order currently waiting for decisions (lowest with a mandatory pending row)."""
    pending = [s.step_order for s in _open_steps(steps) if s.is_mandatory]
    if not pending:
        return None
    return min(pending)


async def _chain_is_ordered(steps: List[ApprovalStep]) -> bool:
    if not steps:
        return True
    return (await ApprovalChain.get_async(steps[0].chain.id)).require_all_steps_in_order


async def _decidable_step(
    ctx: RequestContext, workflow: Workflow
) -> tuple[ApprovalStep, List[ApprovalStep]]:
    if workflow.status not in _REVIEW_STATES:
        raise HTTPException(409, "The workflow is not waiting for approval")
    if workflow.created_by.id == ctx.user.id:
        raise HTTPException(403, "You cannot decide on your own workflow")
    steps = await ApprovalStep.filter_async(workflow__object_id=workflow.id)
    if not steps:
        raise HTTPException(409, "The workflow has no approval steps")
    ordered = await _chain_is_ordered(steps)
    current = _current_order(steps, ordered)
    if current is None:
        raise HTTPException(409, "There is nothing left to approve")
    already = {
        s.decided_by.id
        for s in steps
        if s.decided_by is not None and s.step_order == current
    }
    if ctx.user.id in already:
        raise HTTPException(409, "You have already decided on this step")
    for step in sorted(_open_steps(steps), key=lambda s: s.step_order):
        if ordered and step.step_order != current:
            continue
        if await _can_decide(ctx, step):
            if _expired(step):
                raise HTTPException(409, "This approval step has timed out")
            return step, steps
    raise HTTPException(403, "You are not an approver for the current step")


@app.get("/api/v1/approvals/pending", tags=["Approvals"])
async def list_pending_approvals(
    pagination: dict = Depends(Pagination),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_REVIEW)),
):
    """Steps waiting for a decision from the caller, in the caller's organization."""
    workflows = await Workflow.filter_async(organization__object_id=ctx.organization.id)
    mine = []
    for workflow in workflows:
        if workflow.status not in _REVIEW_STATES:
            continue
        if workflow.created_by.id == ctx.user.id:
            continue
        steps = await ApprovalStep.filter_async(workflow__object_id=workflow.id)
        if not steps:
            continue
        ordered = await _chain_is_ordered(steps)
        current = _current_order(steps, ordered)
        if current is None:
            continue
        done_by = {
            s.decided_by.id
            for s in steps
            if s.decided_by is not None and s.step_order == current
        }
        if ctx.user.id in done_by:
            continue
        for step in _open_steps(steps):
            if ordered and step.step_order != current:
                continue
            if await _can_decide(ctx, step):
                mine.append(step)
                break
    start = (pagination["page"] - 1) * pagination["page_size"]
    window = mine[start : start + pagination["page_size"]]
    return {
        "status": "success",
        "data": [_serialize_model(s) for s in window],
        "meta": meta(pagination, len(mine)),
    }


@app.post("/api/v1/approvals/{workflow_id}/approve", tags=["Approvals"])
async def approve_workflow(
    data: ApprovalDecision,
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_APPROVE)),
):
    workflow = await load_workflow(ctx, workflow_id)
    step, steps = await _decidable_step(ctx, workflow)
    step.status = ApprovalStatus.APPROVED
    step.comments = data.comments
    step.decided_by = ctx.user
    step.decided_at = datetime.now(timezone.utc).timestamp()
    await step.save_async()

    remaining = [
        s
        for s in steps
        if s.id != step.id and s.is_mandatory and s.status == ApprovalStatus.PENDING
    ]
    if not remaining:
        workflow.status = WorkflowStatus.APPROVED
        workflow.updated_by = ctx.user
        await workflow.save_async()
    await audit(
        ctx,
        AuditEventType.WORKFLOW_APPROVED,
        "workflow",
        workflow.id,
        "approved a step" if remaining else "approved the last step",
        {"step": step.id, "step_order": step.step_order},
    )
    return {"status": "success", "data": _serialize_model(workflow)}


@app.post("/api/v1/approvals/{workflow_id}/reject", tags=["Approvals"])
async def reject_workflow(
    data: RejectDecision,
    workflow_id: str = Path(...),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_REJECT)),
):
    workflow = await load_workflow(ctx, workflow_id)
    step, steps = await _decidable_step(ctx, workflow)
    outcome = (
        ApprovalStatus.CHANGES_REQUESTED
        if data.request_changes
        else ApprovalStatus.REJECTED
    )
    step.status = outcome
    step.comments = data.reason
    step.decided_by = ctx.user
    step.decided_at = datetime.now(timezone.utc).timestamp()
    await step.save_async()

    workflow.status = (
        WorkflowStatus.CHANGES_REQUESTED
        if data.request_changes
        else WorkflowStatus.REJECTED
    )
    workflow.updated_by = ctx.user
    await workflow.save_async()

    chain = await ApprovalChain.get_async(step.chain.id)
    if chain.failure_behavior == "reset_chain":
        # The next submission starts the chain again.
        for other in steps:
            if other.id != step.id:
                await other.delete_async()
    await audit(
        ctx,
        AuditEventType.WORKFLOW_REJECTED,
        "workflow",
        workflow.id,
        data.reason,
        {"step": step.id, "request_changes": data.request_changes},
    )
    return {"status": "success", "data": _serialize_model(workflow)}


@app.get("/api/v1/approvals/chains", tags=["Approvals"])
async def list_approval_chains(
    pagination: dict = Depends(Pagination),
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_REVIEW)),
):
    scope = {"organization__object_id": ctx.organization.id}
    chains = await ApprovalChain.filter_async(**page_args(pagination), **scope)
    return {
        "status": "success",
        "data": [_serialize_model(c) for c in chains],
        "meta": meta(pagination, await ApprovalChain.count_async(**scope)),
    }


@app.post("/api/v1/approvals/chains", status_code=201, tags=["Approvals"])
async def create_approval_chain(
    data: ApprovalChainCreate,
    ctx: RequestContext = Depends(get_current_user),
    _: None = Depends(require_permission(Permission.APPROVAL_MANAGE)),
):
    template = []
    for spec in data.steps:
        entry = spec.model_dump(mode="json")
        if spec.role_id:
            role = await Role.get_async(spec.role_id)
            entry["role_id"] = role.id
        if spec.individual_id:
            person = await User.get_async(spec.individual_id)
            ensure_same_org(ctx, person.organization.id, "User")
        if spec.team_id:
            team = await Team.get_async(spec.team_id)
            ensure_same_org(ctx, team.organization.id, "Team")
        need = {
            ApproverType.ROLE_BASED: spec.role_id,
            ApproverType.COMPLIANCE: spec.role_id,
            ApproverType.NAMED_INDIVIDUAL: spec.individual_id,
            ApproverType.TEAM_QUORUM: spec.team_id,
        }[spec.approver_type]
        if not need:
            raise HTTPException(
                422, f"A {spec.approver_type.value} step needs its approver"
            )
        template.append(entry)
    chain = ApprovalChain(
        name=data.name,
        organization=ctx.organization,
        applicable_categories=data.applicable_categories,
        failure_behavior=data.failure_behavior,
        require_all_steps_in_order=data.require_all_steps_in_order,
        steps_template=template,
        created_by=ctx.user,
    )
    await chain.save_async()
    return {"status": "success", "data": _serialize_model(chain)}
