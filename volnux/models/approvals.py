from typing import Any, Dict, List, Optional

from formax import MiniAnnotated, Attrib

from volnux.backends.fields import ForeignKeyField, FKConfig, OnDelete
from volnux.models.enums import (
    WorkflowCategory,
    ApproverType,
    ApprovalStatus,
    FailureBehavior,
)

from .base import GovernanceModel
from .users import Organization, User, Role, Team
from .workflows import Workflow


class ApprovalChain(GovernanceModel):
    """Configurable approval chain definition.

    Reverse Relations:
        workflows — Workflows using this chain
        steps — Steps in this chain
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="approval_chains", on_delete=OnDelete.CASCADE),
    ]
    applicable_categories: MiniAnnotated[
        List[WorkflowCategory], Attrib(default_factory=list)
    ]
    is_active: MiniAnnotated[bool, Attrib(default=True)]
    failure_behavior: MiniAnnotated[
        FailureBehavior, Attrib(default=FailureBehavior.RESET_CHAIN)
    ]
    require_all_steps_in_order: MiniAnnotated[bool, Attrib(default=True)]
    # Step definitions copied into ApprovalStep rows when a workflow is submitted.
    steps_template: MiniAnnotated[List[Dict[str, Any]], Attrib(default_factory=list)]
    created_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="created_approval_chains", on_delete=OnDelete.PROTECT),
    ]


class ApprovalStep(GovernanceModel):
    """A single step within an approval chain."""

    chain: ForeignKeyField[
        ApprovalChain,
        FKConfig(reverse_name="steps", on_delete=OnDelete.CASCADE),
    ]
    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="approval_steps", on_delete=OnDelete.CASCADE),
    ]
    step_order: MiniAnnotated[int, Attrib(ge=1)]
    approver_type: ApproverType
    role: ForeignKeyField[
        Role,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="approval_steps",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    individual: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="approval_steps_assigned",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    team: ForeignKeyField[
        Team,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="approval_steps",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    required_count: MiniAnnotated[int, Attrib(default=1, ge=1)]
    is_mandatory: MiniAnnotated[bool, Attrib(default=True)]
    timeout_hours: MiniAnnotated[Optional[int], Attrib(default=None, ge=1)]
    description: MiniAnnotated[Optional[str], Attrib(default=None)]
    status: MiniAnnotated[ApprovalStatus, Attrib(default=ApprovalStatus.PENDING)]
    decided_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="decided_approval_steps",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    comments: Optional[str] = None
    decided_at: Optional[float] = None
    requires_human_readable_review: bool = False
