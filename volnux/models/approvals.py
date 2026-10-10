from typing import List, Optional

from formax import BaseModel, MiniAnnotated, Attrib, InitStrategy

from volnux.backends.fields import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
    FKConstraint,
)
from volnux.models.enums import (
    WorkflowCategory,
    ApproverType,
    ApprovalStatus,
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
    failure_behavior: MiniAnnotated[str, Attrib(default="reset_chain")]
    require_all_steps_in_order: MiniAnnotated[bool, Attrib(default=True)]
    created_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="created_approval_chains", on_delete=OnDelete.PROTECT),
    ]

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True


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
            nullable=True, reverse_name="approval_steps", on_delete=OnDelete.SET_NULL
        ),
    ]
    individual: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            reverse_name="approval_steps_assigned",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    team: ForeignKeyField[
        Team,
        FKConfig(
            nullable=True, reverse_name="approval_steps", on_delete=OnDelete.SET_NULL
        ),
    ]
    required_count: MiniAnnotated[int, Attrib(default=1)]
    is_mandatory: MiniAnnotated[bool, Attrib(default=True)]
    timeout_hours: MiniAnnotated[Optional[int], Attrib(default=None)]
    description: MiniAnnotated[Optional[str], Attrib(default=None)]
    status: MiniAnnotated[ApprovalStatus, Attrib(default=ApprovalStatus.PENDING)]
    decided_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            reverse_name="decided_approval_steps",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    decision: Optional[str] = None
    comments: Optional[str] = None
    decided_at: Optional[float] = None
    requires_human_readable_review: bool = False

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
