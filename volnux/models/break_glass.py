from datetime import datetime, timezone
from typing import Any, Dict, Optional

from formax import MiniAnnotated, Attrib, InitStrategy

from volnux.backends.fields import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
    FKConstraint,
)

from .base import GovernanceModel
from .users import User, Organization
from .workflows import Workflow
from .execution import Execution
from .enums import BreakGlassReviewStatus
from .utils import pre_format_timestamps, post_format_timestamps


class BreakGlassAccess(GovernanceModel):
    """Emergency break-glass access record.

    Reverse Relations:
        actions — Actions taken during this session
    """

    super_admin: ForeignKeyField[
        User,
        FKConfig(reverse_name="break_glass_accesses", on_delete=OnDelete.PROTECT),
    ]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="break_glass_accesses", on_delete=OnDelete.PROTECT),
    ]
    workflow: ForeignKeyField[
        Workflow,
        FKConfig(
            nullable=True,
            reverse_name="break_glass_accesses",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    execution: ForeignKeyField[
        Execution,
        FKConfig(
            nullable=True,
            reverse_name="break_glass_accesses",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    justification: MiniAnnotated[str, Attrib(min_length=10, max_length=2000)]
    access_time: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    review_status: MiniAnnotated[
        BreakGlassReviewStatus, Attrib(default=BreakGlassReviewStatus.PENDING)
    ]
    review_findings: MiniAnnotated[Optional[str], Attrib(default=None)]
    review_remediation: MiniAnnotated[Optional[str], Attrib(default=None)]
    reviewed_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            reverse_name="reviewed_break_glass",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    reviewed_at: Optional[float] = None

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True


class BreakGlassAction(GovernanceModel):
    """Record of actions taken during a break-glass session."""

    access: ForeignKeyField[
        BreakGlassAccess,
        FKConfig(reverse_name="actions", on_delete=OnDelete.CASCADE),
    ]
    action_type: str
    target_type: str
    target_id: str
    metadata: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    performed_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="break_glass_actions", on_delete=OnDelete.CASCADE),
    ]
    performed_at: MiniAnnotated[
        float,
        Attrib(
            default_factory=lambda: datetime.now(timezone.utc).timestamp(),
            pre_formatter=pre_format_timestamps,
            post_formatter=post_format_timestamps,
        ),
    ]

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
