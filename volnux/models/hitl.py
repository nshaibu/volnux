from typing import Any, Dict, List, Optional

from formax import MiniAnnotated, Attrib, InitStrategy

from volnux.backends.fields import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
)

from .base import GovernanceModel
from .workflows import Workflow
from .users import User, Team
from .execution import Execution, ExecutionTrace
from .utils import pre_format_timestamps, post_format_timestamps
from .enums import HITLStatus, HITLPriority


class HITLRequest(GovernanceModel):
    """Human-in-the-Loop request during workflow execution."""

    execution: ForeignKeyField[
        Execution,
        FKConfig(reverse_name="hitl_requests", on_delete=OnDelete.CASCADE),
    ]
    trace: ForeignKeyField[
        ExecutionTrace,
        FKConfig(reverse_name="hitl_requests", on_delete=OnDelete.CASCADE),
    ]
    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="hitl_requests", on_delete=OnDelete.PROTECT),
    ]
    node_id: str
    status: MiniAnnotated[HITLStatus, Attrib(default=HITLStatus.PENDING)]
    priority: MiniAnnotated[HITLPriority, Attrib(default=HITLPriority.MEDIUM)]
    assigned_role: MiniAnnotated[Optional[str], Attrib(default=None)]
    assigned_team: ForeignKeyField[
        Team,
        FKConfig(
            nullable=True, reverse_name="hitl_requests", on_delete=OnDelete.SET_NULL
        ),
    ]
    assigned_user: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            reverse_name="assigned_hitl_requests",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    eligible_users: MiniAnnotated[List[str], Attrib(default=list)]
    prompt: str
    context_data: MiniAnnotated[Dict[str, Any], Attrib(default=dict)]
    available_actions: MiniAnnotated[List[str], Attrib(default=list)]
    required_inputs: MiniAnnotated[List[Dict[str, Any]], Attrib(default=list)]
    sla_deadline: MiniAnnotated[Optional[float], Attrib(default=None)]
    sla_warning_threshold: MiniAnnotated[float, Attrib(default=0.8)]
    sla_breach_action: MiniAnnotated[str, Attrib(default="escalate_to_super_admin")]
    responded_at: MiniAnnotated[
        Optional[float],
        Attrib(
            default=None,
            pre_formatter=pre_format_timestamps,
            post_formatter=post_format_timestamps,
        ),
    ]
    escalated_at: MiniAnnotated[
        Optional[float],
        Attrib(
            default=None,
            pre_formatter=pre_format_timestamps,
            post_formatter=post_format_timestamps,
        ),
    ]
    escalated_to: MiniAnnotated[Optional[str], Attrib(default=None)]
    escalated_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            reverse_name="escalated_hitl_requests",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    resolved_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            reverse_name="resolved_hitl_requests",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    decision: MiniAnnotated[Optional[str], Attrib(default=None)]
    decision_metadata: MiniAnnotated[Dict[str, Any], Attrib(default=dict)]
    escalation_count: MiniAnnotated[int, Attrib(default=0)]
    notification_channels: MiniAnnotated[List[str], Attrib(default=list)]

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
