from typing import Any, Dict, Optional

from formax import BaseModel, MiniAnnotated, Attrib, InitStrategy

from volnux.backends.formax_fk import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
)
from volnux.models.enums import EnvironmentType


from .base import GovernanceModel
from .users import User, Role, Organization, Team
from .enums import DelegationStatus
from .utils import pre_format_timestamps, post_format_timestamps


class Delegation(GovernanceModel):
    """Time-bounded role delegation.

    Reverse Relations:
        actions — Actions taken under this delegation
    """

    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="delegations", on_delete=OnDelete.CASCADE),
    ]
    delegator: ForeignKeyField[
        User,
        FKConfig(reverse_name="delegations_made", on_delete=OnDelete.CASCADE),
    ]
    delegate: ForeignKeyField[
        User,
        FKConfig(reverse_name="delegations_received", on_delete=OnDelete.CASCADE),
    ]
    role: ForeignKeyField[
        Role,
        FKConfig(reverse_name="delegations", on_delete=OnDelete.CASCADE),
    ]
    start_time: float
    end_time: float
    status: MiniAnnotated[DelegationStatus, Attrib(default=DelegationStatus.ACTIVE)]
    scope_team: ForeignKeyField[
        Team,
        FKConfig(
            nullable=True, reverse_name="delegations", on_delete=OnDelete.SET_NULL
        ),
    ]
    scope_environment: MiniAnnotated[Optional[EnvironmentType], Attrib(default=None)]
    reason: MiniAnnotated[Optional[str], Attrib(default=None)]
    revoked_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            reverse_name="revoked_delegations",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    revoked_at: MiniAnnotated[
        Optional[float],
        Attrib(
            default=None,
            pre_formatter=pre_format_timestamps,
            post_formatter=post_format_timestamps,
        ),
    ]

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True


class DelegationAction(GovernanceModel):
    """Record of an action taken under a delegation."""

    delegation: ForeignKeyField[
        Delegation,
        FKConfig(reverse_name="actions", on_delete=OnDelete.CASCADE),
    ]
    action_type: str
    target_type: str
    target_id: str
    metadata: MiniAnnotated[Dict[str, Any], Attrib(default=dict)]

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
