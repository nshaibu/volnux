from typing import Any, Dict, List
from dataclasses import field

from formax import InitStrategy

from volnux.backends.formax_fk import (
    ForeignKeyField,
    FKConfig,
    ForeignKey,
    OnDelete,
    FKConstraint,
)

from .base import GovernanceModel
from .enums import NotificationChannel
from .users import User, Organization


class NotificationConfig(GovernanceModel):
    """User notification preferences."""

    user: ForeignKeyField[
        User,
        FKConfig(reverse_name="notification_configs", on_delete=OnDelete.CASCADE),
    ]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="notification_configs", on_delete=OnDelete.CASCADE),
    ]
    channel: NotificationChannel
    config: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    event_types: List[str] = field(default_factory=list)

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
