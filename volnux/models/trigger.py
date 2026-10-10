from typing import Any, Dict
from dataclasses import field

from formax import InitStrategy

from volnux.backends.formax_fk import (
    ForeignKeyField,
    FKConfig,
    ForeignKey,
    OnDelete,
    FKConstraint,
)
from volnux.models.enums import TriggerType

from .base import GovernanceModel
from .users import User, Organization
from .workflows import Workflow


class TriggerConfig(GovernanceModel):
    """Trigger configuration for workflow activation."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="trigger_configs", on_delete=OnDelete.CASCADE),
    ]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="trigger_configs", on_delete=OnDelete.CASCADE),
    ]
    trigger_type: TriggerType
    created_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="created_triggers", on_delete=OnDelete.PROTECT),
    ]
    config: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
