from typing import Any, Dict, Optional
from dataclasses import field


from volnux.backends.fields.foreign_key import ForeignKeyField, FKConfig, OnDelete
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
    # The id of the live trigger this configuration is registered as in the
    # TriggerEngine (None while it is not registered).
    engine_trigger_id: Optional[str] = None

    # (a webhook trigger's config holds its secret)
    encrypted_fields = ("config",)
