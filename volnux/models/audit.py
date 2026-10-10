from typing import Any, Dict, Optional
from formax import MiniAnnotated, Attrib, InitStrategy

from volnux.backends.formax_fk import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
)

from .enums import AuditEventType
from .base import GovernanceModel
from .users import Organization, User


class AuditEntry(GovernanceModel):
    """Immutable audit log entry with a cryptographic chain.

    Reverse Relations:
        (none — leaf entity, never deleted)
    """

    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="audit_entries", on_delete=OnDelete.PROTECT),
    ]
    event_type: AuditEventType
    actor: ForeignKeyField[
        User,
        FKConfig(reverse_name="audit_entries", on_delete=OnDelete.PROTECT),
    ]
    actor_role: str
    target_type: str
    target_id: str
    action: str
    metadata: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    hash: str = ""
    previous_hash: Optional[str] = None

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
