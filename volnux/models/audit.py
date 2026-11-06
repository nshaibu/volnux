import hashlib
import json
from typing import Any, Dict, Optional
from formax import MiniAnnotated, Attrib

from volnux.backends.fields.foreign_key import (
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

    # --- the chain --------------------------------------------------------------

    def _digest_payload(self) -> dict:
        return {
            "id": self.id,
            "organization": self.organization.id,
            "event_type": getattr(self.event_type, "value", self.event_type),
            "actor": self.actor.id,
            "actor_role": self.actor_role,
            "target_type": self.target_type,
            "target_id": self.target_id,
            "action": self.action,
            "metadata": self.metadata,
        }

    def compute_hash(self) -> str:
        body = json.dumps(
            self._digest_payload(), sort_keys=True, default=str, separators=(",", ":")
        )
        return hashlib.sha256(f"{self.previous_hash or ''}|{body}".encode()).hexdigest()

    @classmethod
    async def append(cls, **fields: Any) -> "AuditEntry":
        """Add an entry to the organization's chain: it records the hash of the one
        before it, so altering or removing an entry breaks every hash after it."""
        organization = fields["organization"]
        previous = await cls.filter_async(
            organization__object_id=organization.id, order_by="-created_at", limit=1
        )
        entry = cls(
            previous_hash=previous[0].hash if previous else None,
            metadata=fields.pop("metadata", None) or {},
            **fields,
        )
        entry.hash = entry.compute_hash()
        await entry.save()
        return entry

    @classmethod
    async def verify_chain(cls, organization_id: str) -> bool:
        """True when every entry of the organization links to the one before it and
        matches its own hash."""
        previous_hash = None
        for entry in await cls.filter_async(
            organization__object_id=organization_id, order_by="created_at"
        ):
            if (
                entry.previous_hash != previous_hash
                or entry.hash != entry.compute_hash()
            ):
                return False
            previous_hash = entry.hash
        return True

    # --- immutability -------------------------------------------------------------

    def validate(self) -> None:
        if not self.hash:
            raise ValueError("Audit entries are created with AuditEntry.append()")

    async def save(self, *args: Any, **kwargs: Any) -> None:
        if self._is_loaded_from_backend():
            raise PermissionError("Audit entries are immutable")
        await super().save(*args, **kwargs)

    async def update(self) -> None:
        raise PermissionError("Audit entries are immutable")

    async def delete(self) -> None:
        raise PermissionError("Audit entries cannot be deleted")
