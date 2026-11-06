from datetime import datetime, timezone
from typing import Any, Dict, Optional

from formax import MiniAnnotated, Attrib

from volnux.backends.fields import ForeignKeyField, FKConfig, OnDelete

from .base import GovernanceModel
from .users import Organization, Team, User
from .enums import NamespaceType, EventStatus
from .utils import SEMVER


class Event(GovernanceModel):
    """EventBase component in the EventHub registry.

    Reverse Relations:
        versions     — Version history
        dependencies — Events that depend on this event
        dependents — Events this event depends on
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    namespace: MiniAnnotated[NamespaceType, Attrib(default=NamespaceType.LOCAL)]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="events", on_delete=OnDelete.CASCADE),
    ]
    team: ForeignKeyField[
        Team,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="events",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    version: MiniAnnotated[str, Attrib(default="0.1.0", pattern=SEMVER)]
    status: MiniAnnotated[EventStatus, Attrib(default=EventStatus.PUBLISHED)]
    manifest: Dict[str, Any]
    publisher: ForeignKeyField[
        User,
        FKConfig(reverse_name="published_events", on_delete=OnDelete.PROTECT),
    ]

    unique_together = (("organization", "name", "version"),)


class EventVersion(GovernanceModel):
    """Immutable record of an event version."""

    event: ForeignKeyField[
        Event,
        FKConfig(reverse_name="versions", on_delete=OnDelete.CASCADE),
    ]
    version_number: MiniAnnotated[str, Attrib(pattern=SEMVER)]
    manifest: Dict[str, Any]
    changelog: MiniAnnotated[Optional[str], Attrib(default=None)]
    compatibility_matrix: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    published_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="published_event_versions", on_delete=OnDelete.PROTECT),
    ]
    published_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]


class EventDependency(GovernanceModel):
    """Declared dependencies between events."""

    event: ForeignKeyField[
        Event,
        FKConfig(reverse_name="dependencies", on_delete=OnDelete.CASCADE),
    ]
    depends_on_event: ForeignKeyField[
        Event,
        FKConfig(reverse_name="dependents", on_delete=OnDelete.CASCADE),
    ]
    version_constraint: MiniAnnotated[str, Attrib(default="*")]


class Namespace(GovernanceModel):
    """EventHub namespace configuration."""

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    namespace_type: NamespaceType
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="namespaces", on_delete=OnDelete.CASCADE),
    ]
    team: ForeignKeyField[
        Team,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="namespaces",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    config: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    is_private: bool = False

    unique_together = (("organization", "name"),)
