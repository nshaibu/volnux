from typing import Any, Dict, List, Optional

from formax import MiniAnnotated, Attrib

from volnux.backends.fields import (
    ForeignKeyField,
    FKConfig,
    OnDelete,
    DateTimeField,
    DTConfig,
)
from volnux.models.enums import EnvironmentType, ScopeType

from .base import GovernanceModel


class Organization(GovernanceModel):
    """Top-level organization entity for multi-tenancy.

    Reverse Relations:
        users — All users in this organization
        teams — All teams in this organization
        workflows — All workflows in this organization
        events — All EventHub components in this organization
        namespaces — All EventHub namespaces in this organization
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    slug: MiniAnnotated[str, Attrib(pattern=r"^[a-z0-9-]+$", metadata={"unique": True})]
    policies: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    is_active: bool = True
    sso_config: Optional[Dict[str, Any]] = None


class User(GovernanceModel):
    """User identity within an organization.

    Reverse Relations (auto-registered by ForeignKeyField):
        created_workflows           — Workflows created by this user
        updated_workflows           — Workflows last updated by this user
        approved_workflows          — Workflows approved by this user
        decided_approval_steps      — Approval decisions by this user
        audit_entries               — Audit entries for actions by this user
        triggered_executions        — Executions triggered by this user
        assigned_hitl_requests      — HITL requests assigned to this user
        resolved_hitl_requests      — HITL requests resolved by this user
        delegations_made            — Delegations made by this user
        delegations_received        — Delegations received by this user
        break_glass_accesses        — Break-glass accesses by this user
        notification_configs        — Notification configs for this user
        role_assignments            — Role assignments for this user
    """

    name: MiniAnnotated[
        str, Attrib(min_length=1, max_length=255, metadata={"unique": True})
    ]
    email: MiniAnnotated[
        str,
        Attrib(pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
    ]
    sso_identifier: MiniAnnotated[Optional[str], Attrib(default=None)]
    is_active: MiniAnnotated[bool, Attrib(default=True)]

    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="users", on_delete=OnDelete.CASCADE),
    ]


class Team(GovernanceModel):
    """Team entity for scoped role boundaries.

    Reverse Relations:
        team_members — Members of this team
        workflows — Workflows owned by this team
        role_assignments — Role assignments scoped to this team
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    slug: MiniAnnotated[str, Attrib(pattern=r"^[a-z0-9-]+$")]
    description: MiniAnnotated[Optional[str], Attrib(default=None)]
    namespace: MiniAnnotated[str, Attrib(default="local")]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="teams", on_delete=OnDelete.CASCADE),
    ]


class TeamMember(GovernanceModel):
    """Many-to-many relationship between teams and users."""

    team: ForeignKeyField[
        Team,
        FKConfig(reverse_name="team_members", on_delete=OnDelete.CASCADE),
    ]
    user: ForeignKeyField[
        User,
        FKConfig(reverse_name="team_memberships", on_delete=OnDelete.CASCADE),
    ]


class Role(GovernanceModel):
    """Role definition with associated permissions.

    Reverse Relations:
        role_assignments — Assignments of this role
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=100)]
    slug: MiniAnnotated[str, Attrib(pattern=r"^[a-z0-9-]+$", metadata={"unique": True})]
    description: MiniAnnotated[Optional[str], Attrib(default=None)]
    permissions: MiniAnnotated[List[str], Attrib(default_factory=list)]


class RoleAssignment(GovernanceModel):
    """Assignment of a role to a user with scope boundaries."""

    user: ForeignKeyField[
        User,
        FKConfig(reverse_name="role_assignments", on_delete=OnDelete.CASCADE),
    ]
    role: ForeignKeyField[
        Role,
        FKConfig(reverse_name="role_assignments", on_delete=OnDelete.CASCADE),
    ]
    scope_type: MiniAnnotated[ScopeType, Attrib(default=ScopeType.TEAM)]
    team: ForeignKeyField[
        Team,
        FKConfig(
            nullable=True, reverse_name="role_assignments", on_delete=OnDelete.SET_NULL
        ),
    ]
    environment: MiniAnnotated[Optional[EnvironmentType], Attrib(default=None)]
    granted_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="granted_role_assignments", on_delete=OnDelete.PROTECT),
    ]
    revoked_at: DateTimeField[DTConfig(nullable=True)]
