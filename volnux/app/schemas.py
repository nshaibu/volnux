"""Request bodies of the governance API.

Every body forbids unknown keys, so a client cannot set a field the server owns
(organization, creator, status, timestamps, ...).
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .enums import Permission

from volnux.models.enums import (
    ApproverType,
    BreakGlassReviewStatus,
    EnvironmentType,
    NamespaceType,
    NodeType,
    NotificationChannel,
    ScopeType,
    TriggerType,
    WorkflowCategory,
)


class Body_(BaseModel):
    model_config = ConfigDict(extra="forbid")


SLUG = r"^[a-z0-9-]+$"
EMAIL = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"


class UserCreate(Body_):
    name: str = Field(min_length=1, max_length=255)
    email: str = Field(pattern=EMAIL)
    password: str = Field(min_length=8, max_length=256)
    sso_identifier: Optional[str] = None


class UserUpdate(Body_):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    email: Optional[str] = Field(default=None, pattern=EMAIL)
    password: Optional[str] = Field(default=None, min_length=8, max_length=256)
    is_active: Optional[bool] = None
    sso_identifier: Optional[str] = None


class OrganizationCreate(Body_):
    name: str = Field(min_length=1, max_length=255)
    slug: str = Field(pattern=SLUG)
    policies: Dict[str, Any] = Field(default_factory=dict)
    sso_config: Optional[Dict[str, Any]] = None


class OrganizationUpdate(Body_):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    policies: Optional[Dict[str, Any]] = None
    sso_config: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class TeamCreate(Body_):
    name: str = Field(min_length=1, max_length=255)
    slug: str = Field(pattern=SLUG)
    description: Optional[str] = None
    namespace: str = "local"


class TeamUpdate(Body_):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = None
    namespace: Optional[str] = None


class TeamMemberAdd(Body_):
    user_id: str


class RoleCreate(Body_):
    name: str = Field(min_length=1, max_length=100)
    slug: str = Field(pattern=SLUG)
    description: Optional[str] = None
    permissions: List[Permission] = Field(default_factory=list)

    @field_validator("permissions")
    @classmethod
    def _no_super_admin(cls, value):
        if Permission.SUPER_ADMIN in value:
            raise ValueError("super.admin cannot be granted through a custom role")
        return value


class RoleAssign(Body_):
    user_id: str
    role_id: str
    scope_type: ScopeType = ScopeType.TEAM
    team_id: Optional[str] = None
    environment: Optional[EnvironmentType] = None


class DelegationCreate(Body_):
    delegate_id: str
    role_id: str
    start_time: float
    end_time: float
    scope_team_id: Optional[str] = None
    scope_environment: Optional[EnvironmentType] = None
    reason: Optional[str] = None


class WorkflowCreate(Body_):
    name: str = Field(min_length=1, max_length=255)
    description: Optional[str] = None
    team_id: str
    category: WorkflowCategory = WorkflowCategory.STANDARD
    approval_chain_id: Optional[str] = None
    pty_source: Optional[str] = None


class WorkflowUpdate(Body_):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = None
    category: Optional[WorkflowCategory] = None
    approval_chain_id: Optional[str] = None
    pty_source: Optional[str] = None


class ApprovalDecision(Body_):
    comments: Optional[str] = None


class RejectDecision(Body_):
    reason: str = Field(min_length=1)
    request_changes: bool = False


class ApprovalStepSpec(Body_):
    step_order: int = Field(ge=1)
    approver_type: ApproverType
    role_id: Optional[str] = None
    individual_id: Optional[str] = None
    team_id: Optional[str] = None
    required_count: int = Field(default=1, ge=1)
    is_mandatory: bool = True
    timeout_hours: Optional[int] = Field(default=None, ge=1)
    description: Optional[str] = None


class ApprovalChainCreate(Body_):
    name: str = Field(min_length=1, max_length=255)
    applicable_categories: List[WorkflowCategory] = Field(default_factory=list)
    failure_behavior: Literal["reset_chain", "continue"] = "reset_chain"
    require_all_steps_in_order: bool = True
    steps: List[ApprovalStepSpec] = Field(default_factory=list)


class ExecuteRequest(Body_):
    inputs: Dict[str, Any] = Field(default_factory=dict)
    environment: Optional[EnvironmentType] = None


class HITLRespond(Body_):
    decision: str = Field(min_length=1)
    response_data: Dict[str, Any] = Field(default_factory=dict)
    comments: Optional[str] = None


class BreakGlassInitiate(Body_):
    justification: str = Field(min_length=10)
    workflow_id: Optional[str] = None
    execution_id: Optional[str] = None


class BreakGlassReview(Body_):
    status: Literal[
        BreakGlassReviewStatus.COMPLIANT, BreakGlassReviewStatus.NON_COMPLIANT
    ]
    findings: Optional[str] = None
    remediation: Optional[str] = None


class NotificationConfigCreate(Body_):
    channel: NotificationChannel
    config: Dict[str, Any] = Field(default_factory=dict)
    event_types: List[str] = Field(default_factory=list)
    is_active: bool = True


class NotificationConfigUpdate(Body_):
    config: Optional[Dict[str, Any]] = None
    event_types: Optional[List[str]] = None
    is_active: Optional[bool] = None


class TriggerCreate(Body_):
    workflow_id: str
    trigger_type: TriggerType
    config: Dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True


class TriggerUpdate(Body_):
    is_active: bool


class EventPublish(Body_):
    name: str = Field(min_length=1, max_length=255)
    version: str = Field(default="0.1.0", pattern=r"^\d+\.\d+\.\d+$")
    namespace: NamespaceType = NamespaceType.LOCAL
    team_id: Optional[str] = None
    manifest: Dict[str, Any] = Field(default_factory=dict)
    changelog: Optional[str] = None


class NodeRegister(Body_):
    name: str = Field(min_length=1, max_length=255)
    node_type: NodeType = NodeType.GENERAL
    region: Optional[str] = None
    endpoint: Optional[str] = None
    mtls_cert_fingerprint: Optional[str] = None
    capacity_max_concurrent_tasks: int = Field(default=10, ge=1)
    capacity_cpu_cores: Optional[int] = Field(default=None, ge=1)
    capacity_memory_gb: Optional[float] = Field(default=None, gt=0)
    capacity_disk_gb: Optional[float] = Field(default=None, gt=0)
