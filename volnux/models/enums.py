from enum import Enum


class WorkflowStatus(str, Enum):
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    PENDING_COMPLIANCE = "pending_compliance"
    PENDING_BUSINESS = "pending_business"
    APPROVED = "approved"
    REJECTED = "rejected"
    CHANGES_REQUESTED = "changes_requested"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"


class WorkflowCategory(str, Enum):
    STANDARD = "standard"
    SENSITIVE = "sensitive"
    REGULATED = "regulated"
    CRITICAL = "critical"


class ExecutionState(str, Enum):
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"
    PREEMPTED = "preempted"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CHANGES_REQUESTED = "changes_requested"


class ApproverType(str, Enum):
    ROLE_BASED = "role_based"
    COMPLIANCE = "compliance"
    NAMED_INDIVIDUAL = "named_individual"
    TEAM_QUORUM = "team_quorum"


class HITLStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RESPONDED = "responded"
    COMPLETED = "completed"
    EXPIRED = "expired"
    ESCALATED = "escalated"
    REROUTED = "rerouted"
    REJECTED = "rejected"


class HITLPriority(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class DelegationStatus(str, Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"


class BreakGlassReviewStatus(str, Enum):
    PENDING = "pending"
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"


class ScopeType(str, Enum):
    GLOBAL = "global"
    TEAM = "team"
    ENVIRONMENT = "environment"


class EnvironmentType(str, Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class EventStatus(str, Enum):
    PUBLISHED = "published"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class NodeType(str, Enum):
    GENERAL = "general"
    COMPUTE = "compute"
    MEMORY = "memory"
    GPU = "gpu"
    GATEWAY = "gateway"
    ORCHESTRATOR = "orchestrator"


class NodeStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    DECOMMISSIONED = "decommissioned"


class TriggerType(str, Enum):
    SCHEDULE = "schedule"
    EVENT = "event"
    WEBHOOK = "webhook"
    KAFKA = "kafka"
    WINDOWED = "windowed"
    CONDITIONAL = "conditional"
    CHAINED = "chained"
    MANUAL = "manual"


class NamespaceType(str, Enum):
    LOCAL = "local"
    PYPI = "pypi"
    GITHUB = "github"
    CUSTOM = "custom"


class AuditEventType(str, Enum):
    WORKFLOW_CREATED = "workflow_created"
    WORKFLOW_SUBMITTED = "workflow_submitted"
    WORKFLOW_APPROVED = "workflow_approved"
    WORKFLOW_REJECTED = "workflow_rejected"
    WORKFLOW_PROMOTED = "workflow_promoted"
    WORKFLOW_DEPRECATED = "workflow_deprecated"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_PAUSED = "execution_paused"
    EXECUTION_RESUMED = "execution_resumed"
    EXECUTION_STOPPED = "execution_stopped"
    EXECUTION_COMPLETED = "execution_completed"
    EXECUTION_FAILED = "execution_failed"
    HITL_REQUESTED = "hitl_requested"
    HITL_ASSIGNED = "hitl_assigned"
    HITL_RESPONDED = "hitl_responded"
    HITL_ESCALATED = "hitl_escalated"
    HITL_SLA_BREACHED = "hitl_sla_breached"
    DELEGATION_CREATED = "delegation_created"
    DELEGATION_USED = "delegation_used"
    DELEGATION_REVOKED = "delegation_revoked"
    BREAK_GLASS_INITIATED = "break_glass_initiated"
    BREAK_GLASS_REVIEWED = "break_glass_reviewed"
    ROLE_ASSIGNED = "role_assigned"
    ROLE_REVOKED = "role_revoked"
    USER_IMPERSONATED = "user_impersonated"
    NODE_REGISTERED = "node_registered"
    NODE_DECOMMISSIONED = "node_decommissioned"
    EVENT_PUBLISHED = "event_published"
    EVENT_DEPRECATED = "event_deprecated"


class NotificationChannel(str, Enum):
    WIDGET = "widget"
    EMAIL = "email"
    SLACK = "slack"
    WHATSAPP = "whatsapp"
    TEAMS = "teams"
    PAGERDUTY = "pagerduty"
