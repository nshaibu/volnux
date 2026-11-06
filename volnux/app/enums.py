from enum import Enum


class ClientType(str, Enum):
    """Registered client types for Volnux API access."""

    CLI = "cli"
    WEBCLIENT = "webclient"
    THIRD_PARTY = "third_party"


class Permission(str, Enum):
    """Granular permissions for API operations."""

    # Workflow permissions
    WORKFLOW_CREATE = "workflow.create"
    WORKFLOW_READ = "workflow.read"
    WORKFLOW_UPDATE = "workflow.update"
    WORKFLOW_DELETE = "workflow.delete"
    WORKFLOW_SUBMIT = "workflow.submit"
    WORKFLOW_PUBLISH = "workflow.publish"
    WORKFLOW_EXECUTE = "workflow.execute"

    # Approval permissions
    APPROVAL_REVIEW = "approval.review"
    APPROVAL_APPROVE = "approval.approve"
    APPROVAL_REJECT = "approval.reject"
    APPROVAL_MANAGE = "approval.manage"

    # Execution permissions
    EXECUTION_READ = "execution.read"
    EXECUTION_CONTROL = "execution.control"  # pause, resume, stop
    EXECUTION_TRACE = "execution.trace"

    # HITL permissions
    HITL_READ = "hitl.read"
    HITL_RESPOND = "hitl.respond"

    # User & Team management
    USER_CREATE = "user.create"
    USER_READ = "user.read"
    USER_UPDATE = "user.update"
    USER_DELETE = "user.delete"
    TEAM_CREATE = "team.create"
    TEAM_MANAGE = "team.manage"
    ROLE_ASSIGN = "role.assign"
    ROLE_REVOKE = "role.revoke"

    # Audit permissions
    AUDIT_READ = "audit.read"
    AUDIT_EXPORT = "audit.export"

    # Delegation permissions
    DELEGATION_CREATE = "delegation.create"
    DELEGATION_REVOKE = "delegation.revoke"

    # Break-glass permissions
    BREAK_GLASS_INITIATE = "break_glass.initiate"
    BREAK_GLASS_REVIEW = "break_glass.review"

    # EventHub permissions
    EVENT_READ = "event.read"
    EVENT_PUBLISH = "event.publish"
    EVENT_MANAGE = "event.manage"

    # Node permissions
    NODE_READ = "node.read"
    NODE_REGISTER = "node.register"
    NODE_DECOMMISSION = "node.decommission"

    # Trigger permissions
    TRIGGER_CREATE = "trigger.create"
    TRIGGER_DELETE = "trigger.delete"

    # Notification permissions
    NOTIFICATION_MANAGE = "notification.manage"

    # Admin permissions
    ORG_MANAGE = "org.manage"
    SUPER_ADMIN = "super.admin"
