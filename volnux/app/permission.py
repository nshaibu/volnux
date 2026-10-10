from typing import Dict, List
from .enums import Permission

# Role-to-permission mapping
ROLE_PERMISSIONS: Dict[str, List[Permission]] = {
    "workflow-author": [
        Permission.WORKFLOW_CREATE,
        Permission.WORKFLOW_READ,
        Permission.WORKFLOW_UPDATE,
        Permission.WORKFLOW_SUBMIT,
        Permission.EVENT_READ,
    ],
    "workflow-reviewer": [
        Permission.WORKFLOW_READ,
        Permission.APPROVAL_REVIEW,
        Permission.APPROVAL_APPROVE,
        Permission.APPROVAL_REJECT,
    ],
    "workflow-operator": [
        Permission.EXECUTION_READ,
        Permission.EXECUTION_CONTROL,
        Permission.EXECUTION_TRACE,
        Permission.HITL_READ,
        Permission.HITL_RESPOND,
        Permission.DELEGATION_CREATE,
        Permission.DELEGATION_REVOKE,
    ],
    "workflow-consumer": [
        Permission.WORKFLOW_READ,
        Permission.WORKFLOW_EXECUTE,
    ],
    "eventhub-publisher": [
        Permission.EVENT_READ,
        Permission.EVENT_PUBLISH,
        Permission.EVENT_MANAGE,
    ],
    "eventhub-consumer": [
        Permission.EVENT_READ,
    ],
    "mesh-node-admin": [
        Permission.NODE_READ,
        Permission.NODE_REGISTER,
        Permission.NODE_DECOMMISSION,
    ],
    "governance-auditor": [
        Permission.AUDIT_READ,
        Permission.AUDIT_EXPORT,
        Permission.WORKFLOW_READ,
        Permission.EXECUTION_READ,
        Permission.EXECUTION_TRACE,
        Permission.HITL_READ,
    ],
    "compliance-approver": [
        Permission.WORKFLOW_READ,
        Permission.APPROVAL_REVIEW,
        Permission.APPROVAL_APPROVE,
        Permission.APPROVAL_REJECT,
    ],
    "org-admin": [
        Permission.USER_CREATE,
        Permission.USER_READ,
        Permission.USER_UPDATE,
        Permission.USER_DELETE,
        Permission.TEAM_CREATE,
        Permission.TEAM_MANAGE,
        Permission.ROLE_ASSIGN,
        Permission.ROLE_REVOKE,
        Permission.ORG_MANAGE,
        Permission.TRIGGER_CREATE,
        Permission.TRIGGER_DELETE,
        Permission.NOTIFICATION_MANAGE,
    ],
    "super-admin": [Permission.SUPER_ADMIN],
}
