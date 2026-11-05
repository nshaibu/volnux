import jwt
import logging
import secrets
import dataclasses
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Union
from fastapi.security import HTTPBearer
from fastapi import HTTPException, Depends, Header
from fastapi.security import HTTPAuthorizationCredentials

from .enums import ClientType
from .permission import Permission, ROLE_PERMISSIONS
from volnux.models import User, AuditEntry, Organization
from volnux.models.enums import AuditEventType
from volnux.config import setting
from volnux.exceptions import ImproperlyConfigured, ObjectDoesNotExist


security = HTTPBearer()

logger = logging.getLogger(__name__)

# The SECRET_KEY / JWT_ALGORITHM / JWT_EXPIRATION_HOURS settings. (The secret used
# to be written here, readable by anyone with the source, so anyone could forge a
# token.)
_ALLOWED_ALGORITHMS = ("HS256", "HS384", "HS512")
_development_secret: Optional[str] = None


def get_jwt_secret() -> str:
    """The key that signs tokens: the ``SECRET_KEY`` setting.

    It is required unless ``DEBUG`` is on. With ``DEBUG`` a random one is made for
    the process (tokens stop working on restart, which is acceptable there).

    :raises ImproperlyConfigured: no secret key and ``DEBUG`` is off.
    """
    global _development_secret
    secret = setting("SECRET_KEY", None)
    if secret:
        return str(secret)
    if not setting("DEBUG", False):
        raise ImproperlyConfigured(
            "SECRET_KEY is not set. Set it in the environment, for example "
            '`export VOLNUX_SECRET_KEY="$(openssl rand -hex 32)"`, or run with '
            "VOLNUX_DEBUG=true (development only)."
        )
    if _development_secret is None:
        _development_secret = secrets.token_hex(32)
        logger.warning(
            "SECRET_KEY is not set: using a random key for this process "
            "(development only; tokens stop working when it restarts)"
        )
    return _development_secret


def get_jwt_algorithm() -> str:
    algorithm = str(setting("JWT_ALGORITHM", "HS256")).upper()
    if algorithm not in _ALLOWED_ALGORITHMS:
        raise ImproperlyConfigured(
            f"JWT_ALGORITHM {algorithm!r} is not allowed; use one of "
            f"{', '.join(_ALLOWED_ALGORITHMS)} (the key is a shared secret)"
        )
    return algorithm


def get_jwt_expiration_hours() -> float:
    return float(setting("JWT_EXPIRATION_HOURS", 24))


@dataclasses.dataclass
class RequestContext:
    user: User
    organization: Organization
    roles: List[str]
    client_type: Union[ClientType, str]
    client_id: Optional[str] = None


def create_jwt_token(user_id: str, org_id: str, roles: List[str]) -> str:
    """Create a JWT token for a user."""
    payload = {
        "sub": user_id,
        "org": org_id,
        "roles": roles,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=get_jwt_expiration_hours()),
    }
    return jwt.encode(payload, get_jwt_secret(), algorithm=get_jwt_algorithm())


def decode_jwt_token(token: str) -> dict:
    """Decode and validate a JWT token."""
    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[get_jwt_algorithm()])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    x_client_type: Optional[str] = Header(None, alias="X-Client-Type"),
    x_client_id: Optional[str] = Header(None, alias="X-Client-ID"),
) -> RequestContext:
    """Authenticate the current user from JWT token and validate client type.

    Args:
        credentials: Bearer token from Authorization header.
        x_client_type: Client type header (cli, webclient, third_party).
        x_client_id: Client identifier for audit logging.

    Returns:
        Dict with user_id, organization_id, roles, client_type, client_id.

    Raises:
        401: If token is invalid or a client type is not registered.
    """
    payload = decode_jwt_token(credentials.credentials)

    # Validate a client type
    client_type = x_client_type or ClientType.WEBCLIENT

    if client_type not in [ct.value for ct in ClientType]:
        raise HTTPException(
            status_code=401,
            detail=f"Invalid client type: {client_type}. Must be one of: {[ct.value for ct in ClientType]}",
        )

    # Verify a client is registered (for third-party integrations)
    if client_type == ClientType.THIRD_PARTY and not x_client_id:
        raise HTTPException(
            status_code=401,
            detail="X-Client-ID header required for third-party integrations",
        )

    # Verify the user still exists and is active
    try:
        user: User = User.get(payload["sub"])
        if not user.is_active:
            raise HTTPException(status_code=403, detail="User account is deactivated")
    except ObjectDoesNotExist:
        raise HTTPException(status_code=401, detail="User no longer exists")

    return RequestContext(
        user=user,
        organization=Organization.get(payload["org"]),
        roles=payload["roles"],
        client_type=client_type,
        client_id=x_client_id,
    )


def require_permission(permission: Permission):
    """Dependency factory: require a specific permission.

    Usage:
        @app.post("/workflows")
        async def create_workflow(
            user: dict = Depends(get_current_user),
            _: None = Depends(require_permission(Permission.WORKFLOW_CREATE)),
        ):
            ...
    """

    async def check_permission(user: dict = Depends(get_current_user)):
        user_roles = user.get("roles", [])
        allowed = False

        for role in user_roles:
            role_perms = ROLE_PERMISSIONS.get(role, [])
            if permission in role_perms or Permission.SUPER_ADMIN in role_perms:
                allowed = True
                break

        if not allowed:
            raise HTTPException(
                status_code=403, detail=f"Permission denied: {permission.value}"
            )

    return check_permission


def require_any_permission(*permissions: Permission):
    """Dependency factory: require at least one of the specified permissions."""

    async def check_permissions(user: dict = Depends(get_current_user)):
        user_roles = user.get("roles", [])

        for role in user_roles:
            role_perms = ROLE_PERMISSIONS.get(role, [])
            if Permission.SUPER_ADMIN in role_perms:
                return
            if any(p in role_perms for p in permissions):
                return

        raise HTTPException(
            status_code=403,
            detail=f"Permission denied: requires one of {[p.value for p in permissions]}",
        )

    return check_permissions


def require_role(role_name: str):
    """Dependency factory: require a specific role."""

    async def check_role(user: dict = Depends(get_current_user)):
        if role_name not in user.get("roles", []) and "super-admin" not in user.get(
            "roles", []
        ):
            raise HTTPException(status_code=403, detail=f"Role required: {role_name}")

    return check_role


def audit_log(
    organization: Organization,
    event_type: AuditEventType,
    actor: User,
    actor_role: str,
    target_type: str,
    target_id: str,
    action: str,
    metadata: Optional[dict] = None,
) -> None:
    """Create an immutable audit log entry."""
    try:
        entry = AuditEntry(
            organization=organization,
            event_type=event_type,
            actor=actor,
            actor_role=actor_role,
            target_type=target_type,
            target_id=target_id,
            action=action,
            metadata=metadata or {},
        )
        entry.save()
    except Exception as e:
        logger.error(f"Failed to create audit entry: {e}")


def formax_jsonable_encoder(obj):
    """Custom encoder that handles Formax models and ForeignKey references."""
    if dataclasses.is_dataclass(obj):
        result = {}
        for field in dataclasses.fields(obj):
            value = getattr(obj, field.name)
            # Skip internal persistence fields
            if field.name.startswith("_"):
                continue
            # Resolve ForeignKey references
            if hasattr(value, "resolve"):
                try:
                    resolved = value.resolve()
                    if resolved is not None:
                        value = formax_jsonable_encoder(resolved)
                except Exception:
                    value = str(value)
            elif dataclasses.is_dataclass(value):
                value = formax_jsonable_encoder(value)
            elif isinstance(value, list):
                value = [
                    formax_jsonable_encoder(v) if dataclasses.is_dataclass(v) else v
                    for v in value
                ]
            result[field.name] = value
        return result
    return obj
