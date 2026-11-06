"""Creating the first account of a deployment (``volnux createsuperuser``)."""

import re
from typing import Optional

from volnux.exceptions import ObjectExistError
from volnux.models import AuditEntry, Organization, Role, RoleAssignment, User
from volnux.models.enums import AuditEventType, ScopeType

from .schemas import EMAIL
from .security import hash_password

MIN_PASSWORD_LENGTH = 8
SUPER_ADMIN_SLUG = "super-admin"


class BootstrapError(ValueError):
    """The account cannot be created (bad input, or it already exists)."""


async def create_superuser(
    email: str,
    name: str,
    password: str,
    org_slug: str,
    org_name: Optional[str] = None,
) -> User:
    """Create a super-admin, and its organization and ``super-admin`` role if needed.

    :raises BootstrapError: invalid input, or a user with this email already exists
        in the organization.
    """
    if not re.match(EMAIL, email):
        raise BootstrapError(f"{email!r} is not a valid email address")
    if not name.strip():
        raise BootstrapError("A name is required")
    if len(password) < MIN_PASSWORD_LENGTH:
        raise BootstrapError(
            f"The password must have at least {MIN_PASSWORD_LENGTH} characters"
        )
    if not re.match(r"^[a-z0-9-]+$", org_slug):
        raise BootstrapError("The organization slug may only contain a-z, 0-9 and '-'")

    orgs = await Organization.filter_async(slug=org_slug)
    if orgs:
        organization = orgs[0]
    else:
        organization = Organization(name=org_name or org_slug, slug=org_slug)
        await organization.save()

    roles = await Role.filter_async(slug=SUPER_ADMIN_SLUG)
    if roles:
        role = roles[0]
    else:
        role = Role(name="Super admin", slug=SUPER_ADMIN_SLUG)
        await role.save()

    user = User(
        name=name,
        email=email,
        password_hash=hash_password(password),
        organization=organization,
    )
    try:
        await user.save()  # (an existing email in the organization is refused here)
    except ObjectExistError:
        raise BootstrapError(
            f"A user with the email {email} already exists in {org_slug!r}"
        )

    await RoleAssignment(
        user=user,
        role=role,
        scope_type=ScopeType.GLOBAL,
        granted_by=user,
    ).save()
    await AuditEntry.append(
        organization=organization,
        event_type=AuditEventType.ROLE_ASSIGNED,
        actor=user,
        actor_role=SUPER_ADMIN_SLUG,
        target_type="user",
        target_id=user.id,
        action="created as a super-admin by `volnux createsuperuser`",
        metadata={"role": SUPER_ADMIN_SLUG},
    )
    return user
