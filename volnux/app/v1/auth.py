from pydantic import BaseModel
from typing import List
from formax.typing import Annotated
from fastapi import Depends, Body, HTTPException, Form

from volnux.models import User, Role, RoleAssignment
from volnux.exceptions import ObjectDoesNotExist
from ..app import get_current_app
from ..dependencies import _serialize_model
from ..utils import create_jwt_token, get_current_user, RequestContext
from .models.user_login import UserLoginResponse, UserProfileModel

app = get_current_app()


class LoginForm(BaseModel):
    email: str
    password: str
    model_config = {"extra": "forbid"}


@app.post("/api/v1/auth/login", tags=["Auth"])
async def login(data: Annotated[LoginForm, Form()]) -> UserLoginResponse:
    """Authenticate a user and return a JWT token.

    In production, this would validate credentials against SSO or local auth.
    """
    try:
        user_qs = await User.filter_async(email=data.email)
        user = user_qs.first()
        if not user:
            raise HTTPException(401, "Invalid credentials")
        if not user.is_active:
            raise HTTPException(403, "Account deactivated")
    except (ObjectDoesNotExist, IndexError):
        raise HTTPException(401, "Invalid credentials")

    # Get user's role assignments
    assignments = await RoleAssignment.filter_async(user__object_id=user.id)
    role_ids = [a.role.id for a in assignments]
    roles = []
    for rid in role_ids:
        try:
            role: Role = await Role.get_async(rid)
            roles.append(role.slug)
        except ObjectDoesNotExist:
            pass

    token = create_jwt_token(user.id, user.organization.id, roles)

    return {
        "status": "success",
        "data": {
            "access_token": token,
            "token_type": "Bearer",
            "user": _serialize_model(user),
            "roles": roles,
        },
    }


@app.get("/api/v1/auth/me", tags=["Auth"])
async def get_me(request: Annotated[RequestContext, Depends(get_current_user)]):
    """Get the current authenticated user's profile."""
    return {"status": "success", "data": _serialize_model(request.user)}
