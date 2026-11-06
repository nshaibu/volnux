from pydantic import BaseModel
from formax.typing import Annotated
from fastapi import Depends, HTTPException, Form

from volnux.models import User
from ..app import get_current_app
from ..dependencies import _serialize_model
from ..security import verify_password
from ..utils import (
    create_jwt_token,
    current_role_slugs,
    get_current_user,
    RequestContext,
)
from .models.user_login import UserLoginResponse

app = get_current_app()


class LoginForm(BaseModel):
    email: str
    password: str

    model_config = {"extra": "forbid"}


@app.post("/api/v1/auth/login", tags=["Auth"])
async def login(data: Annotated[LoginForm, Form()]) -> UserLoginResponse:
    """Authenticate a user with email and password and return a JWT token."""
    candidates = await User.filter_async(email=data.email)
    # An email may exist in more than one organization: the password decides which.
    user = next(
        (u for u in candidates if verify_password(data.password, u.password_hash)),
        None,
    )
    if user is None:
        # (one message for "no such user" and "wrong password")
        raise HTTPException(401, "Invalid credentials")
    if not user.is_active:
        raise HTTPException(403, "Account deactivated")

    roles = await current_role_slugs(user)
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
    return {
        "status": "success",
        "data": {**_serialize_model(request.user), "roles": request.roles},
    }
