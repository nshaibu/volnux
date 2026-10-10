from typing import List
from typing_extensions import TypedDict, Literal


class UserProfileModel(TypedDict, total=False):
    id: int
    username: str
    email: str


class UserLoginInfoModel(TypedDict, total=False):
    access_token: str
    token_type: Literal["Bearer"]
    roles: List[str]
    user: UserProfileModel


class UserLoginResponse(TypedDict):
    status: Literal["success", "failure"]
    data: UserLoginInfoModel
