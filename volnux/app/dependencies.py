import jwt
from typing import Union, TypedDict
from fastapi import Depends, Query

from .utils import get_current_user


def get_org_id(user: dict = Depends(get_current_user)) -> str:
    """Extract organization ID from authenticated user context."""
    return user["organization_id"]


def Pagination(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    return {"page": page, "page_size": page_size}


def _serialize_model(model) -> Union[dict, list, TypedDict]:
    """Serialize a model instance, excluding internal fields."""
    state = model.__getstate__()
    # Remove internal persistence fields from API responses
    state.pop("_backend_store", None)
    state.pop("_backend_config", None)
    state.pop("_backend_class", None)
    state.pop("_loaded_from_backend", None)
    return state
