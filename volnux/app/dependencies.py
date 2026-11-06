import enum
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from fastapi import Depends, HTTPException, Query

from .utils import RequestContext, get_current_user

# Fields never sent to a client.
_HIDDEN_FIELDS = {
    "password_hash",
    "_backend_store",
    "_backend_config",
    "_backend_class",
    "_loaded_from_backend",
}
# Keys of a free-form dict whose value is replaced by a placeholder.
_SECRET_KEY_PARTS = (
    "secret",
    "token",
    "password",
    "api_key",
    "apikey",
    "authorization",
)
REDACTED = "***"


def get_ctx(ctx: RequestContext = Depends(get_current_user)) -> RequestContext:
    return ctx


def get_org_id(ctx: RequestContext = Depends(get_current_user)) -> str:
    """The authenticated user's organization id."""
    return ctx.organization.id


def Pagination(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    return {"page": page, "page_size": page_size}


def page_args(pagination: dict) -> Dict[str, int]:
    """``limit``/``offset`` keyword arguments for ``filter_async``."""
    return {
        "limit": pagination["page_size"],
        "offset": (pagination["page"] - 1) * pagination["page_size"],
    }


def meta(pagination: dict, total: int) -> Dict[str, int]:
    return {
        "page": pagination["page"],
        "page_size": pagination["page_size"],
        "total": total,
    }


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: (
                REDACTED
                if any(part in str(k).lower() for part in _SECRET_KEY_PARTS)
                else _redact(v)
            )
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        if value.get("type") == "foreignkey" and "object_id" in value:
            return value["object_id"]
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(v) for v in value]
    return value


def _serialize_model(model, redact: Iterable[str] = ("config", "manifest")) -> dict:
    """A JSON-safe dict of a model: foreign keys become ids, enums and datetimes
    become strings, credentials and persistence internals are left out, and the
    free-form dicts named in ``redact`` have secret-looking values masked."""
    state = model.__getstate__()
    out = {k: v for k, v in state.items() if k not in _HIDDEN_FIELDS}
    # (encrypted fields are serialised encrypted: send the plain value instead)
    for name in getattr(model, "encrypted_fields", ()):
        if name in out:
            out[name] = getattr(model, name)
    for name in redact:
        if isinstance(out.get(name), dict):
            out[name] = _redact(out[name])
    return _plain(out)


def not_found(what: str = "Resource") -> HTTPException:
    return HTTPException(status_code=404, detail=f"{what} not found")


def ensure_same_org(ctx: RequestContext, org_id: Optional[str], what: str = "Resource"):
    """404 (not 403, which would confirm the id exists) for another organization's record."""
    if org_id != ctx.organization.id:
        raise not_found(what)


def apply_updates(model, changes: Dict[str, Any], allowed: Iterable[str]) -> None:
    """Set only the ``allowed`` fields that were sent."""
    for name in allowed:
        if name in changes:
            setattr(model, name, changes[name])
