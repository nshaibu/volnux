"""Encryption at rest for the sensitive fields of the governance models.

The key is the ``ENCRYPTION_KEY`` setting, or the ``SECRET_KEY`` setting when that is
not set. Values are encrypted with Fernet (AES-128-CBC + HMAC-SHA256); the stored
form is ``{"__enc__": "<token>"}`` so a stored record never holds the plain value.
"""

import base64
import hashlib
import json
from typing import Any

from volnux.config import setting
from volnux.exceptions import ImproperlyConfigured

MARKER = "__enc__"


def _fernet():
    from cryptography.fernet import Fernet

    secret = setting("ENCRYPTION_KEY", None) or setting("SECRET_KEY", None)
    if not secret:
        raise ImproperlyConfigured(
            "Neither ENCRYPTION_KEY nor SECRET_KEY is set: sensitive fields cannot "
            "be stored. Set VOLNUX_SECRET_KEY (or VOLNUX_ENCRYPTION_KEY)."
        )
    key = base64.urlsafe_b64encode(hashlib.sha256(str(secret).encode()).digest())
    return Fernet(key)


def is_encrypted(value: Any) -> bool:
    return isinstance(value, dict) and set(value) == {MARKER}


def encrypt_value(value: Any) -> Any:
    """``value`` as ``{"__enc__": token}``; ``None`` and already-encrypted pass through."""
    if value is None or is_encrypted(value):
        return value
    payload = json.dumps(value, default=str).encode()
    return {MARKER: _fernet().encrypt(payload).decode()}


def decrypt_value(value: Any) -> Any:
    if not is_encrypted(value):
        return value
    from cryptography.fernet import InvalidToken

    try:
        return json.loads(_fernet().decrypt(value[MARKER].encode()))
    except InvalidToken:
        raise ImproperlyConfigured(
            "A stored value could not be decrypted: the ENCRYPTION_KEY / SECRET_KEY "
            "differs from the one it was saved with."
        )
