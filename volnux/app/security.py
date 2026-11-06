"""Password hashing for API logins (stdlib scrypt, salted, constant-time compare)."""

import base64
import hashlib
import hmac
import os
from typing import Optional

_N, _R, _P = 2**14, 8, 1


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P)
    return "scrypt$%s$%s" % (
        base64.b64encode(salt).decode(),
        base64.b64encode(digest).decode(),
    )


def verify_password(password: str, stored: Optional[str]) -> bool:
    """False for a missing or malformed hash; never raises."""
    if not stored:
        return False
    try:
        scheme, salt_b64, digest_b64 = stored.split("$")
        if scheme != "scrypt":
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(digest_b64)
        actual = hashlib.scrypt(
            password.encode(), salt=salt, n=_N, r=_R, p=_P, dklen=len(expected)
        )
    except Exception:
        return False
    return hmac.compare_digest(actual, expected)
