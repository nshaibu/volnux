import base64
import hashlib
import hmac
import json
import typing

from volnux.conf import ConfigLoader
from volnux.exceptions import RemoteExecutionError

CONF = ConfigLoader.get_lazily_loaded_config()

ALGORITHM = "sha256"


def get_secret_key() -> bytes:
    """Retrieve the secret key from configuration and ensure it is bytes."""
    key = CONF.SECRET_KEY
    if isinstance(key, str):
        return key.encode("utf-8")
    return key


def generate_signature(data: typing.Dict[str, typing.Any]) -> typing.Tuple[bytes, str]:
    """Generate a signature for the payload."""
    data_bytes = json.dumps(data, sort_keys=True).encode("utf-8")

    signature = hmac.new(
        get_secret_key(), data_bytes, getattr(hashlib, ALGORITHM)
    ).digest()

    return base64.b64encode(signature).decode("utf-8"), ALGORITHM


def verify_data(data: typing.Dict[str, typing.Any]) -> bool:
    """verify incoming payload's signature to check if it is the expected signature."""
    if "_signature" not in data:
        raise RemoteExecutionError("INVALID_CHECKSUM")

    received_signature = data["_signature"]
    try:
        received_signature_bytes = base64.b64decode(received_signature)
    except Exception:
        return False

    verification_data = data.copy()
    verification_data.pop("_signature", None)
    verification_data.pop("_algorithm", None)

    verification_bytes = json.dumps(verification_data, sort_keys=True).encode("utf-8")

    expected_signature = hmac.new(
        get_secret_key(),
        verification_bytes,
        getattr(hashlib, ALGORITHM),
    ).digest()

    return hmac.compare_digest(received_signature_bytes, expected_signature)
