import json
import typing
import zlib

from pydantic_mini import Attrib, BaseModel, MiniAnnotated

from volnux.exceptions import RemoteExecutionError

from .checksum import generate_signature, verify_data


def ensure_json_serializable(instance, v: typing.Any) -> typing.Any:
    try:
        json.dumps(v)
    except (TypeError, OverflowError) as e:
        raise ValueError(f"Value is not JSON serializable: {e}")
    return v


class TaskMessage(BaseModel):
    """Message format for task communication"""

    event: str
    args: MiniAnnotated[
        typing.Dict[str, typing.Any], Attrib(validators=[ensure_json_serializable])
    ]
    correlation_id: typing.Optional[str] = None

    def serialize(self) -> bytes:
        return serialize_object(self)


def serialize_object(obj) -> bytes:
    obj_dict = obj.dump(_format="dict")

    signature, algorithm = generate_signature(obj_dict)
    obj_dict["_signature"] = signature
    obj_dict["_algorithm"] = algorithm

    data = json.dumps(obj_dict, sort_keys=True).encode("utf-8")
    return zlib.compress(data)


def serialize_dict(data: typing.Dict[str, typing.Any]) -> bytes:
    """Serialize a dictionary with HMAC signature."""
    working_data = data.copy()
    signature, algorithm = generate_signature(working_data)
    working_data["_signature"] = signature
    working_data["_algorithm"] = algorithm

    data = json.dumps(working_data, sort_keys=True).encode("utf-8")
    return zlib.compress(data)


def deserialize_message(data: bytes) -> typing.Tuple[typing.Any, bool]:
    decompressed_data = zlib.decompress(data)
    decompressed_data = json.loads(decompressed_data)

    if not verify_data(decompressed_data):
        raise RemoteExecutionError("INVALID_CHECKSUM")

    # remove signature and algorithm
    decompressed_data.pop("_signature", None)
    decompressed_data.pop("_algorithm", None)

    try:
        task_msg = TaskMessage(**decompressed_data)
        return task_msg, True
    except Exception:
        return decompressed_data, False
