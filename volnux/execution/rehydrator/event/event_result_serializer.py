import logging
from typing import Any, Dict, List, Union

from volnux.result import EventResult
from volnux.utils import get_obj_klass_import_str


logger = logging.getLogger(__name__)

# Maps serialized key type names back to Python constructors
_KEY_TYPE_MAP = {"int": int, "float": float, "bool": bool}


class ExecResultSerializer:
    """Serializes and deserializes execution results for checkpoint storage.

    Produces JSON-compatible structures. Round-trips preserve types for
    primitives, sequences (list/tuple/set), mappings (with key type
    restoration for int/float/bool keys), EventResult, and custom objects
    supporting __getstate__/__dict__.
    """

    PRIMITIVE_TYPES = (int, str, float, bool, type(None))

    def __init__(self):
        from volnux.execution.rehydrator.engine.serializer import StateSerializer

        self.state_serializer = StateSerializer

    # --- Serialization ---

    def serialize_exec_result(self, result: Any) -> Any:
        """Serialize an execution result for checkpointing.

        Raises:
            TypeError: If result contains non-serializable types.
        """
        if result is None:
            return None

        if isinstance(result, self.PRIMITIVE_TYPES):
            return result

        if isinstance(result, EventResult):
            return self._serialize_event_result(result)

        if isinstance(result, (list, tuple)):
            return self._serialize_sequence(result, type(result))

        if isinstance(result, set):
            return self._serialize_set(result)

        if isinstance(result, dict):
            return self._serialize_mapping(result)

        return self._serialize_object(result)

    def _serialize_event_result(self, result: EventResult) -> Dict[str, Any]:
        return self.state_serializer.serialize_result(result)

    def _serialize_sequence(
        self, sequence: Union[List, tuple], original_type: type
    ) -> Union[List, Dict[str, Any]]:
        serialized_items = [self.serialize_exec_result(item) for item in sequence]

        if original_type is tuple:
            return {"__type__": "tuple", "items": serialized_items}

        return serialized_items

    def _serialize_set(self, set_obj: set) -> Dict[str, Any]:
        serialized_items = [self.serialize_exec_result(item) for item in set_obj]
        return {"__type__": "set", "items": serialized_items}

    def _serialize_mapping(self, mapping: Dict) -> Dict[str, Any]:
        serialized: Dict[str, Any] = {}
        key_types: Dict[str, str] = {}

        for key, value in mapping.items():
            # bool before int — bool is a subclass of int in Python
            if isinstance(key, str):
                str_key = key
            elif isinstance(key, bool):
                str_key = str(key)
                key_types[str_key] = "bool"
            elif isinstance(key, (int, float)):
                str_key = str(key)
                key_types[str_key] = type(key).__name__
            else:
                raise TypeError(
                    f"Cannot serialize dict key {key!r} of type "
                    f"{type(key).__name__}: only str, int, float, and bool keys "
                    f"are supported. Use acquire_resource() for non-serializable data."
                )

            # Detect key collisions from mixed-type keys (e.g. 1 and "1")
            if str_key in serialized:
                raise TypeError(
                    f"Dict key collision during serialization: "
                    f"{key!r} ({type(key).__name__}) maps to string '{str_key}' "
                    f"which is already occupied"
                )

            serialized[str_key] = self.serialize_exec_result(value)

        if key_types:
            serialized["__key_types__"] = key_types

        return serialized

    def _serialize_object(self, obj: Any) -> Dict[str, Any]:
        class_path = get_obj_klass_import_str(obj)

        if hasattr(obj, "__getstate__"):
            state = self.serialize_exec_result(obj.__getstate__())
            return {
                "__type__": "custom_object",
                "__class_path__": class_path,
                "state": state,
                "uses_getstate": True,
            }

        if hasattr(obj, "__dict__"):
            serialized_dict = {
                key: self.serialize_exec_result(value)
                for key, value in obj.__dict__.items()
            }
            return {
                "__type__": "custom_object",
                "__class_path__": class_path,
                "state": serialized_dict,
                "uses_getstate": False,
            }

        raise TypeError(
            f"Cannot serialize object of type {type(obj).__name__}: "
            f"no __getstate__ or __dict__ available"
        )

    # Deserialization
    def deserialize_exec_result(self, data: Any) -> Any:
        """Deserialize an execution result from a checkpoint.

        Raises:
            ValueError: If a data format is invalid.
            ImportError: If a class cannot be imported during object restoration.
        """
        if data is None:
            return None

        if isinstance(data, self.PRIMITIVE_TYPES):
            return data

        # Check for type markers
        if isinstance(data, dict) and "__type__" in data:
            type_marker = data["__type__"]

            if type_marker == "tuple":
                return tuple(
                    self.deserialize_exec_result(item) for item in data["items"]
                )

            if type_marker == "set":
                return set(self.deserialize_exec_result(item) for item in data["items"])

            if type_marker == "EventResult":
                return self._deserialize_event_result(data)

            if type_marker == "custom_object":
                return self._deserialize_object(data)

        # Regular dict — check for non-string key restoration
        if isinstance(data, dict):
            key_types = data.get("__key_types__")
            if key_types:
                items = {k: v for k, v in data.items() if k != "__key_types__"}
                result = {}
                for str_key, value in items.items():
                    if str_key in key_types:
                        original_key = self._restore_dict_key(
                            str_key, key_types[str_key]
                        )
                        result[original_key] = self.deserialize_exec_result(value)
                    else:
                        result[str_key] = self.deserialize_exec_result(value)
                return result

            return {
                key: self.deserialize_exec_result(value) for key, value in data.items()
            }

        if isinstance(data, list):
            return [self.deserialize_exec_result(item) for item in data]

        logger.warning("Unknown data type during deserialization: %s", type(data))
        return data

    @staticmethod
    def _restore_dict_key(str_key: str, type_name: str) -> Any:
        """Restore a dict key from its string representation and recorded type."""
        key_cls = _KEY_TYPE_MAP.get(type_name)
        if key_cls is None:
            logger.warning(
                "Unknown key type '%s' for key '%s', returning as string",
                type_name,
                str_key,
            )
            return str_key
        try:
            return key_cls(str_key)
        except (ValueError, TypeError) as e:
            logger.warning(
                "Failed to restore key '%s' as %s: %s, returning as string",
                str_key,
                type_name,
                e,
            )
            return str_key

    def _deserialize_event_result(self, data: Dict[str, Any]) -> EventResult:
        if self.state_serializer and hasattr(
            self.state_serializer, "deserialize_result"
        ):
            return self.state_serializer.deserialize_result(data)

        from volnux.import_utils import import_string

        class_path = data.get("__class_path__")
        if not class_path:
            raise ValueError("Missing __class_path__ in EventResult data")

        result_class = import_string(class_path)

        if "data" in data and hasattr(result_class, "from_dict"):
            return result_class.from_dict(data["data"])

        if "state" in data:
            instance = result_class.__new__(result_class)
            if hasattr(instance, "__setstate__"):
                instance.__setstate__(data["state"])
            else:
                instance.__dict__.update(data["state"])
            return instance

        raise ValueError(f"Cannot deserialize EventResult from data: {data}")

    def _deserialize_object(self, data: Dict[str, Any]) -> Any:
        from volnux.import_utils import import_string

        class_path = data.get("__class_path__")
        if not class_path:
            raise ValueError("Missing __class_path__ in object data")

        obj_class = import_string(class_path)
        instance = obj_class.__new__(obj_class)

        state = self.deserialize_exec_result(data["state"])

        if data.get("uses_getstate", False) and hasattr(instance, "__setstate__"):
            instance.__setstate__(state)
        else:
            if isinstance(state, dict):
                instance.__dict__.update(state)
            else:
                raise ValueError(
                    f"Expected dict for __dict__ restoration, got {type(state)}"
                )

        return instance


# Module-level singleton — created once, reused across all serialization calls.
# Avoids per-call instantiation and repeated lazy imports.
EXEC_RESULT_SERIALIZER = ExecResultSerializer()
