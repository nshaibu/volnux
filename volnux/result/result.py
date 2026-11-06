import base64
import logging
import os
import pickle
import typing
import itertools
from dataclasses import asdict, InitVar, dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Type, Any, List, Set, Iterator, Union

from formax.typing import is_builtin_type
from formax import Attrib, BaseModel, MiniAnnotated, InitStrategy

from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.utils import get_obj_klass_import_str, get_obj_state
from volnux.exceptions import MultiValueError, SerializationError
from volnux.import_utils import import_string

if typing.TYPE_CHECKING:
    from volnux.backends.store import KeyValueStoreBackendBase

__all__ = ["EventResult", "ResultSet"]

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="KeyValueStoreIntegrationMixin")

Result = typing.TypeVar("Result", bound=typing.Hashable)

_CONTENT_TYPE_KEY = "content_type_import_str"
_CONTENT_STATE_KEY = "state"
_CONTENT_PICKLE_KEY = "pickle"


def _supports_state_restore(content: Any) -> bool:
    """True if ``content`` can be rebuilt from ``__new__`` + ``__setstate__``.

    ``object`` provides a default ``__getstate__`` but no ``__setstate__``, so
    only classes that define one (formax models, opted-in classes) qualify.
    The class must also be importable by path, i.e. defined at module level.
    """
    klass = type(content)
    if not callable(getattr(klass, "__setstate__", None)):
        return False
    try:
        return import_string(get_obj_klass_import_str(content)) is klass
    except Exception:
        return False


def _serialize_content(content: Any) -> Dict[str, Any]:
    """Serialize non-builtin result content.

    Content that restores from its state keeps the readable
    ``{"content_type_import_str", "state"}`` form. Anything else (plain
    classes, dataclasses, datetime, Decimal, OrderedDict, ...) is pickled and
    base64-encoded so the state stays JSON-safe for persistent backends.
    """
    import_str = get_obj_klass_import_str(content)

    if _supports_state_restore(content):
        content_state = get_obj_state(content)
        if content_state is not None:
            return {_CONTENT_TYPE_KEY: import_str, _CONTENT_STATE_KEY: content_state}

    try:
        payload = pickle.dumps(content, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        raise SerializationError(
            f"EventResult content of type {import_str!r} cannot be serialized: {e}. "
            "Use a builtin type, a formax model, or a module-level picklable class."
        ) from e

    return {
        _CONTENT_TYPE_KEY: import_str,
        _CONTENT_PICKLE_KEY: base64.b64encode(payload).decode("ascii"),
    }


def _deserialize_content(content: Dict[str, Any]) -> Any:
    """Rebuild content produced by :func:`_serialize_content`."""
    import_str = content[_CONTENT_TYPE_KEY]
    try:
        if _CONTENT_PICKLE_KEY in content:
            # Only unpickle data this application wrote: pickle can execute
            # code, so the persistence backend must be trusted.
            return pickle.loads(base64.b64decode(content[_CONTENT_PICKLE_KEY]))

        klass = import_string(import_str)
        instance = klass.__new__(klass)  # type: ignore
        instance.__setstate__(content[_CONTENT_STATE_KEY])
        return instance
    except Exception as e:
        raise SerializationError(
            f"Failed to restore EventResult content of type {import_str!r}: {e}"
        ) from e


class EventResult(KeyValueStoreIntegrationMixin, BaseModel):
    error: bool
    event_name: str
    content: typing.Any
    workflow_id: typing.Optional[str]
    task_id: typing.Optional[str]
    process_id: MiniAnnotated[int, Attrib(default_factory=lambda: os.getpid())]
    creation_time: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    ]
    order: typing.Optional[int] = 0
    is_persisted: bool = False

    # Internal state and configuration
    autosave: InitVar[bool] = False
    storage_backend: InitVar[typing.Optional["KeyValueStoreBackendBase"]] = None

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def success(self) -> bool:
        return not self.error

    @success.setter
    def success(self, value: bool) -> None:
        self.error = not value

    def should_persist(self) -> bool:
        raise NotImplementedError(
            "should_persist method must be implemented by subclasses"
        )

    def get_state(self) -> typing.Dict[str, typing.Any]:
        state = dict(super().get_state())
        state.pop("_objectid_lock", None)

        if self.content is not None:
            content_type = type(self.content)
            if not is_builtin_type(content_type):  # type: ignore
                state["content"] = _serialize_content(self.content)

        return state

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        state = dict(state)  # don't mutate the caller's state
        content = state.get("content")

        if isinstance(content, dict) and _CONTENT_TYPE_KEY in content:
            state["content"] = _deserialize_content(content)

        state.pop("_objectid_lock", None)
        record_id = state.pop("id", None) or state.get("_id")
        # Same recursion hazard as get_state: __setstate__ calls set_state
        super().set_state(state)
        # Formax ignores the identity keys; restore them so a round-tripped
        # result keeps the id it is keyed by in ResultSets and backends.
        if record_id:
            self.change_object_id(record_id)

    def is_error(self) -> bool:
        return self.error

    def as_dict(self) -> typing.Dict[str, typing.Any]:
        """Serialize event result"""
        content = None
        if isinstance(self.content, Exception):
            # Exceptions don't deep-copy reliably, so asdict() runs without it
            content = self.content
            self.content = None
            try:
                result_dict = asdict(self)
            finally:
                self.content = content
        else:
            result_dict = asdict(self)
        if content is not None:
            if hasattr(content, "as_dict"):
                content = content.as_dict()
            elif hasattr(content, "to_dict"):
                content = content.to_dict()
            result_dict["content"] = content
        return result_dict


_FILTER_OPERATORS: typing.Final[frozenset[str]] = frozenset(
    {
        "contains",
        "startswith",
        "endswith",
        "icontains",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "exact",
        "isnull",
    }
)


@dataclass(frozen=True)
class EntityContentType:
    """Immutable, serializable content type descriptor."""

    model_path: str
    backend_alias: str = "default"

    @classmethod
    def from_entity(cls, entity: "Result") -> Optional["EntityContentType"]:
        """Create a content type descriptor from a Result entity."""
        if not entity or not getattr(entity, "id", None):
            return None
        return cls(
            model_path=get_obj_klass_import_str(entity),
            backend_alias=getattr(entity, "_backend_alias", "default"),
        )

    def resolve_model(self) -> Type["Result"]:
        """Resolve and return the referenced model class."""
        return import_string(self.model_path)  # type: ignore[return-value]

    def __repr__(self) -> str:
        return f"<EntityContentType: {self.model_path} (backend={self.backend_alias})>"


class ResultSet(typing.MutableSet[Result]):

    def __init__(self, results: Optional[List["Result"]] = None) -> None:
        self._content: Dict[str, "Result"] = {}
        self._context_types: Set[EntityContentType] = set()

        if results:
            for result in results:
                self._insert_entity(result)

    @staticmethod
    def _identity_key(value: "Result") -> str:
        """Generate a stable identity key that survives process boundaries.

        Priority:
          1. Formax-Py primary key (id) — stable across serialization
          2. ExecutionContext state_id — stable for execution contexts
          3. Python hash fallback — for transient/non-model results only
        """
        pk = getattr(value, "id", None) or getattr(value, "state_id", None)
        if pk is not None:
            return f"pk:{pk}"

        try:
            return f"hash:{hash(value)}"
        except TypeError:
            return f"ref:{id(value)}"

    def __contains__(self, item: object) -> bool:
        if not isinstance(item, object):
            return False
        try:
            key = self._identity_key(typing.cast("Result", item))
        except Exception:
            return False
        return key in self._content

    def __iter__(self) -> Iterator["Result"]:
        return iter(self._content.values())

    def __len__(self) -> int:
        return len(self._content)

    def add(self, value: Union["Result", "ResultSet"]) -> None:  # type: ignore[override]
        """Add a result or merge another ResultSet."""
        if isinstance(value, ResultSet):
            for result in value._content.values():
                self._insert_entity(result)
            self._context_types.update(value._context_types)
        else:
            self._insert_entity(value)

    def discard(self, value: Union["Result", "ResultSet"]) -> None:  # type: ignore[override]
        """Remove a result or all results from another ResultSet."""
        if isinstance(value, ResultSet):
            for res in value:
                self._content.pop(self._identity_key(res), None)
        else:
            self._content.pop(self._identity_key(value), None)

    def extend(self, results: typing.Collection["Result"]) -> None:
        """Add multiple items to the set."""
        for result in results:
            self._insert_entity(result)

    def clear(self) -> None:
        """Remove all results and content type metadata."""
        self._content.clear()
        self._context_types.clear()

    def copy(self) -> "ResultSet":
        """Create a shallow copy preserving content type metadata."""
        new = ResultSet()
        new._content = self._content.copy()
        new._context_types = self._context_types.copy()
        return new

    def __getitem__(self, index: int) -> "Result":
        """Access a result by index without materializing the full values list."""
        length = len(self._content)
        if index < 0:
            index += length
        if index < 0 or index >= length:
            raise IndexError(f"ResultSet index {index} out of range [0, {length})")
        return next(itertools.islice(self._content.values(), index, index + 1))

    def first(self) -> Optional["Result"]:
        """Return the first result or None if empty."""
        try:
            return self[0]
        except IndexError:
            return None

    def is_empty(self) -> bool:
        """Return True if the result set has no entries."""
        return len(self._content) == 0

    def count(self) -> int:
        """Return the number of unique results."""
        return len(self._content)

    def _insert_entity(self, record: "Result") -> None:
        """Insert an entity and track its content type."""
        key = self._identity_key(record)
        self._content[key] = record

        content_type = EntityContentType.from_entity(record)
        if content_type is not None:
            self._context_types.add(content_type)

    def get(self, **filters: Any) -> "Result":
        """Get a single result matching filters. Raises on 0 or >1 matches."""
        qs = self.filter(**filters)
        count = qs.count()
        if count == 0:
            raise KeyError(f"No result found matching filters: {filters}")
        if count > 1:
            raise MultiValueError(
                f"More than one result found for filters {filters}: {count} != 1"
            )
        return qs[0]

    def get_entry_by_identity(self, identity: Union[str, int]) -> "Result":
        """Retrieve an entry by its stable identity key or primary key."""
        key = str(identity) if isinstance(identity, int) else identity
        # Try direct lookup first (for pk:-prefixed keys)
        entry = self._content.get(key)
        if entry is not None:
            return entry
        # Try with pk: prefix for raw IDs
        prefixed = f"pk:{key}"
        entry = self._content.get(prefixed)
        if entry is not None:
            return entry
        raise KeyError(f"No result found with identity {identity}")

    def filter(self, **filter_params: Any) -> "ResultSet":
        """Filter results by attribute values with nested field and operator support.

        Content type metadata is inherited from the parent set since
        a filtered subset cannot contain types absent from the original.
        """
        filtered = [
            r for r in self._content.values() if self._matches_filters(r, filter_params)
        ]
        result = ResultSet(filtered)
        # Inherit content types — avoids O(n) re-inspection on chained filters
        result._context_types = self._context_types.copy()
        return result

    def exclude(self, **filter_params: Any) -> "ResultSet":
        """Exclude results matching filters. Complement of filter()."""
        excluded = [
            r
            for r in self._content.values()
            if not self._matches_filters(r, filter_params)
        ]
        result = ResultSet(excluded)
        result._context_types = self._context_types.copy()
        return result

    def _matches_filters(self, result: "Result", filters: Dict[str, Any]) -> bool:
        """Check if a result matches ALL filters (AND semantics)."""
        for key, value in filters.items():
            if "__" in key:
                parts = key.split("__")
                if parts[-1] in _FILTER_OPERATORS:
                    field_path = "__".join(parts[:-1])
                    try:
                        matched = self._check_operator(
                            result, field_path, parts[-1], value
                        )
                    except TypeError:
                        # Incomparable types (e.g. str > int) are a non-match,
                        # consistent with the exact-match branch below.
                        matched = False
                    if not matched:
                        return False
                else:
                    # Nested lookup without operator suffix
                    if not self._check_nested_field(result, parts, value):
                        return False
            else:
                # Simple exact match
                try:
                    if getattr(result, key, None) != value:
                        return False
                except (TypeError, ValueError):
                    return False
        return True

    @staticmethod
    def _get_field_value(obj: Any, field_path: List[str]) -> Any:
        """Traverse nested attributes/dicts to extract a field value."""
        current = obj
        for field in field_path:
            if isinstance(current, dict):
                try:
                    current = current[field]
                    continue
                except (KeyError, TypeError):
                    return None
            if hasattr(current, field):
                current = getattr(current, field)
                continue
            return None
        return current

    def _check_nested_field(
        self, obj: Any, field_path: List[str], expected: Any
    ) -> bool:
        actual = self._get_field_value(obj, field_path)
        return actual == expected

    def _check_operator(
        self, obj: Any, field_path: str, operator: str, filter_value: Any
    ) -> bool:
        actual = self._get_field_value(obj, field_path.split("__"))

        # String operators require string operands
        if operator in ("startswith", "endswith", "icontains"):
            if not isinstance(actual, str) or not isinstance(filter_value, str):
                return False
            if operator == "startswith":
                return actual.startswith(filter_value)
            if operator == "endswith":
                return actual.endswith(filter_value)
            # icontains
            return filter_value.lower() in actual.lower()

        # Null check is valid even when actual is None
        if operator == "isnull":
            return (actual is None) == filter_value

        # All other operators return False for None actual values
        if actual is None:
            return False

        if operator == "contains":
            return hasattr(actual, "__contains__") and filter_value in actual
        if operator == "gt":
            return actual > filter_value
        if operator == "gte":
            return actual >= filter_value
        if operator == "lt":
            return actual < filter_value
        if operator == "lte":
            return actual <= filter_value
        if operator == "in":
            return actual in filter_value
        if operator == "exact":
            return actual == filter_value

        logger.warning("Unknown filter operator: %s", operator)
        return False

    def __str__(self) -> str:
        return str(list(self._content.values()))

    def __repr__(self) -> str:
        return f"<ResultSet: {self.count()} items, {len(self._context_types)} content types>"

    def __class_getitem__(cls, item: Any) -> Type["ResultSet"]:
        return cls
