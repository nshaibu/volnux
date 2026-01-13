import json
import os
import typing
from dataclasses import asdict
from datetime import datetime

from pydantic_mini import Attrib, BaseModel, MiniAnnotated
from pydantic_mini.typing import is_builtin_type

from volnux.mixins import BackendIntegrationMixin
from volnux.utils import get_obj_klass_import_str, get_obj_state

from .exceptions import MultiValueError
from .import_utils import import_string

try:
    from typing import TypeAlias  # noqa: F401
except ImportError:
    from typing_extensions import TypeAlias

__all__ = ["EventResult", "ResultSet"]

T = typing.TypeVar("T", bound="ResultSet")

Result: TypeAlias = typing.Hashable  # Placeholder for a Result type


class EventResult(BackendIntegrationMixin, BaseModel):
    error: bool
    event_name: str
    content: typing.Any
    task_id: typing.Optional[str]
    init_params: typing.Optional[typing.Dict[str, typing.Any]]
    call_params: typing.Optional[typing.Dict[str, typing.Any]]
    process_id: MiniAnnotated[int, Attrib(default_factory=lambda: os.getpid())]
    creation_time: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now().timestamp())
    ]

    class Config:
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

    def get_state(self) -> typing.Dict[str, typing.Any]:
        state = self.__dict__.copy()
        init_params: typing.Optional[typing.Dict[str, typing.Any]] = state.pop(
            "init_params", None
        )

        if init_params:
            execution_context = init_params.get("execution_context")
            if execution_context and not isinstance(execution_context, str):
                init_params["execution_context"] = execution_context.id
        else:
            init_params = {"execution_context": {}}

        if self.content is not None:
            content_type = type(self.content)
            if not is_builtin_type(content_type):  # type: ignore
                state["content"] = {
                    "content_type_import_str": get_obj_klass_import_str(self.content),
                    "state": get_obj_state(self.content),
                }
        state["init_params"] = init_params
        return state

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        # TODO handle the init and call params
        init_params = state.pop("init_params", None)
        call_params = state.pop("call_params", None)

        content = state.get("content")
        if isinstance(content, dict) and "content_type_import_str" in content:
            import_str = content["content_type_import_str"]
            content_state = content["state"]
            klass = import_string(import_str)
            instance = klass.__new__(klass)  # type: ignore
            instance.__setstate__(content_state)
            state["content"] = instance

        if call_params:
            pass

        if init_params:
            pass

        self.__dict__.update(state)

    def is_error(self) -> bool:
        return self.error

    def as_dict(self) -> typing.Dict[str, typing.Any]:
        """Serialize event result"""
        content = None
        if isinstance(self.content, Exception):
            content = self.content
            self.content = None
        result_dict = asdict(self)
        if content is not None:
            if hasattr(content, "as_dict"):
                content = content.as_dict()
            elif hasattr(content, "to_dict"):
                content = content.to_dict()
            result_dict["content"] = content
        return result_dict


class EntityContentType:
    """Represents the content type information for an entity."""

    def __init__(
        self,
        backend_import_str: typing.Optional[str] = None,
        entity_content_type: typing.Optional[str] = None,
    ):
        self.backend_import_str = backend_import_str
        self.entity_content_type = entity_content_type

    @classmethod
    def add_entity_content_type(
        cls, entity: Result
    ) -> typing.Optional["EntityContentType"]:
        """Create an EntityContentType from an ObjectIdentityMixin instance."""
        if not entity or not getattr(entity, "id", None):
            return None

        connector = getattr(entity, "_connector", None)
        backend_import_str = None

        if connector:
            backend_import_str = get_obj_klass_import_str(connector)

        return cls(
            backend_import_str=backend_import_str,
            entity_content_type=getattr(entity, "__object_import_str__", None),
        )

    def get_backend(self) -> typing.Any:
        """Import and return the backend class."""
        if not self.backend_import_str:
            raise ValueError("No backend import string specified")
        return import_string(self.backend_import_str)

    def get_content_type(self) -> typing.Any:
        """Import and return the content type class."""
        if not self.entity_content_type:
            raise ValueError("No entity content type specified")
        return import_string(self.entity_content_type)

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, EntityContentType):
            return False
        return (
            self.backend_import_str == other.backend_import_str
            and self.entity_content_type == other.entity_content_type
        )

    def __hash__(self) -> int:
        return hash((self.backend_import_str, self.entity_content_type))

    def __repr__(self) -> str:
        return f"<EntityContentType: backend={self.backend_import_str}, type={self.entity_content_type}>"


class ResultSet(typing.MutableSet[Result]):
    """A collection of Result objects with filtering and query capabilities."""

    # Dictionary of filter operators and their implementation
    _FILTER_OPERATORS: typing.Final[typing.Set[str]] = {
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

    def __init__(self, results: typing.Optional[typing.List[Result]] = None) -> None:
        self._content: typing.Dict[str, Result] = {}
        self._context_types: typing.Set[EntityContentType] = set()

        if results is None:
            results = []

        for result in results:
            self._content[self.get_hash(result)] = result
            self._insert_entity(result)

    @staticmethod
    def get_hash(value: Result) -> str:
        try:
            return f"{hash(value)}"
        except TypeError as e:
            raise TypeError("Result must be hashable") from e

    def __contains__(self, item: Result) -> bool:
        try:
            key = self.get_hash(item)
        except TypeError:
            return False
        return key in self._content

    def __iter__(self) -> typing.Iterator[Result]:
        return iter(self._content.values())

    def __len__(self) -> int:
        return len(self._content)

    def __getitem__(self, index: int) -> Result:
        """Access a result by index."""
        return list(self._content.values())[index]

    def _insert_entity(self, record: Result) -> None:
        """
        Insert an entity and track its content type.

        Args:
            record: The Result object to insert.
        """
        self._content[self.get_hash(record)] = typing.cast(Result, record)
        content_type = EntityContentType.add_entity_content_type(record)
        if content_type and content_type not in self._context_types:
            self._context_types.add(content_type)

    def add(self, value: typing.Union[Result, "ResultSet"]) -> None:
        """
        Add a result or merge another ResultSet.
        Args:
            value: Result or ResultSet to add.
        """
        if isinstance(value, ResultSet):
            self._content.update(value._content)
            self._context_types.update(value._context_types)
        else:
            self._content[self.get_hash(value)] = value
            self._insert_entity(value)

    def extend(self, results: typing.Collection[Result]) -> None:
        """
        Add multiple items to set.
        Args:
            results: Collection of Result objects to add.
        """
        for result in results:
            self.add(result)

    def clear(self) -> None:
        """Remove all results."""
        self._content.clear()
        self._context_types.clear()

    def discard(self, value: typing.Union[Result, "ResultSet"]) -> None:
        """Remove a result or results from another ResultSet."""
        if isinstance(value, ResultSet):
            for res in value:
                self._content.pop(self.get_hash(res), None)
        else:
            self._content.pop(self.get_hash(value), None)

    def copy(self) -> "ResultSet":
        """Create a shallow copy of this ResultSet."""
        new = ResultSet([])
        new._content = self._content.copy()
        new._context_types = self._context_types.copy()
        return new

    def get(self, **filters: typing.Any) -> Result:
        """
        Get a single result matching the filters.
        Raises MultiValueError if more than one result is found.
        """
        qs = self.filter(**filters)
        if len(qs) == 0:
            raise KeyError(f"No result found matching filters: {filters}")
        if len(qs) > 1:
            raise MultiValueError(
                f"More than one result found for filters {filters}: {len(qs)}!=1"
            )
        return qs[0]

    def filter(self, **filter_params: typing.Any) -> "ResultSet":
        """
        Filter results by attribute values with support for nested fields.

        Features:
        - Basic attribute matching (user=x)
        - Nested dictionary lookups (profile__name=y)
        - List/iterable searching (tags__contains=z)
        - Special lookup operators:
            - __contains: Check if value is in a list/iterable
            - __startswith: String starts with value
            - __endswith: String ends with value
            - __icontains: Case-insensitive contains
            - __gt, __gte, __lt, __lte: Comparisons
            - __in: Check if field value is in provided list
            - __exact: Exact matching (default behavior)
            - __isnull: Check if field is None

        Examples:
        - rs.filter(name="Alice") - Basic field matching
        - rs.filter(user__profile__city="New York") - Nested dict lookup
        - rs.filter(tags__contains="urgent") - Check if list contains value
        - rs.filter(name__startswith="A") - String prefix matching
        """
        filtered_results = []

        for result in self._content.values():
            if self._matches_filters(result, filter_params):
                filtered_results.append(result)

        return ResultSet(filtered_results)

    def _matches_filters(
        self, result: Result, filters: typing.Dict[str, typing.Any]
    ) -> bool:
        """
        Check if a result matches all filters, supporting nested lookups and operators.

        Args:
            result: The result object to check
            filters: Dictionary of filter parameters

        Returns:
            True if the result matches all filters, False otherwise
        """
        for key, value in filters.items():
            # Check if this is a special lookup with operator
            if "__" in key:
                parts = key.split("__")
                if parts[-1] in self._FILTER_OPERATORS:
                    field_path_list, operator = parts[:-1], parts[-1]
                    field_path: str = "__".join(field_path_list)
                    if not self._check_operator(result, field_path, operator, value):
                        return False
                else:
                    # This is a nested lookup without operator
                    if not self._check_nested_field(result, key.split("__"), value):
                        return False
            else:
                # Simple field comparison
                try:
                    field_value = getattr(result, key, None)
                    if field_value != value:
                        return False
                except (TypeError, ValueError):
                    return False

        return True

    def _get_field_value(
        self, obj: typing.Any, field_path: typing.List[str]
    ) -> typing.Any:
        """
        Get a value from potentially nested objects.

        Args:
            obj: The object to extract value from
            field_path: List of field names to traverse

        Returns:
            The value at the end of the path or None if not found
        """
        current = obj

        for field in field_path:
            # Handle dictionary access
            if hasattr(current, "__getitem__") and isinstance(current, dict):
                try:
                    current = current[field]
                    continue
                except (KeyError, TypeError):
                    pass

            # Handle object attribute access
            if hasattr(current, field):
                current = getattr(current, field)
                continue

            # Nothing found
            return None

        return current

    def _check_nested_field(
        self, obj: typing.Any, field_path: typing.List[str], expected_value: typing.Any
    ) -> bool:
        """
        Check if a nested field matches the expected value.

        Args:
            obj: The object to check
            field_path: Path to the field
            expected_value: Value to compare against

        Returns:
            True if the field exists and matches the value
        """
        actual_value = self._get_field_value(obj, field_path)
        return actual_value == expected_value  # type: ignore

    def _check_operator(
        self, obj: typing.Any, field_path: str, operator: str, filter_value: typing.Any
    ) -> bool:
        """
        Apply a filter operator to a field.
        Args:
            obj: The object to check
            field_path: Path to the field (as string with __ separators)
            operator: The operator to apply
            filter_value: Value to compare against
        Returns:
            True if the condition is met, False otherwise
        """
        actual_value = self._get_field_value(obj, field_path.split("__"))

        if operator in ("startswith", "endswith", "icontains") and not isinstance(
            actual_value, str
        ):
            return False

        # Handle None case for all operators except isnull
        if actual_value is None and operator != "isnull":
            return False

        # Apply the appropriate operator
        if operator == "contains":
            if hasattr(actual_value, "__contains__"):
                return filter_value in actual_value
            return False

        elif operator == "startswith":
            return isinstance(actual_value, str) and actual_value.startswith(
                filter_value
            )

        elif operator == "endswith":
            return isinstance(actual_value, str) and actual_value.endswith(filter_value)

        elif operator == "icontains":
            if not isinstance(actual_value, str) or not isinstance(filter_value, str):
                return False
            return filter_value.lower() in actual_value.lower()

        elif operator == "gt":
            return actual_value > filter_value

        elif operator == "gte":
            return actual_value >= filter_value

        elif operator == "lt":
            return actual_value < filter_value

        elif operator == "lte":
            return actual_value <= filter_value

        elif operator == "in":
            return actual_value in filter_value

        elif operator == "exact":
            return actual_value == filter_value

        elif operator == "isnull":
            return (actual_value is None) == filter_value

        # Unknown operator
        return False

    def first(self) -> typing.Optional[Result]:
        """Return the first result or None if empty."""
        try:
            return self[0]
        except IndexError:
            return None

    def is_empty(self) -> bool:
        """Return True if the result is empty."""
        return self.first() is None

    def __str__(self) -> str:
        return str(list(self._content.values()))

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {len(self)}>"

    def __class_getitem__(cls, item: typing.Any) -> typing.Type["ResultSet"]:
        return cls
