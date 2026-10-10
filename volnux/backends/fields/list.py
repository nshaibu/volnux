import logging
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Generic,
    TypeVar,
    overload,
    Type,
    Union,
    Iterator,
    TYPE_CHECKING,
)

from formax import MiniAnnotated, Attrib
from volnux.utils import get_obj_klass_import_str
from volnux.import_utils import import_string
from .utils import formax_null_validator

if TYPE_CHECKING:
    from volnux.mixins import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class ListConfig:
    """Configuration for a ListField relationship."""

    max_length: Optional[int] = None
    min_length: int = 0
    unique_items: bool = False
    nullable: bool = True
    item_nullable: bool = False


class ManagedList(Generic[T]):
    """Active, governed collection proxy for ListField relationships.

    Provides add(), remove(), len(), and iteration while maintaining
    serialization governance through the parent model's pre_formatter.

    This is NOT a passive DTO. It is a live collection that tracks
    mutations and ensures type safety at the boundary.
    """

    __slots__ = (
        "_items",
        "_item_model",
        "_parent_instance",
        "_field_name",
        "_dirty",
    )

    def __init__(
        self,
        items: Optional[List[Any]] = None,
        item_model: Optional[Type[T]] = None,
        parent_instance: Any = None,
        field_name: str = "",
    ):
        self._items: List[Any] = list(items) if items else []
        self._item_model = item_model
        self._parent_instance = parent_instance
        self._field_name = field_name
        self._dirty = False

    def add(self, item: T) -> None:
        """Add a typed item to the list. Validates type at insertion."""
        if self._item_model is not None and not isinstance(item, self._item_model):
            raise TypeError(
                f"Cannot add {type(item).__name__} to "
                f"ListField[{self._item_model.__name__}]"
            )
        self._items.append(item)
        self._dirty = True

    def remove(self, item: T) -> bool:
        """Remove the first occurrence of the item. Returns True if found."""
        try:
            self._items.remove(item)
            self._dirty = True
            return True
        except ValueError:
            return False

    def clear(self) -> None:
        """Remove all items."""
        if self._items:
            self._items.clear()
            self._dirty = True

    def extend(self, items: List[T]) -> None:
        """Add multiple typed items. Validates each at insertion."""
        for item in items:
            self.add(item)

    def len(self) -> int:
        """Return the number of items in the list."""
        return len(self._items)

    def contains(self, item: T) -> bool:
        """Check if item exists in the list."""
        return item in self._items

    def is_empty(self) -> bool:
        """Check if the list has no items."""
        return len(self._items) == 0

    def __len__(self) -> int:
        return self.len()

    def __bool__(self) -> bool:
        return not self.is_empty()

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    @overload
    def __getitem__(self, index: int) -> T: ...
    @overload
    def __getitem__(self, index: slice) -> List[T]: ...

    def __getitem__(self, index: Union[int, slice]) -> Union[T, List[T]]:
        return self._items[index]

    def __contains__(self, item: Any) -> bool:
        return self.contains(item)

    def __repr__(self) -> str:
        model_name = self._item_model.__name__ if self._item_model else "Any"
        return f"<ManagedList[{model_name}] len={self.len()}>"

    def to_serializable(self) -> List[Dict[str, str]]:
        """Convert all items to their serializable reference form.

        Called by the ListField pre_formatter during kv_save().
        Handles both live model instances and already-serialized refs.
        """
        result = []
        for item in self._items:
            if isinstance(item, dict):
                # Already serialized (e.g., from deserialization round-trip)
                result.append(item)
            elif hasattr(item, "id") and hasattr(item, "__class__"):
                result.append(
                    {
                        "object_id": str(item.id),
                        "model_path": get_obj_klass_import_str(item),
                        "backend_alias": getattr(item, "_backend_alias", "default"),
                    }
                )
            elif hasattr(item, "object_id") and hasattr(item, "model_path"):
                # ForeignKey-like reference object
                result.append(
                    {
                        "object_id": item.object_id,
                        "model_path": item.model_path,
                        "backend_alias": getattr(item, "backend_alias", "default"),
                    }
                )
            else:
                # Primitive types (str, int, etc.) pass through directly
                result.append(item)
        return result

    @classmethod
    def from_serializable(
        cls,
        raw: Optional[List[Any]],
        item_model: Optional[Type[T]] = None,
        parent_instance: Any = None,
        field_name: str = "",
    ) -> "ManagedList[T]":
        """Reconstruct a ManagedList from serialized data.

        Items remain as raw dicts/primitives until accessed.
        Lazy resolution happens via _resolve_item() on iteration/access.
        """
        instance = cls(
            items=raw or [],
            item_model=item_model,
            parent_instance=parent_instance,
            field_name=field_name,
        )
        return instance


class ListField:
    """Create a MiniAnnotated field definition for a governed, mutable list.

    Returns a ManagedList proxy that supports add(), remove(), len(),
    iteration, and automatic serialization governance.

    Example:
        >>> class WorkflowDef(GovernanceModel):
        ...     tags: ListField[Tag, ListConfig(max_length=50)]
        ...
        >>> wf = WorkflowDef.get("wf-123")
        >>> wf.tags.add(Tag(name="production"))
        >>> wf.tags.len()  # 1
        >>> await wf.kv_save()  # pre_formatter serializes automatically
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError("Cannot subclass ListField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("ListField cannot be instantiated")

    @classmethod
    def _create_result_stream(
        cls, item_model: Type["KeyValueStoreIntegrationMixin"], config: ListConfig
    ):
        from volnux.result import ResultStream

        kwargs: Dict[str, Any] = {"keys": []}
        if config.max_length is not None:
            kwargs["memory_capacity"] = config.max_length
            kwargs["chunk_size"] = config.max_length
        else:
            kwargs["memory_capacity"] = 20
            kwargs["chunk_size"] = 20
        return ResultStream._make(
            model_klass=item_model,
            transaction_id=cls.__name__,
            persisted_backend=None,  # defer backend resolution since it is an async
            **kwargs,
        )

    def __class_getitem__(cls, params) -> MiniAnnotated:
        if not isinstance(params, tuple):
            params = (params, ListConfig())

        if len(params) == 1:
            item_model = params[0]
            config = ListConfig()
        elif len(params) == 2:
            item_model, config = params
            if not isinstance(config, ListConfig):
                raise TypeError(
                    f"Second argument must be ListConfig, got {type(config).__name__}"
                )
        else:
            raise TypeError(
                f"ListField[...] expects 1 or 2 arguments, got {len(params)}"
            )

        validators = []
        if not config.nullable:
            validators.append(formax_null_validator)

        def pre_fmt(instance, value):
            if value is None:
                if not config.nullable:
                    raise ValueError("ListField cannot be None")
                return None

            if isinstance(value, ManagedList):
                items = value.to_serializable()
            elif isinstance(value, (list, tuple)):
                items = list(value)
            else:
                raise TypeError(
                    f"Expected list or ManagedList, got {type(value).__name__}"
                )

            if len(items) < config.min_length:
                raise ValueError(
                    f"List has {len(items)} items, minimum is {config.min_length}"
                )
            if config.max_length is not None and len(items) > config.max_length:
                raise ValueError(
                    f"List has {len(items)} items, maximum is {config.max_length}"
                )

            # Per-item type validation for live model instances
            if item_model is not None:
                resolved_model = item_model
                if hasattr(resolved_model, "__forward_arg__"):
                    from typing import evaluate_forward_ref

                    resolved_model = evaluate_forward_ref(item_model, None, None)

                for i, item in enumerate(items):
                    if item is None:
                        if not config.item_nullable:
                            raise ValueError(f"List item [{i}] cannot be None")
                        continue
                    # Skip validation for already-serialized dicts/primitives
                    if isinstance(item, dict):
                        continue
                    if not isinstance(item, resolved_model):
                        raise TypeError(
                            f"List item [{i}]: expected {resolved_model.__name__}, "
                            f"got {type(item).__name__}"
                        )

            return items

        def post_fmt(instance, value):
            if value is None:
                return None

            # Extract field name from the Attrib metadata context
            field_name = ""
            if hasattr(instance, "__class__"):
                # Find which field this post_formatter belongs to
                for fname, fattr in instance.__class__.__annotations__.items():
                    if (
                        hasattr(fattr, "metadata")
                        and fattr.metadata.get("type") == "listfield"
                    ):
                        # Match by checking if this formatter is the one bound
                        field_name = fname
                        break

            return ManagedList.from_serializable(
                raw=value,
                item_model=item_model,
                parent_instance=instance,
                field_name=field_name,
            )

        return MiniAnnotated[
            Any,
            Attrib(
                pre_formatter=pre_fmt,
                post_formatter=post_fmt,
                validators=validators,
                metadata={
                    "type": "list",
                    "item_model": item_model,
                    "max_length": config.max_length,
                    "min_length": config.min_length,
                    "unique_items": config.unique_items,
                    "nullable": config.nullable,
                    "item_nullable": config.item_nullable,
                },
            ),
        ]
