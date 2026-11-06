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
    ForwardRef,
)

from formax import MiniAnnotated, Attrib
from formax.typing import MISSING
from volnux.utils import get_obj_klass_import_str
from volnux.import_utils import import_string
from volnux.backends.db_utils import ModelResolverMixin
from .foreign_key import ForeignKey
from .utils import UNSET, formax_null_validator, unwrap_optional

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
    # Value used when none is given. By default a field gets a fresh empty list;
    # ``ListField[Optional[T]]`` defaults to None. ``default_factory`` builds
    # the default (a list, or None) for each model.
    default: Any = UNSET
    default_factory: Any = UNSET


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
        # Wrap a stored list by reference so mutations reach the parent's
        # underlying field value instead of a throwaway copy.
        if isinstance(items, list):
            self._items: List[Any] = items
        else:
            self._items = list(items) if items else []
        self._item_model = item_model
        self._parent_instance = parent_instance
        self._field_name = field_name
        self._dirty = False

    def _resolved_item_model(self) -> Optional[Type[Any]]:
        """Resolve the item model, returning None if it is an unresolvable ForwardRef."""
        return _try_resolve_model(self._item_model)

    def add(self, item: T) -> None:
        """Add a typed item to the list. Validates type at insertion."""
        model = self._resolved_item_model()
        if (
            model is not None
            and item is not None
            and not isinstance(item, (model, ForeignKey))
        ):
            raise TypeError(
                f"Cannot add {type(item).__name__} to " f"ListField[{model.__name__}]"
            )
        self._items.append(item)
        self._dirty = True

    # list-compatible alias
    append = add

    def remove(self, item: T) -> bool:
        """Remove the first occurrence of the item. Returns True if found."""
        for i in range(len(self._items)):
            if self._resolve_item(i) == item:
                del self._items[i]
                self._dirty = True
                return True
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
        return any(self._resolve_item(i) == item for i in range(len(self._items)))

    def is_empty(self) -> bool:
        """Check if the list has no items."""
        return len(self._items) == 0

    def __len__(self) -> int:
        return self.len()

    def __bool__(self) -> bool:
        return not self.is_empty()

    @property
    def is_dirty(self) -> bool:
        """Whether the list has been mutated since it was loaded."""
        return self._dirty

    def _resolve_item(self, index: int) -> Any:
        """Lazily convert a serialized reference dict into a ForeignKey.

        The converted reference is cached back into the list so repeated
        access returns the same object.
        """
        item = self._items[index]
        if isinstance(item, dict) and "object_id" in item and "model_path" in item:
            item = ForeignKey(
                object_id=item["object_id"],
                model_path=item["model_path"],
                backend_alias=item.get("backend_alias", "default"),
            )
            self._items[index] = item
        return item

    def __iter__(self) -> Iterator[T]:
        for i in range(len(self._items)):
            yield self._resolve_item(i)

    @overload
    def __getitem__(self, index: int) -> T: ...
    @overload
    def __getitem__(self, index: slice) -> List[T]: ...

    def __getitem__(self, index: Union[int, slice]) -> Union[T, List[T]]:
        if isinstance(index, slice):
            return [self._resolve_item(i) for i in range(*index.indices(len(self)))]
        if index < 0:
            index += len(self._items)
        return self._resolve_item(index)

    def __contains__(self, item: Any) -> bool:
        return self.contains(item)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ManagedList):
            other = list(other)
        if isinstance(other, (list, tuple)):
            return list(self) == list(other)
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        model = self._item_model
        if isinstance(model, ForwardRef):
            model_name = model.__forward_arg__
        else:
            model_name = getattr(model, "__name__", "Any")
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
            elif isinstance(item, ForeignKey) or (
                hasattr(item, "object_id") and hasattr(item, "model_path")
            ):
                # ForeignKey-like reference object. Checked before the generic
                # ``id`` branch since ForeignKey also exposes ``id``.
                result.append(
                    {
                        "object_id": item.object_id,
                        "model_path": item.model_path,
                        "backend_alias": getattr(item, "backend_alias", "default"),
                    }
                )
            elif hasattr(item, "id"):
                result.append(
                    {
                        "object_id": str(item.id),
                        "model_path": get_obj_klass_import_str(item),
                        "backend_alias": getattr(item, "_backend_alias", "default"),
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

        Items remain as raw dicts/primitives until accessed. Reference dicts
        are lazily converted to ForeignKey objects via _resolve_item() on
        iteration/access; call ``await ref.resolve_async()`` to load them.
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

    The field holds a ``ManagedList``: it supports add(), remove(), len() and
    iteration, keeps live model instances live, and is turned into reference
    payloads only when the model's state is persisted. A list loaded from
    storage yields lazy ``ForeignKey`` references for its model items.

    A field without a value gets a fresh empty list each time;
    ``ListField[Optional[T]]`` (or ``ListConfig(default=None)``) defaults to None.

    Example:
        >>> class WorkflowDef(GovernanceModel):
        ...     tags: ListField[Tag, ListConfig(max_length=50)]
        ...
        >>> wf = WorkflowDef.get("wf-123")
        >>> wf.tags.add(Tag(name="production"))
        >>> wf.tags.len()  # 1
        >>> await wf.save()
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError("Cannot subclass ListField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("ListField cannot be instantiated")

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

        if config.min_length < 0:
            raise ValueError("min_length cannot be negative")
        if config.max_length is not None and config.max_length < config.min_length:
            raise ValueError("max_length cannot be less than min_length")

        item_model, optional = unwrap_optional(item_model)
        nullable = config.nullable or optional

        if isinstance(item_model, str):
            try:
                module_path, class_name = item_model.rsplit(".", 1)
            except ValueError:
                module_path, class_name = None, item_model
            item_model = ForwardRef(class_name, module=module_path, is_class=True)

        default, default_factory = config.default, config.default_factory
        if default is UNSET and default_factory is UNSET:
            if optional:
                default = None
            elif config.min_length == 0:
                default_factory = list
        default = MISSING if default is UNSET else default
        default_factory = MISSING if default_factory is UNSET else default_factory

        validators = []
        if not nullable:
            validators.append(formax_null_validator)

        # Resolved lazily (the Attrib isn't bound to a field yet)
        field_name_cache: Dict[str, str] = {}

        def field_name_of(instance) -> str:
            if "name" not in field_name_cache and instance is not None:
                field_name_cache["name"] = _find_field_name(type(instance), attrib)
            return field_name_cache.get("name", "")

        def pre_fmt(instance, value):
            if value is None:
                if not nullable:
                    raise ValueError("ListField cannot be None")
                return None

            if isinstance(value, ManagedList):
                items = list(value._items)
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

            # Per-item validation. Type checks are skipped while a forward
            # reference to the item model is not yet resolvable.
            resolved_model = _try_resolve_model(item_model)
            for i, item in enumerate(items):
                if item is None:
                    if not config.item_nullable:
                        raise ValueError(f"List item [{i}] cannot be None")
                    continue
                # Skip validation for already-serialized dicts and references
                if resolved_model is None or isinstance(item, (dict, ForeignKey)):
                    continue
                if not isinstance(item, resolved_model):
                    raise TypeError(
                        f"List item [{i}]: expected {resolved_model.__name__}, "
                        f"got {type(item).__name__}"
                    )

            if config.unique_items:
                seen = set()
                for i, item in enumerate(items):
                    key = _identity_key(item)
                    if key in seen:
                        raise ValueError(f"List item [{i}] is a duplicate")
                    seen.add(key)

            if isinstance(value, ManagedList):
                return value
            return ManagedList(
                items=items,
                item_model=item_model,
                parent_instance=instance,
                field_name=field_name_of(instance),
            )

        def to_state(instance, value):
            if value is None:
                return None
            if not isinstance(value, ManagedList):
                value = ManagedList(items=list(value))
            return value.to_serializable()

        def from_state(instance, raw):
            if raw is None:
                return None
            return ManagedList.from_serializable(
                raw=raw,
                item_model=item_model,
                parent_instance=instance,
                field_name=field_name_of(instance),
            )

        attrib = Attrib(
            pre_formatter=pre_fmt,
            validators=validators,
            default=default,
            default_factory=default_factory,
            metadata={
                "type": "list",
                "item_model": item_model,
                "max_length": config.max_length,
                "min_length": config.min_length,
                "unique_items": config.unique_items,
                "nullable": nullable,
                "item_nullable": config.item_nullable,
                "to_state": to_state,
                "from_state": from_state,
            },
        )
        return MiniAnnotated[Any, attrib]


def _try_resolve_model(model: Any) -> Optional[Type[Any]]:
    """Resolve an item model, returning None when it is missing or not yet resolvable."""
    if model is None:
        return None
    try:
        resolved = ModelResolverMixin._resolve_target_model(model)
    except (NameError, ImportError, AttributeError, TypeError):
        return None
    return resolved if isinstance(resolved, type) else None


def _identity_key(item: Any) -> Any:
    """Key used for unique_items checks: model references compare by id."""
    if isinstance(item, dict) and "object_id" in item:
        return ("ref", item.get("model_path"), str(item["object_id"]))
    if isinstance(item, ForeignKey):
        return ("ref", item.model_path, str(item.object_id))
    if hasattr(item, "id") and not isinstance(item, (str, int, float, bool)):
        return ("ref", get_obj_klass_import_str(item), str(item.id))
    try:
        hash(item)
        return ("val", item)
    except TypeError:
        return ("val", repr(item))


def _find_field_name(model_class: Type[Any], attrib: Attrib) -> str:
    """Find which annotated field on ``model_class`` is bound to ``attrib``."""
    for klass in model_class.__mro__:
        for fname, annotation in getattr(klass, "__annotations__", {}).items():
            if any(meta is attrib for meta in getattr(annotation, "__metadata__", ())):
                return fname
    return ""
