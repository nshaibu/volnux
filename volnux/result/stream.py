import logging
import math
import typing
import asyncio
import itertools
import warnings
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple, AsyncIterator, Set, Type

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.backends.messaging.util import _BLOCKING_EXECUTOR as _STREAM_EXECUTOR
from volnux.backends.q_compiler import (
    create_q_predicate,
    create_filter_predicate,
    resolve_field_value,
)
from volnux.utils import hydrate_model

try:
    from typing import TypeAlias  # noqa: F401
except ImportError:
    from typing_extensions import TypeAlias

__all__ = ["ResultStream", "Q"]

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="KeyValueStoreIntegrationMixin")

Result: TypeAlias = typing.Hashable  # Placeholder for a Result type


class _ReverseComparable:
    """Wrapper that inverts comparison operators for descending sort."""

    __slots__ = ("_value",)

    def __init__(self, value: Any) -> None:
        self._value = value

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, _ReverseComparable):
            return self._value > other._value
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if isinstance(other, _ReverseComparable):
            return self._value >= other._value
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, _ReverseComparable):
            return self._value < other._value
        return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, _ReverseComparable):
            return self._value <= other._value
        return NotImplemented

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, _ReverseComparable):
            return self._value == other._value
        return NotImplemented


@dataclass(frozen=True)
class Q:
    """Enhanced Q object supporting backend-native filter creation."""

    children: typing.List[typing.Union["Q", tuple]] = field(default_factory=list)
    connector: str = "AND"
    negated: bool = False

    def __init__(self, *args, _connector="AND", _negated=False, **kwargs):
        # Hack for frozen dataclass
        object.__setattr__(self, "children", list(args) + list(kwargs.items()))
        object.__setattr__(self, "connector", _connector)
        object.__setattr__(self, "negated", _negated)

    def __and__(self, other: "Q") -> "Q":
        return Q(self, other, _connector="AND")

    def __or__(self, other: "Q") -> "Q":
        return Q(self, other, _connector="OR")

    def __invert__(self) -> "Q":
        return Q(*self.children, _connector=self.connector, _negated=not self.negated)

    def to_predicate(self) -> typing.Callable[[typing.Any], bool]:
        """Convert a Q object to a predicate function using backend's operators."""
        return create_q_predicate([self])

    def to_filter_kwargs(self) -> typing.Dict[str, typing.Any]:
        """Attempt to convert simple Q objects back to filter kwargs.

        Returns:
            Dictionary suitable for backend's **filter_kwargs, or raises ValueError
            if the Q object is too complex.
        """
        if self.negated or self.connector == "OR" or len(self.children) > 1:
            raise ValueError(
                "Complex Q objects cannot be converted to simple filter kwargs"
            )

        child = self.children[0]
        if isinstance(child, Q):
            raise ValueError(
                "Nested Q objects cannot be converted to simple filter kwargs"
            )

        return {child[0]: child[1]}


@dataclass
class ResultStream(typing.Generic[T]):
    """
    Hybrid lazy-loading result stream with a transient in-memory tier and an optional
    persisted tier.

    The `ResultStream` class facilitates working with large datasets by leveraging a
    hybrid storage model, providing lazy-loading mechanisms with an in-memory layer
    and an optional persisted backend. It supports filtering, partitioning (sharding),
    and iterative processing while keeping memory usage efficient.

    Each object in the stream is identified using a key, and predicates can be registered
    to apply filters lazily during iteration. The class ensures that newly added objects
    can be persisted and tracked effectively.

    :ivar model_klass: The model class associated with the result stream.
    :ivar transaction_id: Identifier representing the unique transactional scope
        for the stream.
    :ivar chunk_size: The size of the data chunks to be retrieved during iteration,
        defaults to 100.
    :ivar memory_backend: Optional in-memory storage backend for the stream.
    :type memory_backend: typing.Optional[KeyValueStoreBackendBase]
    :ivar persisted_backend: Optional persisted storage backend for the stream.
    :type persisted_backend: typing.Optional[KeyValueStoreBackendBase]
    """

    model_klass: typing.Type[T]
    transaction_id: str
    chunk_size: int = 100
    memory_capacity: int = 100
    memory_backend: typing.Optional[KeyValueStoreBackendBase] = field(
        default=None, repr=False
    )
    persisted_backend: typing.Optional[KeyValueStoreBackendBase] = field(
        default=None, repr=False
    )

    # Internal state
    _keys: typing.List[str] = field(default_factory=list, repr=False)
    _key_set: set = field(default_factory=set, repr=False)
    _join_fields: typing.List[str] = field(default_factory=list, repr=False)
    _predicates: typing.List[typing.Callable[[T], bool]] = field(
        default_factory=list, repr=False
    )
    _q_objects: typing.List[Q] = field(default_factory=list, repr=False)
    _access_order: OrderedDict[str, None] = field(
        default_factory=OrderedDict, repr=False, init=False
    )

    def __post_init__(self) -> None:
        if self.chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {self.chunk_size}")

        if self.memory_capacity > 0:
            self.memory_backend = InMemoryKeyValueStoreBackend(
                namespace_prefix=self.transaction_id
            )
        # memory_capacity == 0 → no memory backend, no caching

    async def _cache_get(self, schema: str, record_id: str) -> Optional[T]:
        """Get from memory backend + update LRU access order."""
        if self.memory_backend is None:
            return None

        instance = typing.cast(
            T,
            await self._run_sync(
                self.memory_backend.get, schema, record_id, self.model_klass
            ),
        )
        # Move to end (most recently used)
        self._access_order.move_to_end(record_id)
        return instance

    async def _cache_put(self, schema: str, record_id: str, instance: T) -> None:
        """Put into memory backend with LRU eviction when at capacity."""
        if self.memory_backend is None or self.memory_capacity <= 0:
            return

        # Evict LRU entries if at capacity, and this is a new key
        if (
            record_id not in self._access_order
            and len(self._access_order) >= self.memory_capacity
        ):
            evict_key, _ = self._access_order.popitem(last=False)
            try:
                await self._run_sync(self.memory_backend.delete, schema, evict_key)
            except Exception:
                pass

        await self._run_sync(self.memory_backend.upsert, schema, record_id, instance)
        self._access_order[record_id] = None
        self._access_order.move_to_end(record_id)

    async def _cache_delete(self, schema: str, record_id: str) -> None:
        if self.memory_backend is None:
            return
        await self._run_sync(self.memory_backend.delete, schema, record_id)
        self._access_order.pop(record_id, None)

    async def _cache_clear(self, schema: str) -> None:
        if self.memory_backend is None:
            return
        await self._run_sync(self.memory_backend.clear_schema, schema)
        self._access_order.clear()

    @classmethod
    async def _run_sync(
        cls, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Offload a sync backend method to the stream executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            _STREAM_EXECUTOR, lambda: func(*args, **kwargs)
        )

    async def _ensure_persisted_backend(self) -> Optional["KeyValueStoreBackendBase"]:
        """Lazily initialize persisted backend via async get_backend()."""
        if self.persisted_backend is None:
            try:
                self.persisted_backend = await self.model_klass.get_backend()
            except Exception as e:
                logger.debug(
                    "Could not init persisted backend for %s: %s",
                    self.model_klass.__name__,
                    e,
                )
        return self.persisted_backend

    async def _schema_name(self) -> str:
        """Async schema name resolution (calls await get_backend() internally)."""
        if self.persisted_backend:
            return self.persisted_backend.resolve_physical_target(
                self.model_klass.get_storage_route()
            )
        return await self.model_klass.get_schema_name()

    def no_cache(self) -> "ResultStream[T]":
        """Return a new stream with caching disabled."""
        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates,
            chunk_size=self.chunk_size,
            memory_capacity=0,
            persisted_backend=self.persisted_backend,
            q_predicates=self._q_objects,
            join_fields=self._join_fields,
        )

    @classmethod
    def _make(
        cls,
        model_klass: Type[T],
        transaction_id: str,
        keys: List[str],
        predicates: List[Callable[[T], bool]],
        chunk_size: int,
        memory_capacity: int,
        persisted_backend: Optional["KeyValueStoreBackendBase"],
        q_predicates: Optional[List["Q"]] = None,
        join_fields: Optional[List[str]] = None,
    ) -> "ResultStream[T]":
        stream: ResultStream[T] = cls.__new__(cls)
        stream.model_klass = model_klass
        stream.transaction_id = transaction_id
        stream.chunk_size = chunk_size
        stream.memory_capacity = memory_capacity
        stream.persisted_backend = persisted_backend
        stream._keys = list(keys)
        stream._key_set = set(keys)
        stream._predicates = list(predicates)
        stream._q_objects = list(q_predicates or [])
        stream._join_fields = list(join_fields or [])
        stream.memory_backend = (
            InMemoryKeyValueStoreBackend(namespace_prefix=transaction_id)
            if memory_capacity > 0
            else None
        )
        stream._access_order = OrderedDict()
        return stream

    async def _memory_get_or_none(self, record_id: str) -> Optional[T]:
        try:
            schema = await self._schema_name()
            return await self._cache_get(schema, record_id)
        except Exception:
            return None

    async def _persisted_get_or_none(self, record_id: str) -> Optional[T]:
        backend = await self._ensure_persisted_backend()
        if backend is None:
            return None
        try:
            schema = await self._schema_name()
            return typing.cast(
                T,
                await self._run_sync(backend.get, schema, record_id, self.model_klass),
            )
        except Exception:
            return None

    async def add(self, instance: T, persist: Optional[bool] = None) -> None:
        """
        Adds an instance to the cache and optionally persists it to a backend.

        This method adds a given instance to a cache, keyed by its ID. Depending
        on the provided persist flag or the instance's `is_persisted` attribute,
        the instance may also be persisted to a backend. If the instance is new
        to the key set, it will be added to internal tracking structures.

        :param instance: The instance to be added to the cache.
        :param persist: Optional boolean flag indicating whether the instance
            should be persisted. If None, the instance's `is_persisted` attribute
            will be used to determine persistence behavior.
        :return: None
        """
        key = instance.id
        schema = await self._schema_name()

        await self._cache_put(schema, key, instance)

        should_persist = (
            persist if persist is not None else getattr(instance, "is_persisted", False)
        )
        if should_persist:
            backend = await self._ensure_persisted_backend()
            if backend is not None:
                await self._run_sync(backend.upsert, schema, key, instance)
                if hasattr(instance, "is_persisted"):
                    instance.is_persisted = True

        if key not in self._key_set:
            self._keys.append(key)
            self._key_set.add(key)

    async def persist(self, instance: T) -> None:
        """
        Persist an instance into the backend storage after ensuring that the backend is
        properly initialized and persisted. This includes upserting the instance data
        into the backend, updating the cache, and tracking the instance's key. If the
        instance has an attribute `is_persisted`, it will be updated to `True`.

        :param instance: The instance to be persisted into the storage.
        :type instance: T
        :return: None
        :rtype: None
        :raises RuntimeError: If no persisted backend is initialized for the model class.
        """
        backend = await self._ensure_persisted_backend()
        if backend is None:
            raise RuntimeError(f"No persisted backend for {self.model_klass.__name__}")

        schema = await self._schema_name()
        await self._run_sync(backend.upsert, schema, instance.id, instance)
        await self._cache_put(schema, instance.id, instance)

        if hasattr(instance, "is_persisted"):
            instance.is_persisted = True
        if instance.id not in self._key_set:
            self._keys.append(instance.id)
            self._key_set.add(instance.id)

    def filter(self, **filter_kwargs) -> "ResultStream[T]":
        """
        Registers a filter predicate to be applied lazily during iteration.
        Now supports Django-style lookups.

        Examples:
            >>> stream.filter(age__gt=25, name__icontains="john")
            >>> stream.filter(status__in=["active", "pending"])
            >>> stream.filter(created_at__gte=datetime(2023, 1, 1))
        """
        new_predicate: typing.Callable[[T], bool] = create_filter_predicate(
            **filter_kwargs
        )

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates + [new_predicate],
            chunk_size=self.chunk_size,
            memory_capacity=10,
            persisted_backend=self.persisted_backend,
            q_predicates=self._q_objects,
            join_fields=self._join_fields,
        )

    def where(self, predicate: typing.Callable[[T], bool]) -> "ResultStream[T]":
        """
        Lower-level alternative to filter(): register any callable predicate
        directly without going through a backend.
        """
        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates + [predicate],
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            q_predicates=self._q_objects,
            join_fields=self._join_fields,
        )

    def shard(self, num_shards: int) -> typing.Generator["ResultStream[T]", None, None]:
        """
        Partitions the ID list into N sub-streams for parallel processing.
        """
        if num_shards <= 1 or not self._keys:
            yield self
            return

        total = len(self._keys)
        shard_size = math.ceil(total / num_shards)
        actual_shards = math.ceil(total / shard_size)

        if actual_shards < num_shards:
            logger.warning(
                "Requested %d shards but only %d keys available; "
                "returning %d shard(s).",
                num_shards,
                total,
                actual_shards,
            )

        for i in range(0, total, shard_size):
            yield ResultStream._make(
                model_klass=self.model_klass,
                transaction_id=self.transaction_id,
                keys=self._keys[i : i + shard_size],
                predicates=self._predicates,
                chunk_size=self.chunk_size,
                memory_capacity=self.memory_capacity,
                persisted_backend=self.persisted_backend,
                q_predicates=self._q_objects,
                join_fields=self._join_fields,
            )

    def exists(self):
        """
        Checks whether the object contains any elements or not.

        This method evaluates if the current object has a non-zero
        size or length, indicating the existence of elements.

        :return: True if the object contains one or more elements,
            False otherwise.
        :rtype: bool
        """
        return len(self) > 0

    async def first(self) -> Optional[T]:
        """
        Iterates over an asynchronous iterable and returns the first item if available. If the
        iterable is empty, returns None.

        :return: The first item of the asynchronous iterable if available, otherwise None.
        :rtype: Optional[T]
        """
        async for obj in self:
            return obj
        return None

    async def has_results(self) -> bool:
        """
        Determines whether there are any results available by evaluating the first result.

        This asynchronous method checks if the first result exists, and returns a boolean
        indicating the presence of results.

        :return: Boolean indicating whether any results exist.
        :rtype: bool
        """
        return (await self.first()) is not None

    def __len__(self) -> int:
        """Returns the number of tracked IDs before filtering."""
        return len(self._keys)

    async def _fetch_batch(self, batch_ids: List[str]) -> List[T]:
        """Resolve a batch: memory-first, persisted fallback, cache on hit."""
        results: List[T] = []
        schema = await self._schema_name()

        for rid in batch_ids:
            instance = await self._memory_get_or_none(rid)

            if instance is None:
                instance = await self._persisted_get_or_none(rid)
                if instance is not None:
                    try:
                        await self._cache_put(schema, rid, instance)
                    except Exception:
                        pass

            if instance is not None:
                results.append(instance)

        return results

    async def evict_memory(self, record_id: Optional[str] = None) -> None:
        """Evict one or all in-memory copies without touching durable storage."""
        schema = await self._schema_name()

        if record_id is not None:
            try:
                await self._cache_delete(schema, record_id)
            except Exception as e:
                logger.debug("Failed to evict %s: %s", record_id, e)
        else:
            for rid in self._keys:
                try:
                    await self._cache_delete(schema, rid)
                except Exception as e:
                    logger.debug("Failed to evict %s: %s", rid, e)

    def q_filter(self, q_object: Q) -> "ResultStream[T]":
        """
        Filter using a Q object for complex boolean logic.

        Examples:
            stream.q_filter(Q(age__gt=25) | Q(status="vip"))
            stream.q_filter(~Q(is_deleted=True) & Q(active=True))
        """

        new_predicate = q_object.to_predicate()

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates + [new_predicate],
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            q_predicates=self._q_objects + [q_object],
            join_fields=self._join_fields,
        )

    async def resolve_joins(self, *field_names: str) -> "ResultStream[T]":
        """Request FK resolution at the backend level.

        If the backend supports join pushdown, FK fields are resolved
        in a single query. Otherwise, falls back to deferred resolution.

        Usage:
            workflows = await Workflow.filter(status="active").resolve_joins("created_by", "team")
            async for wf in workflows:
                print(wf.created_by.name)  # Already resolved; no additional query
        """
        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates,
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            q_predicates=self._q_objects,
            join_fields=list(field_names),
        )

    async def order_by(self, *fields: str) -> "ResultStream[T]":
        """Order results by given fields. Prefix with '-' for descending.

        Forces full evaluation to determine sort order. Returns a new stream
        with sorted keys and cleared predicates (pre-filtered).

        Examples:
            await stream.order_by('-age', 'name')
            await stream.filter(status="active").order_by('created_at')
        """
        if not fields:
            return self

        # Parse field specs once before iteration
        sort_specs: List[Tuple[str, bool]] = []  # (field_path, descending)
        for fd in fields:
            descending = fd.startswith("-")
            field_path = fd.lstrip("-")
            sort_specs.append((field_path, descending))

        results_with_keys: List[Tuple[Tuple[Any, ...], str]] = []
        async for obj in self:
            sort_key_parts: List[Any] = []
            for field_path, descending in sort_specs:
                value, found = resolve_field_value(obj, field_path)

                # Sort tuple: (is_none, is_descending, value)
                # - None always sorts last regardless of direction
                # - is_descending inverts comparison via negation wrapper
                if not found or value is None:
                    sort_key_parts.append((True, False, ""))
                else:
                    sort_key_parts.append((False, descending, value))

            results_with_keys.append((tuple(sort_key_parts), obj.id))

        # Sort with proper descending support per field
        def sort_key(entry: Tuple[Tuple[Any, ...], str]) -> Tuple[Any, ...]:
            parts = entry[0]
            result = []
            for is_none, descending, value in parts:
                if is_none:
                    # None sorts last: use max possible sentinel
                    result.append((1,))
                elif descending:
                    # For descending, we invert comparable values.
                    # Wrap in a reverse-comparable container.
                    result.append((0, _ReverseComparable(value)))
                else:
                    result.append((0, value))
            return tuple(result)

        results_with_keys.sort(key=sort_key)
        sorted_keys = [rid for _, rid in results_with_keys]

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=sorted_keys,
            predicates=[],  # Pre-filtered during materialization
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            q_predicates=[],  # Pre-filtered during materialization
            join_fields=self._join_fields,
        )

    async def count(self) -> int:
        """
        Counts the number of items in the stream. This method is more computationally
        expensive than directly using the length of the stream (e.g., len(stream)),
        because it queries the underlying backend.

        :return: The total number of items in the stream.
        :rtype: int
        """
        n = 0
        async for _ in self:
            n += 1
        return n

    async def to_list(self) -> List[T]:
        return [obj async for obj in self]

    async def paginate(
        self, page: int = 1, page_size: int = 20
    ) -> Tuple["ResultStream[T]", int]:
        """
        Paginate through items in the result stream with the given page number and page size. The method
        returns a tuple consisting of a paginated result stream and the total number of items.

        :param page: The page number to fetch. Defaults to 1.
        :param page_size: The number of items to include per page. Defaults to 20.
        :return: A tuple containing the paginated ResultStream object and the total number of items.
        """
        total = len(self)
        offset = (page - 1) * page_size
        paginated_keys: List[str] = []
        idx = 0
        async for obj in self:
            if idx >= offset and len(paginated_keys) < page_size:
                paginated_keys.append(obj.id)
            elif len(paginated_keys) >= page_size:
                break
            idx += 1

        return (
            ResultStream._make(
                model_klass=self.model_klass,
                transaction_id=self.transaction_id,
                keys=paginated_keys,
                predicates=[],
                chunk_size=self.chunk_size,
                memory_capacity=self.memory_capacity,
                persisted_backend=self.persisted_backend,
                join_fields=self._join_fields,
            ),
            total,
        )

    async def cache_results(self) -> "ResultStream[T]":
        """
        Caches the results of the current result stream into a new `ResultStream` instance,
        based on the provided attributes of the current object. This method iterates through
        the current stream to ensure that all the results are processed before creating
        a cached version of the result stream.

        :return: A new `ResultStream` instance with cached results.
        :rtype: ResultStream[T]
        """
        # Evaluate entire stream
        async for obj in self:
            # Already being cached via _memory_get_or_none
            pass
        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates,
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            join_fields=self._join_fields,
        )

    async def bulk_update(self, **kwargs: Any) -> int:
        """
        Performs a bulk update operation on objects in the collection.

        This method iterates over the objects in the collection, applies the provided
        updates by modifying the attributes based on the given keyword arguments, and
        then synchronizes the changes with associated backends. If an object lacks an
        attribute specified in the kwargs, a warning is emitted instead of raising an
        exception.

        :param kwargs: Key-value pairs representing the attributes to update and their
                       respective new values.
        :type kwargs: Any
        :return: The number of objects successfully updated.
        :rtype: int
        """
        schema = await self._schema_name()
        updated = 0

        async for obj in self:
            for key, value in kwargs.items():
                if hasattr(obj, key):
                    setattr(obj, key, value)
                else:
                    warnings.warn(f"{obj} lacks attribute '{key}'", UserWarning)

            await self._run_sync(self.memory_backend.upsert, schema, obj.id, obj)
            backend = await self._ensure_persisted_backend()
            if backend is not None:
                await self._run_sync(backend.upsert, schema, obj.id, obj)
            updated += 1
        return updated

    async def bulk_delete(self) -> int:
        """
        Deletes all objects in the collection and rebuilds the internal key references safely.

        This method collects all objects in the collection and attempts to delete each object
        from both memory and persistent backends. Upon successful deletion, the corresponding
        key is removed from the internal key set. The internal key list is rebuilt at the end
        to ensure consistency with the key set.

        :async: This method is asynchronous and must be awaited.

        :return: The total count of objects successfully deleted.
        :rtype: int
        """
        schema = await self._schema_name()
        deleted = 0

        async for obj in self:
            was_deleted = False
            try:
                await self._cache_delete(schema, obj.id)
                was_deleted = True
            except Exception:
                pass

            backend = await self._ensure_persisted_backend()
            if backend is not None:
                try:
                    await self._run_sync(backend.delete, schema, obj.id)
                    was_deleted = True
                except Exception:
                    pass

            if was_deleted:
                self._key_set.discard(obj.id)
                deleted += 1

        self._keys = [k for k in self._keys if k in self._key_set]
        return deleted

    def union(self, *streams: "ResultStream[T]") -> "ResultStream[T]":
        """Combine multiple streams, preserving order and removing duplicates."""
        combined_keys = set(self._keys)

        for stream in streams:
            combined_keys = combined_keys.union(set(stream._keys))

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=list(combined_keys),
            predicates=[],  # Reset predicates for combined stream
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            join_fields=self._join_fields,
        )

    def explain(self) -> dict:
        """Return the query plan without executing."""
        return {
            "model": self.model_klass.__name__,
            "transaction": self.transaction_id,
            "total_keys": len(self._keys),
            "predicates": len(self._predicates),
            "chunk_size": self.chunk_size,
            "has_memory_backend": self.memory_backend is not None,
            "has_persisted_backend": self.persisted_backend is not None,
            "estimated_memory": len(self._keys) * 8,  # Rough estimate
        }

    def __aiter__(self) -> AsyncIterator[T]:
        return self._async_iterate()

    async def optimize(self) -> "ResultStream[T]":
        """
        Optimize the result stream by pushing simple Q predicates to the backend for
        server-side filtering and returning a new stream with a reduced key set. This
        method ensures that the original stream remains unchanged, applying AND
        semantics to multiple Q objects. Any complex Q objects that cannot be
        converted remain as in-memory predicates.

        :return: A new instance of the ResultStream with optimized filtering applied.
        :rtype: ResultStream[T]
        """
        if not self._q_objects or self.persisted_backend is None:
            return self

        schema = await self._schema_name()

        current_keys: Optional[Set[str]] = None
        remaining_q_objects: List["Q"] = []
        any_optimized = False

        for q in self._q_objects:
            try:
                filter_kwargs = q.to_filter_kwargs()

                backend_results: "ResultStream[T]" = await self._run_sync(
                    self.persisted_backend.filter,
                    schema,
                    self.model_klass,
                    **filter_kwargs,
                )

                matched_ids = set(backend_results._keys)

                if current_keys is None:
                    # First successful push: intersect with original key set
                    current_keys = matched_ids.intersection(self._key_set)
                else:
                    # Subsequent pushes: intersect with accumulated result (AND)
                    current_keys = current_keys.intersection(matched_ids)

                any_optimized = True

            except (ValueError, NotImplementedError, AttributeError):
                remaining_q_objects.append(q)

        if not any_optimized:
            # No Q objects could be pushed — nothing changed
            return self

        # Preserve original key ordering for deterministic chunking/pagination
        if current_keys is not None:
            optimized_key_list = [k for k in self._keys if k in current_keys]
        else:
            optimized_key_list = self._keys

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=optimized_key_list,
            predicates=self._predicates,
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            q_predicates=remaining_q_objects,
            join_fields=self._join_fields,
        )

    async def _async_iterate(self) -> AsyncIterator[T]:
        if self._q_objects and self.persisted_backend is not None:
            optimized = await self.optimize()
            # Iterate over the optimized stream's keys instead
            keys = optimized._keys
            predicates = optimized._predicates
        else:
            keys = self._keys
            predicates = self._predicates

        # Attempt join pushdown if requested and backend supports it
        if (
            self._join_fields
            and self.persisted_backend is not None
            and hasattr(self.persisted_backend, "filter_with_joins")
        ):
            try:
                schema = await self._schema_name()
                joined_results = await self.persisted_backend.filter_with_joins(
                    schema, self.model_klass, self._join_fields
                )

                for record_dict in joined_results:
                    instance = hydrate_model(self.model_klass, record_dict)
                    if all(pred(instance) for pred in predicates):
                        yield instance
                return
            except NotImplementedError:
                pass  # Fall through to standard iteration

        for i in range(0, len(keys), self.chunk_size):
            batch_ids = keys[i : i + self.chunk_size]
            batch = await self._fetch_batch(batch_ids)
            for instance in batch:
                if all(pred(instance) for pred in predicates):
                    yield instance

    def __repr__(self) -> str:
        return (
            f"<ResultStream: {self.model_klass.__name__} | "
            f"Tx: {self.transaction_id[:8]}... | "
            f"Keys: {len(self._keys)} | "
            f"Filters: {len(self._predicates)}>"
        )
