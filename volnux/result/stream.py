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
from volnux.exceptions import ObjectDoesNotExist
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.concurrency.kv_executor import run_in_kv_executor
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

    Caching semantics:

    * The memory tier holds object *references*, not copies. An instance you
      ``add()`` is the same object you get back, and mutating a yielded object
      mutates the cached one (``bulk_update`` relies on this). Records loaded
      from the persisted tier are cached the same way, so a mutation that was
      not saved is visible to later reads of the same stream chain.
    * A cached record is not re-validated against the persisted tier. If
      another writer changes a record while a stream holds it, the stream keeps
      returning its cached copy until it is evicted by capacity, or you call
      ``evict_memory()``. Use ``no_cache()`` when you need fresh reads.
    * Streams derived from one another (``filter``, ``order_by``, ``paginate``,
      ...) share one memory tier and one capacity bound. ``no_cache()`` and
      ``shard()`` start separate caches.
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
    # Result of optimize(), reused across iterations; reset whenever keys change.
    _optimized: typing.Optional["ResultStream[T]"] = field(
        default=None, repr=False, init=False
    )

    def __post_init__(self) -> None:
        if self.chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {self.chunk_size}")

        if self.memory_capacity > 0:
            self.memory_backend = InMemoryKeyValueStoreBackend(
                namespace_prefix=self.transaction_id
            )
        # memory_capacity == 0 → no memory backend, no caching

    # The memory tier is an in-process dict, so its operations run inline on the
    # event loop: a thread hop would cost more than the lookup itself. LRU
    # bookkeeping is therefore never interleaved with an await, which keeps it
    # consistent when several derived streams share one cache (see ``_make``).

    async def _cache_get(self, schema: str, record_id: str) -> Optional[T]:
        """Get from memory backend + update LRU access order."""
        if self.memory_backend is None:
            return None
        instance = typing.cast(
            T, self.memory_backend.get(schema, record_id, self.model_klass)
        )
        if record_id in self._access_order:
            self._access_order.move_to_end(record_id)
        return instance

    async def _cache_put(self, schema: str, record_id: str, instance: T) -> None:
        """Put into memory backend with LRU eviction when at capacity."""
        await self._cache_put_many(schema, {record_id: instance})

    async def _cache_delete(self, schema: str, record_id: str) -> None:
        if self.memory_backend is None:
            return
        self._access_order.pop(record_id, None)
        self.memory_backend.delete(schema, record_id)

    async def _cache_clear(self, schema: str) -> None:
        if self.memory_backend is None:
            return
        self.memory_backend.clear_schema(schema)
        self._access_order.clear()

    @classmethod
    async def _run_sync(
        cls, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Offload a sync backend method to the stream executor."""
        return await run_in_kv_executor(func, *args, **kwargs)

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
        cache_from: Optional["ResultStream[T]"] = None,
    ) -> "ResultStream[T]":
        """Build a stream without running ``__post_init__``.

        When ``cache_from`` is given and caching is enabled, the new stream
        *shares* that stream's memory tier and LRU bookkeeping instead of
        starting with an empty cache. Streams derived from one another
        (``filter``, ``order_by``, ``paginate``...) read the same model's
        records, so records already fetched stay cached across the chain and
        the capacity bound applies to the whole chain, not to each link.
        """
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
        stream._optimized = None
        if (
            cache_from is not None
            and memory_capacity > 0
            and cache_from.memory_backend is not None
        ):
            stream.memory_backend = cache_from.memory_backend
            stream._access_order = cache_from._access_order
            stream.memory_capacity = cache_from.memory_capacity
        else:
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
        except (ObjectDoesNotExist, KeyError):
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
        except ObjectDoesNotExist:
            # Genuinely absent (e.g. deleted since the stream was built).
            # Any other error (connection, deserialisation) must propagate.
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
            self._optimized = None

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
            self._optimized = None

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
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            cache_from=self,
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
            cache_from=self,
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

    async def exists(self) -> bool:
        """
        Check whether at least one object matches this stream.

        This evaluates the stream's filters (len() only counts candidate keys
        before filtering), stopping at the first match.

        :return: True if any object matches, False otherwise.
        """
        return await self.has_results()

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
        """Returns the number of tracked IDs *before* filters are applied.

        Use ``await stream.count()`` for the number of matching objects.
        """
        return len(self._keys)

    async def _cache_put_many(self, schema: str, items: typing.Dict[str, T]) -> None:
        """Insert several records into the memory tier, evicting LRU entries."""
        backend = self.memory_backend
        if backend is None or self.memory_capacity <= 0 or not items:
            return

        # Only the most recent `memory_capacity` records can be retained.
        if len(items) > self.memory_capacity:
            items = dict(list(items.items())[-self.memory_capacity :])

        for rid, instance in items.items():
            if rid in self._access_order:
                self._access_order.move_to_end(rid)
            else:
                while len(self._access_order) >= self.memory_capacity:
                    evict_key, _ = self._access_order.popitem(last=False)
                    try:
                        backend.delete(schema, evict_key)
                    except Exception:
                        pass
                self._access_order[rid] = None
            backend.upsert(schema, rid, instance)

    async def _fetch_batch(self, batch_ids: List[str]) -> List[T]:
        """Resolve a batch: memory-first, then one multi-get for the rest.

        The schema is resolved once, the memory tier is read inline and the
        persisted tier is queried with a single ``get_many`` round trip. Results
        keep the order of ``batch_ids``; ids missing from every tier are dropped.
        """
        if not batch_ids:
            return []
        schema = await self._schema_name()
        found: typing.Dict[str, T] = {}

        memory = self.memory_backend
        if memory is not None:

            for rid in batch_ids:
                try:
                    found[rid] = typing.cast(
                        T, memory.get(schema, rid, self.model_klass)
                    )
                except (ObjectDoesNotExist, KeyError):
                    pass
            for rid in found:  # refresh LRU recency
                if rid in self._access_order:
                    self._access_order.move_to_end(rid)

        missing = [rid for rid in batch_ids if rid not in found]
        if missing:
            backend = await self._ensure_persisted_backend()
            if backend is not None:
                fetched = typing.cast(
                    typing.Dict[str, T],
                    await self._run_sync(
                        backend.get_many, schema, missing, self.model_klass
                    ),
                )
                found.update(fetched)
                try:
                    await self._cache_put_many(schema, fetched)
                except Exception as e:
                    logger.debug("Failed to cache fetched batch: %s", e)

        return [found[rid] for rid in batch_ids if rid in found]

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
            cache_from=self,
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
            cache_from=self,
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
            cache_from=self,
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
        offset = (page - 1) * page_size
        paginated_keys: List[str] = []
        has_filters = bool(self._predicates or self._q_objects)
        total = 0
        async for obj in self:
            if offset <= total < offset + page_size:
                paginated_keys.append(obj.id)
            total += 1
            # Without filters the total is just the key count, so stop early.
            if not has_filters and len(paginated_keys) >= page_size:
                total = len(self)
                break

        return (
            ResultStream._make(
                model_klass=self.model_klass,
                transaction_id=self.transaction_id,
                keys=paginated_keys,
                predicates=[],
                chunk_size=self.chunk_size,
                memory_capacity=self.memory_capacity,
                persisted_backend=self.persisted_backend,
                cache_from=self,
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
            cache_from=self,
            q_predicates=self._q_objects,
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
        backend = await self._ensure_persisted_backend()
        updated = 0

        async for obj in self:
            for key, value in kwargs.items():
                if hasattr(obj, key):
                    setattr(obj, key, value)
                else:
                    warnings.warn(f"{obj} lacks attribute '{key}'", UserWarning)

            # _cache_put is a no-op when caching is disabled (memory_capacity == 0)
            await self._cache_put(schema, obj.id, obj)
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
        backend = await self._ensure_persisted_backend()
        deleted = 0

        async for obj in self:
            # Enforce FK protect/cascade rules, same as obj.delete()
            await obj._on_delete_hook()

            was_deleted = False
            if backend is not None:
                try:
                    await self._run_sync(backend.delete, schema, obj.id)
                    was_deleted = True
                except ObjectDoesNotExist:
                    pass  # already gone from durable storage

            if self.memory_backend is not None:
                try:
                    await self._cache_delete(schema, obj.id)
                    was_deleted = True
                except ObjectDoesNotExist:
                    pass

            if was_deleted:
                self._key_set.discard(obj.id)
                deleted += 1

        self._keys = [k for k in self._keys if k in self._key_set]
        self._optimized = None
        return deleted

    def union(self, *streams: "ResultStream[T]") -> "ResultStream[T]":
        """Combine streams, preserving order (first occurrence wins) and
        removing duplicates.

        Each input's filters still apply to its own keys: a key contributed
        only by a filtered stream is kept only if it passes that stream's
        filters.
        """
        sources = [self, *streams]
        combined_keys: List[str] = []
        seen: Set[str] = set()
        for stream in sources:
            for key in stream._keys:
                if key not in seen:
                    seen.add(key)
                    combined_keys.append(key)

        predicates: List[Callable[[T], bool]] = []
        if any(src._predicates for src in sources):
            memberships = [(set(src._keys), list(src._predicates)) for src in sources]

            def _passes_any_source(obj: T) -> bool:
                return any(
                    obj.id in keys and all(pred(obj) for pred in preds)
                    for keys, preds in memberships
                )

            predicates = [_passes_any_source]

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=combined_keys,
            predicates=predicates,
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend
            or next(
                (s.persisted_backend for s in streams if s.persisted_backend), None
            ),
            cache_from=self,
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
        if self._optimized is not None:
            return self._optimized

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

            except (ValueError, NotImplementedError):
                remaining_q_objects.append(q)

        if not any_optimized:
            # No Q objects could be pushed — nothing changed
            self._optimized = self
            return self

        # Preserve original key ordering for deterministic chunking/pagination
        if current_keys is not None:
            optimized_key_list = [k for k in self._keys if k in current_keys]
        else:
            optimized_key_list = self._keys

        optimized = ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=optimized_key_list,
            predicates=self._predicates,
            chunk_size=self.chunk_size,
            memory_capacity=self.memory_capacity,
            persisted_backend=self.persisted_backend,
            cache_from=self,
            q_predicates=remaining_q_objects,
            join_fields=self._join_fields,
        )
        self._optimized = optimized
        return optimized

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
                joined_results = await self._run_sync(
                    self.persisted_backend.filter_with_joins,
                    schema,
                    self.model_klass,
                    self._join_fields,
                )

                # Restrict to this stream's keys (filter/shard/paginate/order_by
                # all narrow `keys`) and keep their order.
                by_id = {str(r.get("id")): r for r in joined_results}
                for key in keys:
                    record_dict = by_id.get(str(key))
                    if record_dict is None:
                        continue
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
