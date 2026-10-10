import inspect
import logging
import typing
import re
import os
import asyncio
from contextlib import asynccontextmanager
from functools import wraps
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
    Set,
    Tuple,
    Union,
    get_args,
    TYPE_CHECKING,
    ForwardRef,
)
from concurrent.futures import ThreadPoolExecutor
from formax import Attrib
from formax.typing import get_type_hints, evaluate_forward_ref

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.fields import OnDelete
from volnux.exceptions import (
    ObjectExistError,
    ObjectProtectedError,
    ObjectDoesNotExist,
)
from volnux.import_utils import import_string
from .connection import BackendConnectionIntegrationMixin
from volnux.backends.db_utils import default_native_fk_check

if TYPE_CHECKING:
    from volnux.config import VolnuxConfig
    from volnux.result.stream import ResultStream

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="KeyValueStoreIntegrationMixin")

# Dedicated executor for blocking KV backend calls.
# NEVER use default asyncio.to_thread pool for persistence operations.
_KV_BACKEND_EXECUTOR = ThreadPoolExecutor(
    max_workers=128, thread_name_prefix="volnux-kv-backend"
)


def _resolve_foreign_keys_for_class(cls: Type["KeyValueStoreIntegrationMixin"]) -> None:
    """
    Scan a class for ForeignKey fields and register backreferences.

    Detects fields tagged with _volnux_fk metadata on their Attrib
    and calls register_backreference on the target model.

    Args:
        cls: The class to scan (a KeyValueStoreIntegrationMixin subclass).
    """

    try:
        hints = get_type_hints(cls, include_extras=True)
    except Exception:
        return

    for field_name, hint in hints.items():
        args = get_args(hint)
        if len(args) != 2:
            continue

        attrib = args[1]

        if isinstance(attrib, Attrib):
            fk_meta = attrib.metadata
            if not fk_meta:
                continue

            target_model: Union[
                Type["KeyValueStoreIntegrationMixin"], None, ForwardRef
            ] = fk_meta.get("target_model")
            if not target_model:
                continue

            if isinstance(target_model, ForwardRef):
                try:
                    if target_model.__forward_arg__ == cls.__name__:
                        target_model = cls
                    else:
                        import sys

                        module = sys.modules[cls.__module__]
                        target_model: Type["KeyValueStoreIntegrationMixin"] = (
                            evaluate_forward_ref(target_model, module.__dict__, None)
                        )
                    fk_meta["target_model"] = target_model
                except Exception as e:
                    logger.warning(
                        "Could not resolve ForwardRef for %s.%s: %s",
                        cls.__name__,
                        field_name,
                        e,
                    )
                    raise

            if default_native_fk_check(cls, target_model):
                fk_meta["has_native_fk"] = True
            else:
                fk_meta["has_native_fk"] = False

            attrib.metadata = fk_meta

            reverse_name = fk_meta.get("reverse_name")
            if not reverse_name:
                # Auto-generate: class_name + "_" + field_name
                source_name = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()
                reverse_name = f"{source_name}_{field_name}"

            target_model.register_backreference(
                field_name=field_name,
                field_attrib=attrib,
                referencing_model=cls,
                reverse_name=reverse_name,
                has_native_fk=fk_meta["has_native_fk"],
            )


class _ReverseRelationDescriptor:
    """Descriptor providing async reverse relation access.

    Enables patterns like:
        workflows = await user.workflows # All workflows created by this user
        entries = await user.audit_entries # All audit entries referencing this user

    Returns a ResultStream (never a single object). No caching — each
    access performs a fresh query to ensure consistency. Caching is
    the caller's responsibility if needed.
    """

    __slots__ = ("_referencing_model", "_foreign_key_field")

    def __init__(
        self,
        referencing_model: Union[Type["KeyValueStoreIntegrationMixin"], str],
        foreign_key_field: str,
    ):
        self._referencing_model = referencing_model
        self._foreign_key_field = foreign_key_field

    def _get_model(self) -> Type["KeyValueStoreIntegrationMixin"]:
        """Resolve string references lazily. Mutates slot in place."""
        if isinstance(self._referencing_model, str):
            self._referencing_model = import_string(self._referencing_model)
        return self._referencing_model  # type: ignore[return-value]

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self

        # Return an awaitable that resolves to a ResultStream.
        # Callers use: results = await user.workflows
        return _ReverseRelationQuery(
            descriptor=self,
            instance=instance,
        )

    def __set__(self, instance: Any, value: Any) -> None:
        raise AttributeError("Reverse relations are read-only")

    def __delete__(self, instance: Any) -> None:
        raise AttributeError("Reverse relations cannot be deleted")

    def __repr__(self) -> str:
        model_name = (
            self._referencing_model
            if isinstance(self._referencing_model, str)
            else self._referencing_model.__name__
        )
        return f"<ReverseRelation: {model_name}.{self._foreign_key_field}>"


class _ReverseRelationQuery:
    """Awaitable proxy returned by _ReverseRelationDescriptor.__get__.

    Defers the async filter call until the caller actually awaits it.
    This allows `await user.workflows` syntax while keeping __get__
    synchronous (as required by the descriptor protocol).
    """

    __slots__ = ("_descriptor", "_instance")

    def __init__(
        self,
        descriptor: _ReverseRelationDescriptor,
        instance: "KeyValueStoreIntegrationMixin",
    ):
        self._descriptor = descriptor
        self._instance = instance

    async def _resolve(self) -> "ResultStream[T]":
        model_class = self._descriptor._get_model()
        filter_key = f"{self._descriptor._foreign_key_field}__object_id"
        return await model_class.filter(**{filter_key: str(self._instance.id)})

    def __await__(self):
        return self._resolve().__await__()

    def __repr__(self) -> str:
        return (
            f"<ReverseRelationQuery: "
            f"{type(self._instance).__name__}({self._instance.id}) "
            f"-> {self._descriptor}>"
        )


def backend_operation(
    auto_save: bool = False, force_insert: bool = False, ttl: Optional[int] = None
):
    """Decorator for methods that perform backend operations.

    Args:
        auto_save: If True, automatically save the object after the operation.
        force_insert: If True, always attempt insert (raises error if exists).
        ttl: Time-to-live in seconds for the operation result. None for no expiration.

    Example:
        >>> @backend_operation(auto_save=True)
        ... def update_status(self, status: str):
        ...     self.status = status
    """

    def decorator(method: Callable) -> Callable:
        @wraps(method)
        async def wrapper(self: "KeyValueStoreIntegrationMixin", *args, **kwargs):
            result = method(self, *args, **kwargs)
            # Support async methods: finish the operation before auto-saving
            if inspect.isawaitable(result):
                result = await result
            if auto_save:
                await self.save(force_insert=force_insert, ttl=ttl)
            return result

        return wrapper

    return decorator


class KeyValueStoreIntegrationMixin(BackendConnectionIntegrationMixin):
    """
    Mixin to enable backend persistence for classes.

    Provides an interface for classes to integrate backend storage by utilizing
    a key-value store. This allows objects to be persistently stored, retrieved,
    and updated via a backend configured in the application settings. This mixin
    also supports flexible backend initialization and schema management, ensuring
    seamless integration with diverse storage systems.

    :ivar _backend_store: Class-level backend store instance used for backend operations.
    :type _backend_store: ClassVar[Optional[KeyValueStoreBackendBase]]
    :ivar _backend_config: Configuration settings for the backend.
    :type _backend_config: ClassVar[Optional[Dict[str, Any]]]

    The backend is configured via CONFIG.KEY_VALUE_STORE_CONFIG.

    Example:
        >>> @dataclass
        ... class User(KeyValueStoreIntegrationMixin):
        ...     name: str
        ...     email: str
        ...     status: str = "active"
        ...
        >>> user = User(name="Alice", email="alice@example.com")
        >>> user.save()  # Automatically persisted
        >>>
        >>> loaded = User.get("user_123")  # Load from backend
        >>> loaded.status = "inactive"
        >>> loaded.save()  # Update in backend
    """

    # Backreference registry
    # Maps field_name -> set of (model_class, reverse_name, Attrib, has_native_fk) tuples
    # Example: {"created_by": {(Workflow, "workflows", attrib, False), (AuditEntry, "audit_entries", attrib, True)}}
    _backreferences: ClassVar[Dict[str, Set[Tuple[Type, str, Attrib, bool]]]]

    def __init_subclass__(cls, **kwargs):
        """Register ForeignKey backreferences when a subclass is defined.

        At this point, both the source class and the target model are
        fully defined. We scan annotations for ForeignKey fields and
        register backreferences on the target models.
        """
        super().__init_subclass__(**kwargs)

        # Each subclass gets its OWN isolated backreference registry
        cls._backreferences = {}
        _resolve_foreign_keys_for_class(cls)

    @classmethod
    async def _run_in_kv_executor(cls, func, *args, **kwargs):
        """Run a sync backend method in the dedicated KV executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            _KV_BACKEND_EXECUTOR, lambda: func(*args, **kwargs)
        )

    @classmethod
    def register_backreference(
        cls,
        field_name: str,
        field_attrib: Attrib,
        referencing_model: Type["KeyValueStoreIntegrationMixin"],
        reverse_name: Optional[str] = None,
        has_native_fk: bool = False,
    ) -> None:
        """Register that another model references this model via a field.

        This enables reverse relation accessors. Called automatically by
        ForeignKeyField when a model class is defined.

        Args:
            field_name: The field name on the referencing model.
            field_attrib: The attribute descriptor for the foreign key field.
            referencing_model: The model class that references this model.
            reverse_name: Name for the reverse accessor. If None, defaults
                         to the referencing model's class name in snake_case
                         with 's' appended.
            has_native_fk: Whether the referencing model has a native foreign key field.
        """
        if reverse_name is None:
            name = re.sub(r"(?<!^)(?=[A-Z])", "_", referencing_model.__name__).lower()
            reverse_name = f"{name}s"

        if field_name not in cls._backreferences:
            cls._backreferences[field_name] = set()

        cls._backreferences[field_name].add(
            (referencing_model, reverse_name, field_attrib, has_native_fk)
        )

        # Create the reverse accessor on this model
        if not hasattr(cls, reverse_name):
            setattr(
                cls,
                reverse_name,
                _ReverseRelationDescriptor(
                    referencing_model=referencing_model,
                    foreign_key_field=field_name,
                ),
            )

    def _after_backend_init(self, **kwargs: Any) -> None:
        self._loaded_from_backend = False

    async def _on_delete_hook(self) -> None:
        """Process all backreferences before deletion."""
        for field_name, references in self._backreferences.items():
            for reference_tuple in references:
                referencing_model, reverse_name, field_attrib, has_native_fk = cast(
                    Tuple[Type["KeyValueStoreIntegrationMixin"], str, Attrib, bool],
                    reference_tuple,
                )

                action: OnDelete = field_attrib.metadata.get(
                    "on_delete", OnDelete.PROTECT
                )

                if has_native_fk:
                    # Database handles CASCADE, SET NULL, SET DEFAULT automatically
                    # We only need to handle PROTECT (which is NO ACTION in SQL)
                    if action == OnDelete.PROTECT:
                        raise ObjectProtectedError(
                            f"Cannot delete {self.__class__.__name__}({self.id}): "
                            f"{referencing_model.__name__} objects via '{field_name}'. "
                            f"Protected by native foreign key constraint."
                        )

                    # For other on_delete values, the database handles it
                    continue

                try:
                    await action.operation_handler(
                        self.id,
                        self.__class__.__name__,
                        referencing_model,
                        field_name,
                        field_attrib,
                    )
                except Exception as e:
                    logger.error(f"Failed to process backreference: {e}")
                    raise

    @classmethod
    def get_migration_dir(cls) -> str:
        """Get the directory where migrations are stored for this class."""
        volnux_config = cls.get_volnux_config()
        try:
            return os.path.join(volnux_config.MIGRATION_DIR, f"{cls.__name__}")
        except AttributeError:
            return os.path.join(
                os.path.expanduser("~"), ".volnux", "migrations", f"{cls.__name__}"
            )

    @classmethod
    def get_volnux_config(cls) -> "VolnuxConfig":
        from volnux.config import VolnuxConfig

        return VolnuxConfig.get_instance()

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        """Get the backend configuration for this class."""
        return cls.get_volnux_config().KEY_VALUE_STORE_CONFIG

    def _is_loaded_from_backend(self) -> bool:
        """Check if this instance was loaded from the backend.

        Returns:
            True if loaded from the backend, False if newly created.
        """
        return getattr(self, "_loaded_from_backend", False)

    def _mark_as_loaded(self) -> None:
        """Mark this instance as loaded from the backend."""
        self._loaded_from_backend = True

    async def save(
        self, force_insert: bool = False, ttl: typing.Optional[int] = None
    ) -> None:
        """Save this object to the backend store.

        Performs an insert if the record doesn't exist, or an update if it does.
        This is an upsert operation.

        Args:
            force_insert: If True, always attempt insert (raises error if exists).
            ttl: Optional TTL for the new record.

        Raises:
            ObjectExistError: If force_insert is True and the record already exists.
        """
        try:
            backend = await self.get_backend()
            schema_name = await self.get_schema_name()

            if force_insert:
                await self._run_in_kv_executor(
                    backend.insert, schema_name, self.id, self, ttl=ttl
                )
                logger.debug(f"Inserted {self.__class__.__name__}:{self.id}")
            else:
                # Use upsert for save operation
                if hasattr(backend, "upsert"):
                    await self._run_in_kv_executor(
                        backend.upsert, schema_name, self.id, self
                    )
                else:
                    # Fallback: try insert, if fails, then update
                    try:
                        await self._run_in_kv_executor(
                            backend.insert, schema_name, self.id, self, ttl=ttl
                        )
                    except ObjectExistError:
                        await self._run_in_kv_executor(
                            backend.update, schema_name, self.id, self
                        )

                logger.debug(f"Saved {self.__class__.__name__}:{self.id}")

            self._mark_as_loaded()

        except ObjectExistError:
            raise
        except Exception as e:
            logger.error(f"Failed to save {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def update(self) -> None:
        """Update this object in the backend store.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = await self.get_backend()
            await self._run_in_kv_executor(
                backend.update, await self.get_schema_name(), self.id, self
            )
            logger.debug(f"Updated {self.__class__.__name__}:{self.id}")
        except Exception as e:
            logger.error(f"Failed to update {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def delete(self) -> None:
        """Delete this object from the backend store.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = await self.get_backend()
            await self._on_delete_hook()
            await self._run_in_kv_executor(
                backend.delete, await self.get_schema_name(), self.id
            )
            logger.debug(f"Deleted {self.__class__.__name__}:{self.id}")
        except Exception as e:
            logger.error(f"Failed to delete {self.__class__.__name__}:{self.id}: {e}")
            raise

    @classmethod
    async def atomic_update_field(cls, record_id: str, field: str, value: Any) -> bool:
        """Atomically update a single JSON field. Returns False if key missing."""
        backend = await cls.get_backend()
        if not hasattr(backend, "atomic_update_field"):
            raise NotImplementedError(
                f"{type(backend).__name__} does not support atomic_update_field"
            )
        return await cls._run_in_kv_executor(
            backend.atomic_update_field,
            await cls.get_schema_name(),
            record_id,
            field,
            value,
        )

    @classmethod
    async def atomic_append_to_field(
        cls, record_id: str, field: str, value: Any
    ) -> bool:
        """Atomically append to a JSON array field. Returns False if key missing."""
        backend = await cls.get_backend()
        if not hasattr(backend, "atomic_append_to_field"):
            raise NotImplementedError(
                f"{type(backend).__name__} does not support atomic_append_to_field"
            )
        return await cls._run_in_kv_executor(
            backend.atomic_append_to_field,
            await cls.get_schema_name(),
            record_id,
            field,
            value,
        )

    async def reload(self) -> None:
        """Reload this object's data from the backend store.

        Updates the current instance with fresh data from the backend.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = await self.get_backend()
            await self._run_in_kv_executor(
                backend.reload, await self.get_schema_name(), self
            )
            self._mark_as_loaded()
            logger.debug(f"Reloaded {self.__class__.__name__}:{self.id}")
        except Exception as e:
            logger.error(f"Failed to reload {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def refresh(self) -> None:
        """Alias for reload(). Refresh data from the backend."""
        await self.reload()

    async def exists(self) -> bool:
        """Check if this object exists in the backend store.

        Returns:
            True if the record exists, False otherwise.
        """
        try:
            backend = await self.get_backend()
            return await self._run_in_kv_executor(
                backend.exists, await self.get_schema_name(), self.id
            )
        except Exception as e:
            logger.error(
                f"Failed to check existence of {self.__class__.__name__}:{self.id}: {e}"
            )
            return False

    @classmethod
    async def get(cls, record_id: str) -> T:
        """Get an object by its ID from the backend store.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            An instance of the class loaded from the backend.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist.
        """
        try:
            backend = await cls.get_backend()
            instance = await cls._run_in_kv_executor(
                backend.get, await cls.get_schema_name(), record_id, cls
            )
            instance._mark_as_loaded()
            logger.debug(f"Retrieved {cls.__name__}:{record_id}")
            return instance
        except Exception as e:
            logger.error(f"Failed to get {cls.__name__}:{record_id}: {e}")
            raise

    @classmethod
    async def get_or_none(cls, record_id: str) -> Optional[T]:
        """Get an object by ID, returning None if it doesn't exist.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            An instance of the class, or None if not found.
        """
        try:
            return await cls.get(record_id)
        except ObjectDoesNotExist:
            return None

    @classmethod
    async def filter(cls: Type[T], **filters: Any) -> "ResultStream[T]":
        """Filter objects by the given criteria.

        Args:
            **filters: Field-value pairs to filter by.

        Returns:
            Stream of instances matching the filters.

        Example:
            >>> active_users = await User.filter(status="active")
            >>> admins = await User.filter(role="admin", status="active")
        """
        try:
            backend = await cls.get_backend()
            instances = await cls._run_in_kv_executor(
                backend.filter, await cls.get_schema_name(), cls, **filters
            )
            logger.debug(f"Filtered {cls.__name__}: found {len(instances)} records")
            return instances
        except Exception as e:
            logger.error(f"Failed to filter {cls.__name__}: {e}")
            raise

    @classmethod
    async def all(cls) -> "ResultStream[T]":
        """Get all objects of this class from the backend.

        Returns:
            List of all instances.
        """
        return await cls.filter()

    @classmethod
    async def count(cls, **filters: Any) -> int:
        """Count objects matching the given filters.

        Args:
            **filters: Optional field-value pairs to filter by.

        Returns:
            Number of matching records.
        """
        try:
            backend = await cls.get_backend()
            return await cls._run_in_kv_executor(
                backend.count, await cls.get_schema_name(), **filters
            )
        except Exception as e:
            logger.error(f"Failed to count {cls.__name__}: {e}")
            raise

    @classmethod
    async def exists_in_backend(cls, record_id: str) -> bool:
        """Check if a record with the given ID exists.

        Args:
            record_id: The ID to check.

        Returns:
            True if exists, False otherwise.
        """
        try:
            backend = await cls.get_backend()
            return await cls._run_in_kv_executor(
                backend.exists, await cls.get_schema_name(), record_id
            )
        except Exception as e:
            return False

    @classmethod
    async def bulk_create(cls, instances: List[T]) -> None:
        """Create multiple instances in a single batch operation.

        Args:
            instances: List of instances to create.

        Raises:
            Exception: If bulk creation fails.
        """
        try:
            backend = await cls.get_backend()

            if hasattr(backend, "bulk_insert"):
                # Use native bulk insert if available
                records = {instance.id: instance for instance in instances}
                await cls._run_in_kv_executor(
                    backend.bulk_insert, await cls.get_schema_name(), records
                )
            else:
                # Fallback: insert one by one
                for instance in instances:
                    await instance.save(force_insert=True)

            # Mark all as loaded
            for instance in instances:
                instance._mark_as_loaded()

            logger.info(f"Bulk created {len(instances)} {cls.__name__} instances")
        except Exception as e:
            logger.error(f"Failed to bulk create {cls.__name__}: {e}")
            raise

    @classmethod
    async def bulk_delete(cls, record_ids: List[str]) -> None:
        """Delete multiple records in a single batch operation.

        Args:
            record_ids: List of record IDs to delete.

        Raises:
            Exception: If bulk deletion fails.
        """
        try:
            backend = await cls.get_backend()

            if hasattr(backend, "bulk_delete"):
                # Use native bulk delete if available
                await cls._run_in_kv_executor(
                    backend.bulk_delete, await cls.get_schema_name(), record_ids
                )
            else:
                # Fallback
                for record_id in record_ids:
                    await cls._run_in_kv_executor(
                        backend.delete, await cls.get_schema_name(), record_id
                    )

            logger.info(f"Bulk deleted {len(record_ids)} {cls.__name__} records")
        except Exception as e:
            logger.error(f"Failed to bulk delete {cls.__name__}: {e}")
            raise

    @classmethod
    async def clear_all(cls) -> None:
        """Delete all records of this class from the backend.

        Warning: This is a destructive operation!
        """
        try:
            backend = await cls.get_backend()
            if hasattr(backend, "clear_schema"):
                await cls._run_in_kv_executor(
                    backend.clear_schema, await cls.get_schema_name()
                )
            else:
                # Fallback: get all IDs and delete
                all_instances = await cls.all()
                record_ids = [instance.id async for instance in all_instances]
                await cls.bulk_delete(record_ids)  # type: ignore

            logger.warning(f"Cleared all {cls.__name__} records from backend")
        except Exception as e:
            logger.error(f"Failed to clear {cls.__name__} records: {e}")
            raise

    @asynccontextmanager
    async def atomic(self):
        """Context manager for atomic operations.

        Changes are only saved if the context exits successfully.

        Example:
            >>> async with user.atomic():
            ...     user.status = "inactive"
            ...     user.last_login = datetime.now()
            ...     # Changes saved only if no exception
        """
        original_state = self.__getstate__()
        try:
            yield self
            await self.save()
        except Exception as e:
            # Restore the original state on error
            self.__setstate__(original_state)
            logger.error(f"Atomic operation failed, state restored: {e}")
            raise

    @classmethod
    @asynccontextmanager
    async def transaction(cls):
        """Context manager for backend transactions.

        Only supported by backends with transaction support.

        Example:
            >>> async with User.transaction():
            ...     await user1.save()
            ...     await user2.save()
            ...     # Both saved atomically
        """
        backend = await cls.get_backend()
        connector = getattr(backend, "connector", None)

        if connector and hasattr(connector, "transaction"):
            # Run sync transaction context in executor thread
            loop = asyncio.get_running_loop()
            tx_ctx = connector.transaction()

            # Enter transaction in executor thread
            await loop.run_in_executor(_KV_BACKEND_EXECUTOR, tx_ctx.__enter__)
            try:
                yield
                # Commit in executor thread
                await loop.run_in_executor(
                    _KV_BACKEND_EXECUTOR, tx_ctx.__exit__, None, None, None
                )
            except BaseException as exc:
                # Rollback in executor thread
                await loop.run_in_executor(
                    _KV_BACKEND_EXECUTOR,
                    tx_ctx.__exit__,
                    type(exc),
                    exc,
                    exc.__traceback__,
                )
                raise
        else:
            logger.warning(
                "Backend %s does not support transactions",
                type(backend).__name__,
            )
            yield

    def __repr__(self) -> str:
        """String representation of the object."""
        exists_str = "exists" if self._is_loaded_from_backend() else "new"
        return f"<{self.__class__.__name__}:{self.id} [{exists_str}]>"
