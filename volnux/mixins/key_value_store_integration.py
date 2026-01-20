import logging
import pickle
import typing
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, ClassVar, Dict, List, Optional, Type, TypeVar, cast

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.conf import ConfigLoader
from volnux.exceptions import ObjectExistError, ObjectDoesNotExist, ImproperlyConfigured
from volnux.import_utils import import_string
from volnux.mixins.identity import ObjectIdentityMixin
from volnux.utils import get_obj_klass_import_str
from volnux.concurrency.async_utils import to_thread

logger = logging.getLogger(__name__)

CONFIG = ConfigLoader.get_lazily_loaded_config()

T = TypeVar("T", bound="ObjectIdentityMixin")


def backend_operation(auto_save: bool = False):
    """Decorator for methods that perform backend operations.

    Args:
        auto_save: If True, automatically save the object after the operation.

    Example:
        >>> @backend_operation(auto_save=True)
        ... def update_status(self, status: str):
        ...     self.status = status
    """

    def decorator(method: Callable) -> Callable:
        @wraps(method)
        def wrapper(self: "KeyValueStoreIntegrationMixin", *args, **kwargs):
            result = method(self, *args, **kwargs)
            if auto_save:
                self.save()
            return result

        return wrapper

    return decorator


class KeyValueStoreIntegrationMixin(ObjectIdentityMixin):
    """
    Mixin to enable backend persistence for classes.

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

    # Class-level backend store instance (shared across all instances)
    _backend_store: ClassVar[Optional[KeyValueStoreBackendBase]] = None
    _backend_config: ClassVar[Optional[Dict[str, Any]]] = None

    def __model_init__(self) -> None:
        """Initialize the model with backend integration.

        This method is called during object initialization to set up
        the backend connection and perform initial save.

        Raises:
            ImproperlyConfigured: If backend initialization fails.
        """
        ObjectIdentityMixin.__init__(self)

        if self._backend_store is None:
            self._initialize_backend()

        if not self._is_loaded_from_backend():
            try:
                self.save()
            except Exception as e:
                logger.warning(f"Failed to auto-save new object: {e}")

    @classmethod
    def _initialize_backend(cls) -> None:
        """Initialize the backend store for this class.

        This method is called once per class to set up the backend connection.
        It reads configuration from CONFIG and creates the appropriate backend store.

        Raises:
            StopProcessingError: If backend initialization fails.
        """
        try:
            backend_config = CONFIG.KEY_VALUE_STORE_CONFIG
            cls._backend_config = backend_config

            backend_class_path = backend_config.get("ENGINE")
            if not backend_class_path:
                raise ImproperlyConfigured("Backend ENGINE not configured")

            backend_class = import_string(backend_class_path)

            connector_config = cast(
                Dict[str, Any], backend_config.get("CONNECTOR_CONFIG", {})
            )

            cls._backend_store = backend_class(**connector_config)

            # Ensure the backend is connected
            if hasattr(cls._backend_store.connector, "connect"):
                if not cls._backend_store.connector.is_connected():
                    cls._backend_store.connector.connect()

            logger.info(
                f"Initialized backend store: {backend_class.__name__} "
                f"for class {cls.__name__}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize backend: {e}")
            raise ImproperlyConfigured(f"Backend initialization failed: {e}") from e

    @classmethod
    def get_backend(cls) -> KeyValueStoreBackendBase:
        """Get the backend store instance.

        Returns:
            The backend store instance.

        Raises:
            RuntimeError: If backend is not initialized.
        """
        if cls._backend_store is None:
            cls._initialize_backend()
        return cls._backend_store

    @classmethod
    def get_schema_name(cls) -> str:
        """Get the schema name for this class.

        By default, uses the class name. Can be overridden for custom schemas.

        Returns:
            The schema name to use for backend storage.
        """
        return cls.__name__

    def _is_loaded_from_backend(self) -> bool:
        """Check if this instance was loaded from the backend.

        Returns:
            True if loaded from backend, False if newly created.
        """
        return getattr(self, "_loaded_from_backend", False)

    def _mark_as_loaded(self) -> None:
        """Mark this instance as loaded from backend."""
        self._loaded_from_backend = True

    def save(
        self, force_insert: bool = False, ttl: typing.Optional[int] = None
    ) -> None:
        """Save this object to the backend store.

        Performs an insert if the record doesn't exist, or an update if it does.
        This is an upsert operation.

        Args:
            force_insert: If True, always attempt insert (raises error if exists).
            ttl: Optional TTL for the new record.

        Raises:
            ObjectExistError: If force_insert is True and record already exists.
        """
        try:
            backend = self.get_backend()
            schema_name = self.get_schema_name()

            if force_insert:
                backend.insert(schema_name, self.id, self, ttl=ttl)
                logger.debug(f"Inserted {self.__class__.__name__}:{self.id}")
            else:
                # Use upsert for save operation
                if hasattr(backend, "upsert"):
                    backend.upsert(schema_name, self.id, self)
                else:
                    # Fallback: try insert, if fails then update
                    try:
                        backend.insert(schema_name, self.id, self, ttl=ttl)
                    except ObjectExistError:
                        backend.update(schema_name, self.id, self)

                logger.debug(f"Saved {self.__class__.__name__}:{self.id}")

            self._mark_as_loaded()

        except ObjectExistError:
            raise
        except Exception as e:
            logger.error(f"Failed to save {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def save_async(
            self, force_insert: bool = False, ttl: typing.Optional[int] = None
    ) -> None:
        """Save this object to the backend store."""
        await to_thread(self.save, force_insert=force_insert, ttl=ttl)

    def update(self) -> None:
        """Update this object in the backend store.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = self.get_backend()
            backend.update(self.get_schema_name(), self.id, self)
            logger.debug(f"Updated {self.__class__.__name__}:{self.id}")
        except Exception as e:
            logger.error(f"Failed to update {self.__class__.__name__}:{self.id}: {e}")
            raise

    def delete(self) -> None:
        """Delete this object from the backend store.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = self.get_backend()
            backend.delete(self.get_schema_name(), self.id)
            logger.debug(f"Deleted {self.__class__.__name__}:{self.id}")
        except Exception as e:
            logger.error(f"Failed to delete {self.__class__.__name__}:{self.id}: {e}")
            raise

    def reload(self) -> None:
        """Reload this object's data from the backend store.

        Updates the current instance with fresh data from the backend.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = self.get_backend()
            backend.reload(self.get_schema_name(), self)
            logger.debug(f"Reloaded {self.__class__.__name__}:{self.id}")
            self._mark_as_loaded()
        except Exception as e:
            logger.error(f"Failed to reload {self.__class__.__name__}:{self.id}: {e}")
            raise

    def refresh(self) -> None:
        """Alias for reload(). Refresh data from backend."""
        self.reload()

    def exists(self) -> bool:
        """Check if this object exists in the backend store.

        Returns:
            True if the record exists, False otherwise.
        """
        try:
            backend = self.get_backend()
            return backend.exists(self.get_schema_name(), self.id)
        except Exception as e:
            logger.error(
                f"Failed to check existence of {self.__class__.__name__}:{self.id}: {e}"
            )
            return False

    @classmethod
    def get(cls: Type[T], record_id: str) -> T:
        """Get an object by its ID from the backend store.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            An instance of the class loaded from the backend.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist.
        """
        try:
            backend = cls.get_backend()
            instance = backend.get(cls.get_schema_name(), record_id, cls)
            instance._mark_as_loaded()
            logger.debug(f"Retrieved {cls.__name__}:{record_id}")
            return instance
        except Exception as e:
            logger.error(f"Failed to get {cls.__name__}:{record_id}: {e}")
            raise

    @classmethod
    def get_or_none(cls: Type[T], record_id: str) -> Optional[T]:
        """Get an object by ID, returning None if it doesn't exist.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            An instance of the class, or None if not found.
        """
        try:
            return cls.get(record_id)
        except ObjectDoesNotExist:
            return None

    @classmethod
    def filter(cls: Type[T], **filters: Any) -> List[T]:
        """Filter objects by the given criteria.

        Args:
            **filters: Field-value pairs to filter by.

        Returns:
            List of instances matching the filters.

        Example:
            >>> active_users = User.filter(status="active")
            >>> admins = User.filter(role="admin", status="active")
        """
        try:
            backend = cls.get_backend()
            instances = backend.filter(cls.get_schema_name(), cls, **filters)

            # Mark all instances as loaded
            for instance in instances:
                instance._mark_as_loaded()

            logger.debug(f"Filtered {cls.__name__}: found {len(instances)} records")
            return instances
        except Exception as e:
            logger.error(f"Failed to filter {cls.__name__}: {e}")
            raise

    @classmethod
    def all(cls: Type[T]) -> List[T]:
        """Get all objects of this class from the backend.

        Returns:
            List of all instances.
        """
        return cls.filter()

    @classmethod
    def count(cls, **filters: Any) -> int:
        """Count objects matching the given filters.

        Args:
            **filters: Optional field-value pairs to filter by.

        Returns:
            Number of matching records.
        """
        try:
            backend = cls.get_backend()
            return backend.count(cls.get_schema_name(), **filters)
        except Exception as e:
            logger.error(f"Failed to count {cls.__name__}: {e}")
            raise

    @classmethod
    def exists_in_backend(cls, record_id: str) -> bool:
        """Check if a record with the given ID exists.

        Args:
            record_id: The ID to check.

        Returns:
            True if exists, False otherwise.
        """
        try:
            backend = cls.get_backend()
            return backend.exists(cls.get_schema_name(), record_id)
        except Exception as e:
            logger.error(f"Failed to check existence: {e}")
            return False

    @classmethod
    def bulk_create(cls: Type[T], instances: List[T]) -> None:
        """Create multiple instances in a single batch operation.

        Args:
            instances: List of instances to create.

        Raises:
            Exception: If bulk creation fails.
        """
        try:
            backend = cls.get_backend()

            if hasattr(backend, "bulk_insert"):
                # Use native bulk insert if available
                records = {instance.id: instance for instance in instances}
                backend.bulk_insert(cls.get_schema_name(), records)
            else:
                # Fallback: insert one by one
                for instance in instances:
                    instance.save(force_insert=True)

            # Mark all as loaded
            for instance in instances:
                instance._mark_as_loaded()

            logger.info(f"Bulk created {len(instances)} {cls.__name__} instances")
        except Exception as e:
            logger.error(f"Failed to bulk create {cls.__name__}: {e}")
            raise

    @classmethod
    def bulk_delete(cls, record_ids: List[str]) -> None:
        """Delete multiple records in a single batch operation.

        Args:
            record_ids: List of record IDs to delete.

        Raises:
            Exception: If bulk deletion fails.
        """
        try:
            backend = cls.get_backend()

            if hasattr(backend, "bulk_delete"):
                # Use native bulk delete if available
                backend.bulk_delete(cls.get_schema_name(), record_ids)
            else:
                # Fallback: delete one by one
                for record_id in record_ids:
                    backend.delete(cls.get_schema_name(), record_id)

            logger.info(f"Bulk deleted {len(record_ids)} {cls.__name__} records")
        except Exception as e:
            logger.error(f"Failed to bulk delete {cls.__name__}: {e}")
            raise

    @classmethod
    def clear_all(cls) -> None:
        """Delete all records of this class from the backend.

        Warning: This is a destructive operation!
        """
        try:
            backend = cls.get_backend()
            if hasattr(backend, "clear_schema"):
                backend.clear_schema(cls.get_schema_name())
            else:
                # Fallback: get all IDs and delete
                all_instances = cls.all()
                record_ids = [instance.id for instance in all_instances]
                cls.bulk_delete(record_ids)

            logger.warning(f"Cleared all {cls.__name__} records from backend")
        except Exception as e:
            logger.error(f"Failed to clear {cls.__name__} records: {e}")
            raise

    @contextmanager
    def atomic(self):
        """Context manager for atomic operations.

        Changes are only saved if the context exits successfully.

        Example:
            >>> with user.atomic():
            ...     user.status = "inactive"
            ...     user.last_login = datetime.now()
            ...     # Changes saved only if no exception
        """
        original_state = self.__getstate__()
        try:
            yield self
            self.save()
        except Exception as e:
            # Restore original state on error
            self.__setstate__(original_state)
            logger.error(f"Atomic operation failed, state restored: {e}")
            raise

    @classmethod
    @contextmanager
    def transaction(cls):
        """Context manager for backend transactions.

        Only supported by backends with transaction support.

        Example:
            >>> with User.transaction():
            ...     user1.save()
            ...     user2.save()
            ...     # Both saved atomically
        """
        backend = cls.get_backend()
        connector = backend.connector

        # Check if backend supports transactions
        if not hasattr(connector, "transaction"):
            logger.warning(
                f"Backend {backend.__class__.__name__} doesn't support transactions"
            )
            yield
            return

        # Use backend's transaction support
        with connector.transaction():
            yield

    def __getstate__(self) -> Dict[str, Any]:
        """Prepare object for pickling.

        Returns:
            Dictionary representation of the object state.

        Raises:
            pickle.PickleError: If the object cannot be pickled.
        """
        try:
            state = self.get_state()
        except NotImplementedError:
            raise pickle.PickleError(
                f"Cannot pickle object of type {self.__class__.__name__}"
            )

        if hasattr(self, "_backend_store") and self._backend_store is not None:
            state["_backend_class"] = get_obj_klass_import_str(
                self._backend_store.__class__
            )

        # Remove non-serializable attributes
        state.pop("_backend_store", None)
        state.pop("_backend_config", None)

        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Restore object state after unpickling.

        Args:
            state: Dictionary containing object state.

        Raises:
            pickle.UnpicklingError: If the object cannot be unpickled.
        """
        # Remove backend class info (will be reinitialized)
        state.pop("_backend_class", None)

        try:
            self.set_state(state)
        except NotImplementedError:
            raise pickle.UnpicklingError(
                f"Cannot unpickle object of type {self.__class__.__name__}"
            )

        # Ensure backend is initialized for this class
        if self._backend_store is None:
            self._initialize_backend()

    @classmethod
    def close_backend(cls) -> None:
        """Close the backend connection.

        This should be called when the application shuts down.
        """
        if cls._backend_store is not None:
            try:
                if hasattr(cls._backend_store, "close"):
                    cls._backend_store.close()
                elif hasattr(cls._backend_store.connector, "disconnect"):
                    cls._backend_store.connector.disconnect()

                logger.info(f"Closed backend for {cls.__name__}")
            except Exception as e:
                logger.warning(f"Error closing backend: {e}")
            finally:
                cls._backend_store = None
                cls._backend_config = None

    def __repr__(self) -> str:
        """String representation of the object."""
        exists_str = "exists" if self.exists() else "new"
        return f"<{self.__class__.__name__}:{self.id} [{exists_str}]>"
