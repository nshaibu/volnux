import logging
import asyncio
from typing import (
    Any,
    ClassVar,
    Dict,
    Optional,
    Type,
    TypeVar,
    TYPE_CHECKING,
    Callable,
    AsyncGenerator,
)
from contextlib import asynccontextmanager

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.exceptions import (
    ImproperlyConfigured,
    SerializationError,
)
from volnux.backends.storage_route import StorageRoute
from volnux.import_utils import import_string
from volnux.mixins.identity import ObjectIdentityMixin
from volnux.utils import get_obj_klass_import_str
from volnux.concurrency.async_utils import to_thread

if TYPE_CHECKING:
    from volnux.config import VolnuxConfig

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BackendConnectionIntegrationMixin")


class BackendConnectionIntegrationMixin(ObjectIdentityMixin):
    """
    Mixin providing foundational backend connectivity, configuration loading,
    and lifecycle management across storage and messaging integration mixins.

    Provides a clean, unified interface for initializing database/broker connections
    without coupling models to specific persistence paradigms (KeyValue, Messaging, Relational).

    :ivar _backend_store: Class-level backend store instance shared across instances.
    :ivar _backend_config: Configuration dictionary for the active backend.
    """

    # Class-level backend store instance (shared across all instances)
    _backend_store: ClassVar[Optional["KeyValueStoreBackendBase"]]
    _backend_config: ClassVar[Optional[Dict[str, Any]]]
    _backend_lock: ClassVar[asyncio.Lock]
    _backend_initialized: ClassVar[bool]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._backend_store = None
        cls._backend_config = None
        cls._backend_lock = asyncio.Lock()
        cls._backend_initialized = False

    def __post_init__(
        self,
        *args,
        **kwargs: Any,
    ) -> None:
        """
        Base post-initialization hook. Ensures backend connectivity upon model instantiation.
        """
        super().__post_init__(*args, **kwargs)
        # ObjectIdentityMixin.__init__(self)

        # Paradigm-specific hook for subclasses
        self._after_backend_init(**kwargs)

    def _after_backend_init(self, **kwargs: Any) -> None:
        """
        Optional lifecycle hook for specialized mixins (e.g., ORM autosave, queue verification).
        The default implementation is a no-op.
        """
        pass

    @classmethod
    @asynccontextmanager
    async def change_backend(
        cls: Type[T],
        storage_backend: "KeyValueStoreBackendBase",
    ) -> AsyncGenerator[Type[T], None]:
        """ "Temporarily or permanently switch the class-level storage backend.

        Acts as a thread-safe context manager. Inside the ``with`` block,
        all operations on this class (and its instances) use the provided
        ``storage_backend``.

        Upon exiting the context block, the original backend store and config
        are atomically restored, even if an exception was raised inside the block.

        Usage:
            >>> mock_backend = RedisStoreBackend(host="mock-host")
            >>> with User.change_backend(mock_backend):
            ...     user = User.get("user_123")  # Uses mock_backend
            ...
            >>> user = User.get("user_123")  # Automatically restored to real_backend
        """
        async with cls._backend_lock:
            previous_store = cls._backend_store
            previous_initialized = cls._backend_initialized
            try:
                cls._backend_store = storage_backend
                cls._backend_initialized = True
                yield cls
            finally:
                cls._backend_store = previous_store
                cls._backend_initialized = previous_initialized

    def __getstate__(self) -> Dict[str, Any]:
        """Prepare an object for serialization.

        Returns:
            Dictionary representation of the object state.

        Raises:
            SerializationError: If the object cannot be serialized.
        """
        try:
            state = self.get_state()
        except NotImplementedError:
            raise SerializationError(
                f"Cannot serialise object of type {self.__class__.__name__!r}"
            )

        if hasattr(self, "_id"):
            state["id"] = self._id

        if hasattr(self, "_backend_store") and self._backend_store is not None:
            state["_backend_class"] = get_obj_klass_import_str(self._backend_store)

        # Remove non-serializable attributes
        state.pop("_backend_store", None)
        state.pop("_backend_config", None)

        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Restore the object state after deserialization.

        Args:
            state: Dictionary containing object state.

        Raises:
            SerializationError: If the object cannot be deserialized.
        """
        # Remove backend class info (will be reinitialized)
        state.pop("_backend_class", None)

        try:
            self.set_state(state)
        except NotImplementedError:
            raise SerializationError(
                f"Cannot deserialise object of type {self.__class__.__name__!r}"
            )

        # The backend is not initialised here: _initialize_backend() is async and
        # __setstate__ is sync. get_backend() initialises lazily on first use.

    @classmethod
    def get_volnux_config(cls) -> "VolnuxConfig":
        """Get the global VolnuxConfig instance."""
        from volnux.config import VolnuxConfig

        return VolnuxConfig.get_instance()

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        """Get the backend configuration for this class from VolnuxConfig."""
        return cls.get_volnux_config().KEY_VALUE_STORE_CONFIG

    @classmethod
    async def _initialize_backend(cls) -> None:
        """Async backend initialization. MUST be called under _backend_lock."""
        try:
            backend_config = cls.get_backend_config()
            cls._backend_config = backend_config

            engine_path: Optional[str] = backend_config.get("ENGINE")
            if not engine_path:
                raise ImproperlyConfigured(
                    f"Backend ENGINE not configured for {cls.__name__}"
                )

            backend_class = import_string(engine_path)
            if not issubclass(backend_class, KeyValueStoreBackendBase):
                raise ImproperlyConfigured(
                    f"{backend_class.__name__} is not a KeyValueStoreBackendBase subclass"
                )

            connector_config = backend_config.get("CONNECTOR_CONFIG", {})
            cls._backend_store = backend_class(**connector_config)

            if hasattr(cls._backend_store, "connector"):
                connector = cls._backend_store.connector
                if hasattr(connector, "connect"):
                    is_connected: Optional[Callable[[], bool]] = getattr(
                        connector, "is_connected", None
                    )
                    if (
                        is_connected is None or not is_connected()
                    ):  # type: Optional[Callable[[], bool]]
                        await to_thread(connector.connect)

            cls._backend_initialized = True
            logger.info(
                "Initialized backend %s for %s",
                backend_class.__name__,
                cls.__name__,
            )
        except Exception as e:
            logger.error("Failed to initialize backend for %s: %s", cls.__name__, e)
            raise ImproperlyConfigured(f"Backend init failed: {e}") from e

    @classmethod
    async def get_backend(cls) -> "KeyValueStoreBackendBase":
        """Get or initialize the shared backend store instance."""
        if cls._backend_initialized:
            return cls._backend_store  # type: ignore[return-value]

        async with cls._backend_lock:
            # Double-checked locking under async lock
            if cls._backend_initialized:
                return cls._backend_store  # type: ignore[return-value]

            await cls._initialize_backend()

        return cls._backend_store  # type: ignore[return-value]

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        """Get the StorageRoute definition for this class."""
        return StorageRoute(components=["volnux", cls.__name__])

    @classmethod
    async def get_schema_name(cls) -> str:
        """Get the resolved schema name/channel for this class."""
        backend = await cls.get_backend()
        return cls.get_storage_route().resolve(backend)

    @classmethod
    async def close_backend(cls) -> None:
        """Close backend connections upon application shutdown."""
        async with cls._backend_lock:
            if not cls._backend_initialized:
                return

            try:
                if cls._backend_store is not None:
                    if hasattr(cls._backend_store, "close"):
                        await to_thread(cls._backend_store.close)
                    elif hasattr(cls._backend_store, "connector"):
                        connector = cls._backend_store.connector
                        if hasattr(connector, "disconnect"):
                            await to_thread(connector.disconnect)
                    logger.info("Closed backend for %s", cls.__name__)
            except Exception as e:
                logger.warning("Error closing backend for %s: %s", cls.__name__, e)
            finally:
                cls._backend_store = None
                cls._backend_config = None
                cls._backend_initialized = False
