import logging
import pickle
import typing
from functools import partial

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.conf import ConfigLoader
from volnux.exceptions import ObjectExistError, StopProcessingError
from volnux.import_utils import import_string
from volnux.mixins.identity import ObjectIdentityMixin
from volnux.mixins.utils.connector import (
    BaseConnectorManager,
    ConnectionMode,
    ConnectorManagerFactory,
    connector_action_register,
)
from volnux.utils import get_obj_klass_import_str

logger = logging.getLogger(__name__)

CONFIG = ConfigLoader.get_lazily_loaded_config()


class BackendIntegrationMixin(ObjectIdentityMixin):
    _connector_manager: typing.ClassVar[typing.Optional[BaseConnectorManager]] = None

    def __model_init__(self) -> None:
        ObjectIdentityMixin.__init__(self)

        self._backend: typing.Optional[typing.Type[KeyValueStoreBackendBase]] = None

        backend_config = CONFIG.RESULT_BACKEND_CONFIG
        connector_config = backend_config.get("CONNECTOR_CONFIG", {})
        try:
            self._backend = import_string(backend_config["ENGINE"])
        except Exception as e:
            logger.error(f"Error importing backend {backend_config}: {e}")
            raise StopProcessingError(
                f"Error importing backend {backend_config}: {e}"
            ) from e

        try:
            # Initialize the connector manager if not already created
            if self._connector_manager is None:
                # Get connection mode from config or auto-detect
                connection_mode_str = backend_config.get("CONNECTION_MODE", "auto")
                connection_mode = None
                if connection_mode_str != "auto":
                    connection_mode = ConnectionMode(connection_mode_str)

                # Create appropriate connector manager
                self.__class__._connector_manager = (
                    ConnectorManagerFactory.create_manager(
                        connector_class=self._backend,
                        connector_config=connector_config,
                        connection_mode=connection_mode,
                        max_connections=backend_config.get("MAX_CONNECTIONS", 10),
                        connection_timeout=backend_config.get("CONNECTION_TIMEOUT", 30),
                        idle_timeout=backend_config.get("IDLE_TIMEOUT", 300),
                    )
                )

            self.release_connection(self._connector_manager.get_connection())
        except Exception as e:
            logger.error(f"Failed to initialize backend connector: {e}")
            raise StopProcessingError(
                f"Failed to initialize backend connector: {e}"
            ) from e

        self.save()

    def with_connection(
        self,
        method: typing.Callable,
        *args: typing.Tuple[typing.Any],
        **kwargs: typing.Dict[str, typing.Any],
    ) -> typing.Any:
        """
        Execute a function with a connection from the manager.
        Args:
            method: The function to execute, which will be passed a connection as first arg
            *args: Additional arguments to pass to the function
            **kwargs: Additional keyword arguments to pass to the function
        Returns:
            The result of the function execution
        """
        # For pooled managers that support it, use the retry mechanism
        if hasattr(self._connector_manager, "execute_with_retry"):
            return self._connector_manager.execute_with_retry(
                partial(method, self), *args, **kwargs
            )

        # For single connection managers, just get the connection and execute
        connector = self.get_connection()
        try:
            return method(self, *args, connector=connector, **kwargs)
        finally:
            self.release_connection(connector)

    def get_connection(self):
        """
        Get a connection from the manager.
        Returns:
            A connection
        """
        return self._connector_manager.get_connection()

    def release_connection(self, connection) -> None:
        """
        Release a connection back to the manager.
        Args:
            connection: The connection to release
        """
        self._connector_manager.release_connection(connection)

    def __getstate__(self) -> typing.Dict[str, typing.Any]:
        """
        Prepare object for pickling by removing the lock.
        """
        try:
            state = self.get_state()
        except NotImplementedError:
            raise pickle.PickleError(
                "Cannot pickle object of type {}".format(self.__class__)
            )
        backend = state.pop("_backend", self._backend)
        if backend is not None:
            state["_backend"] = (
                backend
                if isinstance(backend, str)
                else get_obj_klass_import_str(backend)
            )
        return state

    def __setstate__(self, state: typing.Dict[str, typing.Any]) -> None:
        """
        Restore object state after unpickling and recreate the lock.
        """
        if "_backend" in state:
            backend = state.pop("_backend")
            try:
                self._backend = import_string(backend)
            except Exception as e:
                logger.error(f"Error importing backend {backend}: {e}")
        try:
            self.set_state(state)
        except NotImplementedError:
            raise pickle.UnpicklingError(
                "Cannot restore object of type {}".format(self.__class__)
            )

    def get_schema_name(self) -> str:
        return self.__class__.__name__

    @connector_action_register
    def save(self, connector: KeyValueStoreBackendBase) -> None:
        try:
            connector.insert_record(
                schema_name=self.get_schema_name(), record_key=self.id, record=self
            )
        except ObjectExistError:
            connector.update_record(
                schema_name=self.get_schema_name(), record_key=self.id, record=self
            )

    @connector_action_register
    def reload(self, connector: KeyValueStoreBackendBase) -> None:
        connector.reload_record(self.get_schema_name(), self)

    @connector_action_register
    def delete(self, connector: KeyValueStoreBackendBase) -> None:
        connector.delete_record(schema_name=self.get_schema_name(), record_key=self.id)

    @connector_action_register
    def update(self, connector: KeyValueStoreBackendBase) -> None:
        connector.update_record(
            schema_name=self.get_schema_name(), record_key=self.id, record=self
        )

    # def __del__(self):
    #     if getattr(self, "_connector", None) is not None:
    #         self.connector.close()
