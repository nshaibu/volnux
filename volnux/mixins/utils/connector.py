import logging
import threading
import time
import typing
from dataclasses import dataclass
from enum import Enum
from functools import wraps

logger = logging.getLogger(__name__)


class ConnectionMode(Enum):
    SINGLE = "single"
    POOLED = "pooled"


@dataclass
class ConnectionStats:
    created_at: float
    last_used_at: float
    use_count: int = 0


class ConnectorManagerFactory:
    """Factory for creating appropriate connector managers based on backend type."""

    @staticmethod
    def create_manager(
        connector_class: typing.Type[typing.Any],
        connector_config: typing.Dict[str, typing.Any],
        connection_mode: typing.Optional[ConnectionMode] = None,
        **kwargs: typing.Dict[str, typing.Any],
    ) -> "BaseConnectorManager":
        """
        Create a connector manager based on the specified connection mode.

        Args:
            connector_class: The class to instantiate for connections
            connector_config: Configuration for the connector
            connection_mode: The connection management mode
            **kwargs: Additional configuration parameters

        Returns:
            A connector manager instance
        """
        # Auto-detect mode if not specified
        if connection_mode is None:
            # Check if connector might benefit from pooling
            connection_mode = ConnectorManagerFactory._detect_connection_mode(
                connector_class, connector_config
            )

        if connection_mode == ConnectionMode.POOLED:
            return PooledConnectorManager(connector_class, connector_config, **kwargs)
        else:
            return SingleConnectorManager(connector_class, connector_config)

    @staticmethod
    def _detect_connection_mode(
        connector_class: typing.Type[typing.Any],
        connector_config: typing.Dict[str, typing.Any],
    ) -> ConnectionMode:
        """
        Detect the appropriate connection mode based on the connector class.

        Args:
            connector_class: The connector class
            connector_config: The connector configuration

        Returns:
            The recommended ConnectionMode
        """
        # Check for known classes or traits that would benefit from pooling
        pooling_indicators = [
            # Class names often associated with backends that benefit from pooling
            any(
                name in connector_class.__name__.lower()
                for name in [
                    "redis",
                    "mysql",
                    "postgres",
                    "sql",
                    "mongo",
                    "elasticsearch",
                ]
            ),
            # Check for connection pool related attributes/methods
            hasattr(connector_class, "connection_pool"),
            hasattr(connector_class, "get_connection"),
            # Check if the connector is likely to be thread-safe
            getattr(connector_class, "thread_safe", False),
        ]

        # If any indicators suggest pooling would be beneficial, use POOLED mode
        if any(pooling_indicators):
            return ConnectionMode.POOLED

        return ConnectionMode.SINGLE


class BaseConnectorManager:
    """Base class for connector managers."""

    def __init__(
        self,
        connector_class: typing.Type[typing.Any],
        connector_config: typing.Dict[str, typing.Any],
    ) -> None:
        """
        Initialize the connector manager.
        Args:
            connector_class: The class to instantiate for connections
            connector_config: Configuration for the connector
        """
        self.connector_class = connector_class
        self.connector_config = connector_config

    def get_connection(self) -> object:
        """Get a connection."""
        raise NotImplementedError

    def release_connection(self, connection: object) -> None:
        """Release a connection."""
        raise NotImplementedError

    def shutdown(self) -> None:
        """Shut down the connector manager."""
        raise NotImplementedError

    def _create_connection(self) -> object:
        """
        Create a new connection instance.
        Returns:
            A new connection
        Raises:
            ConnectionError: If the connection cannot be created
        """
        try:
            connection = self.connector_class(**self.connector_config)
            return connection
        except Exception as e:
            raise ConnectionError(f"Failed to create connection: {e}") from e

    def _check_connection_health(self, connection: object) -> bool:
        """
        Check if a connection is healthy.
        Args:
            connection: The connection to check
        Returns:
            True if the connection is healthy, False otherwise
        """
        try:
            if hasattr(connection, "is_connected"):
                return connection.is_connected()
            elif hasattr(connection, "_check_connection"):
                connection._check_connection()
                return True
            return True  # Assume healthy if we can't check
        except Exception:
            return False

    def _close_connection(self, connection: object) -> None:
        """
        Close a connection.
        Args:
            connection: The connection to close
        """
        try:
            if hasattr(connection, "close"):
                connection.close()
            elif hasattr(connection, "disconnect"):
                connection.disconnect()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")


class SingleConnectorManager(BaseConnectorManager):
    """
    Manages a single shared connection to a backend store.
    Suitable for backends that don't need or support multiple connections.
    """

    def __init__(
        self,
        connector_class: typing.Type[typing.Any],
        connector_config: typing.Dict[str, typing.Any],
    ) -> None:
        """
        Initialize the single connector manager.
        Args:
            connector_class: The class to instantiate for the connection
            connector_config: Configuration for the connector
        """
        super().__init__(connector_class, connector_config)
        self._connection = None
        self._connection_lock = threading.RLock()
        self._connection_stats = None

    def get_connection(self) -> object:
        """
        Get the shared connection, creating it if needed.
        Returns:
            The shared connection
        Raises:
            ConnectionError: If the connection cannot be created or is unhealthy
        """
        with self._connection_lock:
            # Create connection if it doesn't exist
            if self._connection is None:
                self._connection = self._create_connection()
                self._connection_stats = ConnectionStats(
                    created_at=time.time(), last_used_at=time.time()
                )
                return self._connection

            # Check if existing connection is healthy
            if not self._check_connection_health(self._connection):
                logger.warning("Connection is unhealthy, recreating...")
                try:
                    self._close_connection(self._connection)
                except Exception as e:
                    logger.warning(f"Error closing unhealthy connection: {e}")

                self._connection = self._create_connection()
                self._connection_stats = ConnectionStats(
                    created_at=time.time(), last_used_at=time.time()
                )

            # Update stats and return the connection
            if self._connection_stats:
                self._connection_stats.last_used_at = time.time()
                self._connection_stats.use_count += 1

            return self._connection

    def release_connection(self, connection):
        """
        With single connection manager, releasing is a no-op.
        Args:
            connection: The connection that was used
        """
        # Nothing to do for single connection mode
        pass

    def shutdown(self):
        """Close the shared connection."""
        with self._connection_lock:
            if self._connection is not None:
                self._close_connection(self._connection)
                self._connection = None
                self._connection_stats = None


class PooledConnectorManager(BaseConnectorManager):
    """
    Manages a pool of connections to a backend store.
    Suitable for backends that benefit from connection pooling.
    """

    def __init__(
        self,
        connector_class,
        connector_config: dict,
        max_connections: int = 10,
        connection_timeout: int = 30,
        idle_timeout: int = 300,
        retry_attempts: int = 3,
        retry_delay: int = 1,
    ):
        """
        Initialize the pooled connector manager.
        Args:
            connector_class: The class to instantiate for connections
            connector_config: Configuration for the connector
            max_connections: Maximum number of connections to maintain
            connection_timeout: Timeout in seconds when waiting for a connection
            idle_timeout: Time in seconds after which an idle connection is recycled
            retry_attempts: Number of times to retry failed operations
            retry_delay: Delay in seconds between retry attempts
        """
        super().__init__(connector_class, connector_config)
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.idle_timeout = idle_timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

        # Connection pool management
        self._connection_pool = {}  # {connection_id: connection}
        self._connection_stats = {}  # {connection_id: ConnectionStats}
        self._available_connections = set()  # Set of available connection IDs
        self._pool_lock = threading.RLock()
        self._connection_semaphore = threading.BoundedSemaphore(max_connections)

        # Connection health check and cleanup
        self._cleanup_interval = max(60, idle_timeout // 3)
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_idle_connections,
            daemon=True,
            name="ConnectionManagerCleanup",
        )
        self._running = True
        self._cleanup_thread.start()

        logger.info(
            f"Pooled connector manager initialized with max {max_connections} connections"
        )

    def __del__(self):
        """Clean up resources when the manager is deleted."""
        self.shutdown()

    def get_connection(self):
        """
        Get a connection from the pool or create a new one if needed.
        Returns:
            A connection to the backend store
        Raises:
            ConnectionError: If unable to acquire a connection within the timeout
        """
        if not self._connection_semaphore.acquire(timeout=self.connection_timeout):
            raise ConnectionError(
                f"Failed to acquire connection within {self.connection_timeout}s"
            )

        try:
            connection = self._get_available_connection()
            if connection is not None:
                return connection

            # No available connections, create a new one
            return self._create_new_connection()
        except Exception as e:
            # Release the semaphore if we couldn't get a connection
            self._connection_semaphore.release()
            raise ConnectionError(f"Error obtaining connection: {e}") from e

    def release_connection(self, connection):
        """
        Release a connection back to the pool.
        Args:
            connection: The connection to release
        """
        with self._pool_lock:
            conn_id = id(connection)
            if conn_id in self._connection_stats:
                self._connection_stats[conn_id].last_used_at = time.time()
                self._connection_stats[conn_id].use_count += 1

                # Make the connection available again
                self._available_connections.add(conn_id)
            else:
                # This is a connection we don't recognize, close it
                self._close_connection(connection)

        # Always release the semaphore
        self._connection_semaphore.release()

    def execute_with_retry(self, method, *args, **kwargs):
        """
        Execute a function with a connection, with automatic retries.
        Args:
            method: The function to execute, which will be passed a connection as first arg
            *args: Additional arguments to pass to the function
            **kwargs: Additional keyword arguments to pass to the function

        Returns:
            The result of the function execution

        Raises:
            The last exception encountered if all retries fail
        """
        last_exception = None
        for attempt in range(1, self.retry_attempts + 1):
            connector = None
            try:
                connector = self.get_connection()
                result = method(*args, connector=connector, **kwargs)
                return result
            except Exception as e:
                last_exception = e
                logger.warning(f"Attempt {attempt}/{self.retry_attempts} failed: {e}")
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay)
            finally:
                if connector is not None:
                    self.release_connection(connector)

        if last_exception:
            raise last_exception

    def shutdown(self):
        """Shut down the connector manager and close all connections."""
        if hasattr(self, "_running") and self._running:
            self._running = False
            if hasattr(self, "_cleanup_thread") and self._cleanup_thread.is_alive():
                self._cleanup_thread.join(timeout=5)

            with self._pool_lock:
                for conn_id, conn in list(self._connection_pool.items()):
                    self._close_connection(conn)
                self._connection_pool.clear()
                self._connection_stats.clear()
                self._available_connections.clear()

            logger.info("Pooled connector manager shut down")

    def _get_available_connection(self):
        """
        Get an available connection from the pool.

        Returns:
            A connection if one is available, None otherwise
        """
        with self._pool_lock:
            if not self._available_connections:
                return None

            conn_id = self._available_connections.pop()
            connection = self._connection_pool.get(conn_id)

            # Check if the connection is healthy
            if connection and self._check_connection_health(connection):
                return connection

            # Connection is not healthy, remove it
            if conn_id in self._connection_pool:
                if connection:
                    self._close_connection(connection)
                del self._connection_pool[conn_id]
                if conn_id in self._connection_stats:
                    del self._connection_stats[conn_id]

            return None

    def _create_new_connection(self):
        """
        Create a new connection and add it to the pool.

        Returns:
            A new connection
        """
        connection = self._create_connection()

        with self._pool_lock:
            conn_id = id(connection)
            self._connection_pool[conn_id] = connection
            self._connection_stats[conn_id] = ConnectionStats(
                created_at=time.time(), last_used_at=time.time()
            )

        return connection

    def _cleanup_idle_connections(self):
        """
        Periodically clean up idle connections that exceed the idle timeout.
        This method runs in a background thread.
        """
        while self._running:
            try:
                time.sleep(self._cleanup_interval)
                if not self._running:
                    break

                now = time.time()
                with self._pool_lock:
                    # Find idle connections
                    idle_connections = []
                    for conn_id, stats in self._connection_stats.items():
                        if (now - stats.last_used_at) > self.idle_timeout:
                            idle_connections.append(conn_id)

                    # Close and remove idle connections
                    for conn_id in idle_connections:
                        if conn_id in self._connection_pool:
                            connection = self._connection_pool[conn_id]
                            self._close_connection(connection)
                            del self._connection_pool[conn_id]
                            del self._connection_stats[conn_id]
                            self._available_connections.discard(conn_id)

            except Exception as e:
                logger.error(f"Error in connection cleanup: {e}")


def connector_action_register(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        return self.with_connection(method, *args, **kwargs)

    return wrapper
