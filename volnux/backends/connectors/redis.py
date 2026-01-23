import logging
from typing import Any, Optional

import redis
from redis import Redis, ConnectionPool, RedisError
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    AuthenticationError,
)

from volnux.backends.connection import BackendConnectorBase, ConnectionError

logger = logging.getLogger(__name__)


class RedisConnector(BackendConnectorBase[Redis]):
    """Redis backend connector with connection pooling and health monitoring.

    This connector provides a robust interface to Redis with automatic
    reconnection, connection pooling, and comprehensive error handling.

    Example:
        >>> connector = RedisConnector(
        ...     host="localhost",
        ...     port=6379,
        ...     database=0,
        ...     password="secret",
        ...     pool_size=10
        ... )
        >>> with connector:
        ...     connector.cursor.set("key", "value")
        ...     value = connector.cursor.get("key")
    """

    def __init__(
        self,
        host: str,
        port: int = 6379,
        database: int = 0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
        socket_timeout: int = 5,
        socket_connect_timeout: int = 5,
        socket_keepalive: bool = True,
        pool_size: int = 10,
        decode_responses: bool = True,
        ssl_enabled: bool = False,
        **kwargs: Any,
    ):
        """Initialize the Redis connector.

        Args:
            host: Redis server hostname or IP address.
            port: Redis server port (default: 6379).
            database: Redis database number (default: 0).
            username: Optional username for Redis ACL authentication (Redis 6+).
            password: Optional password for authentication.
            timeout: General operation timeout in seconds.
            socket_timeout: Socket timeout in seconds.
            socket_connect_timeout: Connection timeout in seconds.
            socket_keepalive: Enable TCP keepalive.
            pool_size: Maximum number of connections in the pool.
            decode_responses: Automatically decode byte responses to strings.
            ssl_enabled: Use SSL/TLS for connection.
            **kwargs: Additional Redis connection parameters.
        """
        super().__init__(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            timeout=timeout,
            pool_size=pool_size,
            ssl_enabled=ssl_enabled,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            decode_responses=decode_responses,
            **kwargs,
        )

        self._connection_pool: Optional[ConnectionPool] = None
        self._client: Optional[Redis] = None
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize the Redis connection pool."""
        try:
            pool_kwargs = {
                "host": self.config.host,
                "port": self.config.port,
                "db": self.config.database or 0,
                "socket_timeout": self.config.extra_params.get("socket_timeout", 5),
                "socket_connect_timeout": self.config.extra_params.get(
                    "socket_connect_timeout", 5
                ),
                "socket_keepalive": self.config.extra_params.get(
                    "socket_keepalive", True
                ),
                "max_connections": self.config.pool_size,
                "decode_responses": self.config.extra_params.get(
                    "decode_responses", True
                ),
            }

            # Add authentication if provided
            if self.config.username:
                pool_kwargs["username"] = self.config.username
            if self.config.password:
                pool_kwargs["password"] = self.config.password

            # Add SSL configuration if enabled
            if self.config.ssl_enabled:
                pool_kwargs["ssl"] = True
                pool_kwargs["ssl_cert_reqs"] = self.config.extra_params.get(
                    "ssl_cert_reqs", "required"
                )

            # Add any extra parameters
            for key, value in self.config.extra_params.items():
                if key not in pool_kwargs and key not in [
                    "socket_timeout",
                    "socket_connect_timeout",
                    "socket_keepalive",
                    "decode_responses",
                ]:
                    pool_kwargs[key] = value

            self._connection_pool = ConnectionPool(**pool_kwargs)
            logger.info(
                f"Initialized Redis connection pool for {self.config.host}:{self.config.port} "
                f"(db={self.config.database}, pool_size={self.config.pool_size})"
            )
        except Exception as e:
            logger.error(f"Failed to initialize Redis connection pool: {e}")
            raise ConnectionError(f"Failed to initialize connection pool: {e}")

    def connect(self) -> None:
        """Establish connection to Redis server.

        Raises:
            ConnectionError: If connection cannot be established.
        """
        if self._is_connected:
            logger.debug("Already connected to Redis")
            return

        try:
            self._client = Redis(connection_pool=self._connection_pool)
            self._cursor = self._client
            self._connection = self._client

            # Test the connection
            self._client.ping()
            self._is_connected = True

            logger.info(
                f"Successfully connected to Redis at {self.config.host}:{self.config.port} "
                f"(db={self.config.database})"
            )
        except AuthenticationError as e:
            logger.error(f"Redis authentication failed: {e}")
            raise ConnectionError(f"Authentication failed: {e}")
        except RedisConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise ConnectionError(f"Connection failed: {e}")
        except RedisTimeoutError as e:
            logger.error(f"Redis connection timeout: {e}")
            raise ConnectionError(f"Connection timeout: {e}")
        except Exception as e:
            logger.error(f"Unexpected error connecting to Redis: {e}")
            raise ConnectionError(f"Unexpected connection error: {e}")

    def disconnect(self) -> None:
        """
        Close the Redis connection and clean up resources.
        """
        if not self._is_connected and self._client is None:
            return

        try:
            if self._client is not None:
                self._client.close()
                logger.debug("Closed Redis client connection")

            self._client = None
            self._cursor = None
            self._connection = None
            self._is_connected = False

            logger.info(
                f"Disconnected from Redis at {self.config.host}:{self.config.port}"
            )
        except Exception as e:
            logger.warning(f"Error during Redis disconnect: {e}")

            self._client = None
            self._cursor = None
            self._connection = None
            self._is_connected = False

    def is_connected(self) -> bool:
        """Check if the connector is currently connected to Redis.

        Returns:
            True if connected and connection is healthy, False otherwise.
        """
        if not self._is_connected or self._client is None:
            return False

        try:
            return self._client.ping()
        except (RedisConnectionError, RedisTimeoutError, RedisError):
            self._is_connected = False
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking Redis connection: {e}")
            self._is_connected = False
            return False

    def ping(self) -> bool:
        """Test if the Redis connection is alive and responsive.

        Returns:
            True if the connection responds successfully, False otherwise.
        """
        return self.is_connected()

    def get_cursor(self) -> Redis:
        """Get the Redis client for executing commands.

        Returns:
            The Redis client object.

        Raises:
            ConnectionError: If not connected.
        """
        if not self._is_connected or self._client is None:
            raise ConnectionError("Not connected to Redis. Call connect() first.")

        return self._client

    # Transaction Support

    def begin_transaction(self) -> None:
        """Begin a Redis transaction (MULTI).

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()
        # Redis transactions are handled via pipeline
        # Subclasses or users should use get_pipeline() for transactions
        logger.debug("Redis transactions should use pipeline context")

    def commit(self) -> None:
        """Commit a Redis transaction (EXEC).

        Note: Redis transactions are handled via pipelines.
        Use get_pipeline() for proper transaction management.
        """
        logger.debug("Redis transactions are committed via pipeline.execute()")

    def rollback(self) -> None:
        """Rollback a Redis transaction (DISCARD).

        Note: Redis transactions are handled via pipelines.
        Use get_pipeline() for proper transaction management.
        """
        logger.debug("Redis transactions are discarded via pipeline.reset()")

    # Redis-Specific Features

    def get_pipeline(self, transaction: bool = True) -> redis.client.Pipeline:
        """Get a Redis pipeline for batching commands or transactions.

        Args:
            transaction: If True, use MULTI/EXEC for atomic operations.

        Returns:
            A Redis pipeline object.

        Raises:
            ConnectionError: If not connected.

        Example:
            >>> with connector.get_pipeline() as pipe:
            ...     pipe.set("key1", "value1")
            ...     pipe.set("key2", "value2")
            ...     pipe.execute()
        """
        self.ensure_connected()
        return self._client.pipeline(transaction=transaction)

    def get_info(self, section: Optional[str] = None) -> dict:
        """Get Redis server information.

        Args:
            section: Optional section name (e.g., 'server', 'memory', 'stats').

        Returns:
            Dictionary containing Redis server information.

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()
        return self._client.info(section=section)

    def flush_db(self, asynchronous: bool = False) -> bool:
        """Flush the current database.

        Args:
            asynchronous: If True, flush asynchronously (FLUSHDB ASYNC).

        Returns:
            True if successful.

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()
        return self._client.flushdb(asynchronous=asynchronous)

    def flush_all(self, asynchronous: bool = False) -> bool:
        """Flush all databases.

        Args:
            asynchronous: If True, flush asynchronously (FLUSHALL ASYNC).

        Returns:
            True if successful.

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()
        return self._client.flushall(asynchronous=asynchronous)

    def get_connection_info(self) -> dict:
        """Get detailed connection information.

        Returns:
            Dictionary containing connection status and Redis server info.
        """
        info = super().get_connection_info()

        if self._is_connected:
            try:
                redis_info = self.get_info("server")
                info.update(
                    {
                        "redis_version": redis_info.get("redis_version"),
                        "redis_mode": redis_info.get("redis_mode"),
                        "uptime_in_seconds": redis_info.get("uptime_in_seconds"),
                        "connected_clients": self.get_info("clients").get(
                            "connected_clients"
                        ),
                    }
                )
            except Exception as e:
                logger.warning(f"Could not retrieve Redis info: {e}")

        return info

    def __repr__(self) -> str:
        """String representation of the Redis connector."""
        status = "connected" if self._is_connected else "disconnected"
        return (
            f"<RedisConnector {self.config.host}:{self.config.port} "
            f"db={self.config.database} [{status}]>"
        )
