import logging
from typing import Any, Dict, List, Optional, Union

from pymemcache.client.base import Client, PooledClient
from pymemcache.exceptions import (
    MemcacheError,
    MemcacheClientError,
    MemcacheServerError,
    MemcacheUnknownCommandError,
    MemcacheIllegalInputError,
)

from volnux.backends.connection import BackendConnectorBase, ConnectionError

logger = logging.getLogger(__name__)


class MemcacheConnector(BackendConnectorBase[Client]):
    """Memcache backend connector with connection pooling and health monitoring.

    This connector provides a robust interface to Memcache with automatic
    reconnection, connection pooling, serialization support, and comprehensive
    error handling.

    Example:
        >>> connector = MemcacheConnector(
        ...     host="localhost",
        ...     port=11211,
        ...     pool_size=10,
        ...     enable_compression=True
        ... )
        >>> with connector:
        ...     connector.cursor.set("key", "value", expire=3600)
        ...     value = connector.cursor.get("key")
    """

    DEFAULT_PORT = 11211
    DEFAULT_TIMEOUT = 3.0
    DEFAULT_CONNECT_TIMEOUT = 3.0
    DEFAULT_POOL_SIZE = 10

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_PORT,
        timeout: float = DEFAULT_TIMEOUT,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        pool_size: int = DEFAULT_POOL_SIZE,
        use_pooling: bool = True,
        enable_compression: bool = False,
        compression_threshold: int = 1024,
        serializer: Optional[Any] = None,
        deserializer: Optional[Any] = None,
        key_prefix: str = "",
        no_delay: bool = True,
        **kwargs: Any,
    ):
        """Initialize the Memcache connector.

        Args:
            host: Memcache server hostname or IP address.
            port: Memcache server port (default: 11211).
            timeout: Socket timeout in seconds for read/write operations.
            connect_timeout: Timeout for establishing connections.
            pool_size: Maximum number of connections in the pool.
            use_pooling: Enable connection pooling (recommended for production).
            enable_compression: Enable automatic compression for large values.
            compression_threshold: Minimum size in bytes to trigger compression.
            serializer: Custom serializer function (default: pickle).
            deserializer: Custom deserializer function (default: pickle).
            key_prefix: Prefix to add to all keys (useful for namespacing).
            no_delay: Disable Nagle's algorithm for better latency.
            **kwargs: Additional Memcache connection parameters.
        """
        super().__init__(
            host=host,
            port=port,
            timeout=int(timeout),
            pool_size=pool_size,
            connect_timeout=connect_timeout,
            use_pooling=use_pooling,
            enable_compression=enable_compression,
            compression_threshold=compression_threshold,
            key_prefix=key_prefix,
            no_delay=no_delay,
            **kwargs,
        )

        self._timeout = timeout
        self._connect_timeout = connect_timeout
        self._use_pooling = use_pooling
        self._enable_compression = enable_compression
        self._compression_threshold = compression_threshold
        self._serializer = serializer
        self._deserializer = deserializer
        self._key_prefix = key_prefix
        self._no_delay = no_delay
        self._client: Optional[Union[Client, PooledClient]] = None

    def _get_client_params(self) -> Dict[str, Any]:
        """Build client parameters dictionary.

        Returns:
            Dictionary of Memcache client parameters.
        """
        params = {
            "server": (self.config.host, self.config.port),
            "timeout": self._timeout,
            "connect_timeout": self._connect_timeout,
            "no_delay": self._no_delay,
            "key_prefix": self._key_prefix.encode() if self._key_prefix else b"",
        }

        # Add serialization if configured
        if self._serializer:
            params["serializer"] = self._serializer
        if self._deserializer:
            params["deserializer"] = self._deserializer

        # Add compression settings
        if self._enable_compression:
            import zlib

            def compressor(key, value):
                if len(value) >= self._compression_threshold:
                    return zlib.compress(value), 1
                return value, 0

            def decompressor(key, value, flags):
                if flags == 1:
                    return zlib.decompress(value)
                return value

            params["compressor"] = compressor
            params["decompressor"] = decompressor

        for key, value in self.config.extra_params.items():
            if key not in params and key not in [
                "use_pooling",
                "enable_compression",
                "compression_threshold",
                "key_prefix",
            ]:
                params[key] = value

        return params

    def connect(self) -> None:
        """Establish connection to Memcache server.

        Raises:
            ConnectionError: If connection cannot be established.
        """
        if self._is_connected:
            logger.debug("Already connected to Memcache")
            return

        try:
            params = self._get_client_params()

            # Create pooled or single client
            if self._use_pooling:
                self._client = PooledClient(
                    **params, max_pool_size=self.config.pool_size
                )
                logger.info(
                    f"Created Memcache connection pool for {self.config.host}:{self.config.port} "
                    f"(pool_size={self.config.pool_size})"
                )
            else:
                self._client = Client(**params)
                logger.info(
                    f"Created Memcache client for {self.config.host}:{self.config.port}"
                )

            # Test the connection
            self._client.version()

            self._cursor = self._client
            self._connection = self._client
            self._is_connected = True

            logger.info(
                f"Successfully connected to Memcache at {self.config.host}:{self.config.port}"
            )

        except MemcacheClientError as e:
            logger.error(f"Memcache client error: {e}")
            raise ConnectionError(f"Client error: {e}")
        except MemcacheServerError as e:
            logger.error(f"Memcache server error: {e}")
            raise ConnectionError(f"Server error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error connecting to Memcache: {e}")
            raise ConnectionError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        """Close the Memcache connection and clean up resources.

        This method is idempotent and safe to call multiple times.
        """
        if not self._is_connected and self._client is None:
            return

        try:
            if self._client is not None:
                self._client.close()
                logger.debug("Closed Memcache client connection")

            self._client = None
            self._cursor = None
            self._connection = None
            self._is_connected = False

            logger.info(
                f"Disconnected from Memcache at {self.config.host}:{self.config.port}"
            )
        except Exception as e:
            logger.warning(f"Error during Memcache disconnect: {e}")
            # Ensure state is reset even if close fails
            self._client = None
            self._cursor = None
            self._connection = None
            self._is_connected = False

    def is_connected(self) -> bool:
        """Check if the connector is currently connected to Memcache.

        Returns:
            True if connected and connection is healthy, False otherwise.
        """
        if not self._is_connected or self._client is None:
            return False

        try:
            # Test connection with version command
            self._client.version()
            return True
        except (MemcacheError, OSError, AttributeError) as e:
            logger.debug(f"Connection check failed: {e}")
            self._is_connected = False
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking connection: {e}")
            self._is_connected = False
            return False

    def ping(self) -> bool:
        """Test if the Memcache connection is alive and responsive.

        Returns:
            True if the connection responds successfully, False otherwise.
        """
        return self.is_connected()

    def get_cursor(self) -> Client:
        """Get the Memcache client for executing commands.

        Returns:
            The Memcache client object.

        Raises:
            ConnectionError: If not connected.
        """
        if not self._is_connected or self._client is None:
            raise ConnectionError("Not connected to Memcache. Call connect() first.")

        return self._client

    # Transaction Support (Memcache has limited transaction support)

    def begin_transaction(self) -> None:
        """Begin a transaction (no-op for Memcache).

        Note: Memcache doesn't support traditional transactions.
        Use cas (check-and-set) operations for atomic updates.
        """
        self.ensure_connected()
        logger.debug("Memcache doesn't support transactions; use CAS operations")

    def commit(self) -> None:
        """Commit a transaction (no-op for Memcache).

        Note: Memcache operations are immediately committed.
        """
        logger.debug("Memcache operations are auto-committed")

    def rollback(self) -> None:
        """Rollback a transaction (no-op for Memcache).

        Note: Memcache doesn't support rollback.
        """
        logger.debug("Memcache doesn't support rollback")

    # Memcache-Specific Features

    def flush_all(self, delay: int = 0) -> bool:
        """Flush all items from all Memcache servers.

        Args:
            delay: Optional delay in seconds before flushing.

        Returns:
            True if successful.

        Raises:
            ConnectionError: If not connected or flush fails.
        """
        self.ensure_connected()

        try:
            result = self._client.flush_all(delay=delay)
            logger.info(f"Flushed all Memcache data (delay={delay}s)")
            return result
        except MemcacheError as e:
            logger.error(f"Failed to flush Memcache: {e}")
            raise ConnectionError(f"Failed to flush: {e}")

    def get_stats(self) -> Dict[bytes, Dict[bytes, bytes]]:
        """Get server statistics.

        Returns:
            Dictionary containing server statistics.

        Raises:
            ConnectionError: If not connected or stats retrieval fails.
        """
        self.ensure_connected()

        try:
            stats = self._client.stats()
            return stats
        except MemcacheError as e:
            logger.error(f"Failed to get Memcache stats: {e}")
            raise ConnectionError(f"Failed to get stats: {e}")

    def get_version(self) -> bytes:
        """Get Memcache server version.

        Returns:
            Server version string.

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()

        try:
            return self._client.version()
        except MemcacheError as e:
            logger.error(f"Failed to get version: {e}")
            raise ConnectionError(f"Failed to get version: {e}")

    def touch(self, key: Union[str, bytes], expire: int = 0) -> bool:
        """Update the expiration time of an item without fetching it.

        Args:
            key: The key to touch.
            expire: New expiration time in seconds (0 for no expiration).

        Returns:
            True if successful, False if key doesn't exist.

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()

        try:
            return self._client.touch(key, expire=expire)
        except MemcacheError as e:
            logger.error(f"Failed to touch key '{key}': {e}")
            raise ConnectionError(f"Failed to touch key: {e}")

    def increment(
        self, key: Union[str, bytes], value: int = 1, noreply: bool = True
    ) -> Optional[int]:
        """Increment a numeric value.

        Args:
            key: The key to increment.
            value: Amount to increment by.
            noreply: If True, don't wait for response.

        Returns:
            New value if noreply is False, None otherwise.

        Raises:
            ConnectionError: If not connected or increment fails.
        """
        self.ensure_connected()

        try:
            return self._client.incr(key, value, noreply=noreply)
        except MemcacheError as e:
            logger.error(f"Failed to increment key '{key}': {e}")
            raise ConnectionError(f"Failed to increment: {e}")

    def decrement(
        self, key: Union[str, bytes], value: int = 1, noreply: bool = True
    ) -> Optional[int]:
        """Decrement a numeric value.

        Args:
            key: The key to decrement.
            value: Amount to decrement by.
            noreply: If True, don't wait for response.

        Returns:
            New value if noreply is False, None otherwise.

        Raises:
            ConnectionError: If not connected or decrement fails.
        """
        self.ensure_connected()

        try:
            return self._client.decr(key, value, noreply=noreply)
        except MemcacheError as e:
            logger.error(f"Failed to decrement key '{key}': {e}")
            raise ConnectionError(f"Failed to decrement: {e}")

    def get_multi(self, keys: List[Union[str, bytes]]) -> Dict[bytes, Any]:
        """Get multiple keys at once.

        Args:
            keys: List of keys to retrieve.

        Returns:
            Dictionary mapping keys to values.

        Raises:
            ConnectionError: If not connected or get fails.
        """
        self.ensure_connected()

        try:
            return self._client.get_many(keys)
        except MemcacheError as e:
            logger.error(f"Failed to get multiple keys: {e}")
            raise ConnectionError(f"Failed to get keys: {e}")

    def set_multi(
        self,
        mapping: Dict[Union[str, bytes], Any],
        expire: int = 0,
        noreply: bool = True,
    ) -> List[Union[str, bytes]]:
        """Set multiple keys at once.

        Args:
            mapping: Dictionary mapping keys to values.
            expire: Expiration time in seconds.
            noreply: If True, don't wait for response.

        Returns:
            List of keys that failed to be set.

        Raises:
            ConnectionError: If not connected or set fails.
        """
        self.ensure_connected()

        try:
            return self._client.set_many(mapping, expire=expire, noreply=noreply)
        except MemcacheError as e:
            logger.error(f"Failed to set multiple keys: {e}")
            raise ConnectionError(f"Failed to set keys: {e}")

    def delete_multi(self, keys: List[Union[str, bytes]], noreply: bool = True) -> bool:
        """Delete multiple keys at once.

        Args:
            keys: List of keys to delete.
            noreply: If True, don't wait for response.

        Returns:
            True if all deletes succeeded (when noreply is False).

        Raises:
            ConnectionError: If not connected or delete fails.
        """
        self.ensure_connected()

        try:
            return self._client.delete_many(keys, noreply=noreply)
        except MemcacheError as e:
            logger.error(f"Failed to delete multiple keys: {e}")
            raise ConnectionError(f"Failed to delete keys: {e}")

    def get_connection_info(self) -> Dict[str, Any]:
        """Get detailed connection information.

        Returns:
            Dictionary containing connection status and server info.
        """
        info = super().get_connection_info()

        info.update(
            {
                "pooling_enabled": self._use_pooling,
                "compression_enabled": self._enable_compression,
                "key_prefix": self._key_prefix,
            }
        )

        if self._is_connected:
            try:
                version = self.get_version()
                info["memcache_version"] = (
                    version.decode() if isinstance(version, bytes) else version
                )

                # Get basic stats
                stats = self.get_stats()
                if stats:
                    server_stats = next(iter(stats.values()), {})
                    info.update(
                        {
                            "total_items": server_stats.get(
                                b"curr_items", b"0"
                            ).decode(),
                            "total_connections": server_stats.get(
                                b"curr_connections", b"0"
                            ).decode(),
                            "uptime_seconds": server_stats.get(
                                b"uptime", b"0"
                            ).decode(),
                        }
                    )
            except Exception as e:
                logger.warning(f"Could not retrieve Memcache info: {e}")

        return info

    def __repr__(self) -> str:
        """String representation of the Memcache connector."""
        status = "connected" if self._is_connected else "disconnected"
        pool_type = "pooled" if self._use_pooling else "single"
        return (
            f"<MemcacheConnector {self.config.host}:{self.config.port} "
            f"[{pool_type}, {status}]>"
        )
