"""
Backend Connector Interface.

This module provides the abstract base class for implementing backend database
and service connectors with connection pooling, health checks, and robust
resource management.
"""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, TypeVar, Generic
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

CursorType = TypeVar("CursorType")


@dataclass
class ConnectionConfig:
    """Configuration for backend connections.

    Attributes:
        host: The hostname or IP address of the backend server.
        port: The port number for the connection.
        username: Optional username for authentication.
        password: Optional password for authentication.
        database: Optional database/schema name to connect to.
        timeout: Connection timeout in seconds (default: 30).
        pool_size: Maximum number of pooled connections (default: 5).
        ssl_enabled: Whether to use SSL/TLS for the connection.
        extra_params: Additional backend-specific connection parameters.
    """

    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    timeout: int = 30
    pool_size: int = 5
    ssl_enabled: bool = False
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def get_connection_string(self, scheme: str = "unknown") -> str:
        """Generate a connection string URI.

        Args:
            scheme: The URI scheme (e.g., 'postgresql', 'mysql', 'mongodb').

        Returns:
            A formatted connection string with credentials properly encoded.
        """
        auth = ""
        if self.username:
            auth = quote_plus(self.username)
            if self.password:
                auth += f":{quote_plus(self.password)}"
            auth += "@"

        db_path = f"/{self.database}" if self.database else ""
        return f"{scheme}://{auth}{self.host}:{self.port}{db_path}"

    def to_dict(self, include_credentials: bool = False) -> Dict[str, Any]:
        """Convert configuration to dictionary.

        Args:
            include_credentials: Whether to include sensitive credentials.

        Returns:
            Dictionary representation of the configuration.
        """
        config = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "timeout": self.timeout,
            "pool_size": self.pool_size,
            "ssl_enabled": self.ssl_enabled,
            **self.extra_params,
        }

        if include_credentials:
            config["username"] = self.username
            config["password"] = self.password

        return config


class ConnectionError(Exception):
    """Raised when connection operations fail."""

    pass


class BackendConnectorBase(ABC, Generic[CursorType]):
    """Abstract base class for backend database and service connectors.

    This class provides a standard interface for connecting to various backend
    services with support for connection pooling, health checks, and proper
    resource management.

    Type Parameters:
        CursorType: The type of cursor object returned by this connector.

    Attributes:
        config: The connection configuration object.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        timeout: int = 30,
        **kwargs: Any,
    ):
        """Initialize the backend connector.

        Args:
            host: The hostname or IP address of the backend server.
            port: The port number for the connection.
            username: Optional username for authentication.
            password: Optional password for authentication.
            database: Optional database/schema name to connect to.
            timeout: Connection timeout in seconds.
            **kwargs: Additional backend-specific configuration parameters.
        """
        self.config = ConnectionConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            timeout=timeout,
            extra_params=kwargs,
        )
        self._connection: Optional[Any] = None
        self._cursor: Optional[CursorType] = None
        self._is_connected: bool = False

        logger.debug(
            f"Initialized {self.__class__.__name__} connector for "
            f"{host}:{port}/{database or 'default'}"
        )

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the backend.

        This method should set self._connection and self._is_connected.
        Implementations should handle connection pooling if applicable.

        Raises:
            ConnectionError: If the connection cannot be established.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the backend.

        This method should safely close connections and cursors,
        handling any exceptions gracefully. It should be idempotent.
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connector is currently connected.

        Returns:
            True if connected and the connection is healthy, False otherwise.
        """
        pass

    @abstractmethod
    def ping(self) -> bool:
        """Test if the connection is alive and responsive.

        Returns:
            True if the connection responds successfully, False otherwise.
        """
        pass

    def ensure_connected(self) -> None:
        """Ensure the connection is active, reconnecting if necessary.

        Raises:
            ConnectionError: If connection cannot be established.
        """
        if not self.is_connected():
            logger.info(f"Reconnecting to {self.config.host}:{self.config.port}")
            self.connect()

    @property
    def cursor(self) -> Optional[CursorType]:
        """Get the current cursor object.

        Returns:
            The active cursor, or None if no cursor exists.
        """
        return self._cursor

    @abstractmethod
    def get_cursor(self) -> CursorType:
        """Create and return a new cursor.

        Returns:
            A new cursor object for executing queries.

        Raises:
            ConnectionError: If not connected or cursor creation fails.
        """
        pass

    @contextmanager
    def cursor_context(self):
        """Context manager for cursor lifecycle management.

        Yields:
            A cursor object that is automatically closed after use.

        Example:
            >>> with connector.cursor_context() as cursor:
            ...     cursor.execute("SELECT * FROM users")
        """
        cursor = None
        try:
            cursor = self.get_cursor()
            yield cursor
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")

    @abstractmethod
    def begin_transaction(self) -> None:
        """Begin a new transaction.

        Raises:
            ConnectionError: If not connected or transaction cannot start.
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            ConnectionError: If not connected or commit fails.
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Roll back the current transaction.

        Raises:
            ConnectionError: If not connected or rollback fails.
        """
        pass

    @contextmanager
    def transaction(self):
        """Context manager for automatic transaction handling.

        Commits on successful completion, rolls back on exception.

        Example:
            >>> with connector.transaction():
            ...     cursor.execute("INSERT INTO users ...")
            ...     cursor.execute("UPDATE accounts ...")
        """
        try:
            self.begin_transaction()
            yield
            self.commit()
        except Exception as e:
            logger.error(f"Transaction failed, rolling back: {e}")
            self.rollback()
            raise

    def get_connection_info(self) -> Dict[str, Any]:
        """Get current connection information.

        Returns:
            Dictionary containing connection status and metadata.
        """
        return {
            "connected": self.is_connected(),
            "host": self.config.host,
            "port": self.config.port,
            "database": self.config.database,
            "connector_type": self.__class__.__name__,
        }

    def __enter__(self) -> "BackendConnectorBase":
        """Context manager entry: establish connection.

        Returns:
            Self for use in the with block.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit: close connection.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        self.disconnect()

    def __del__(self) -> None:
        """Destructor: ensure connection is closed."""
        try:
            if self._is_connected:
                self.disconnect()
        except Exception as e:
            logger.debug(f"Error during connector cleanup: {e}")

    def __repr__(self) -> str:
        """String representation of the connector."""
        status = "connected" if self._is_connected else "disconnected"
        return (
            f"<{self.__class__.__name__} "
            f"{self.config.host}:{self.config.port} "
            f"[{status}]>"
        )
