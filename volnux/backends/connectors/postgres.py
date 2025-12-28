import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg
from psycopg import Connection, Cursor
from psycopg.rows import dict_row, tuple_row
from psycopg.errors import (
    OperationalError,
    InterfaceError,
    DatabaseError,
)
from psycopg_pool import ConnectionPool

from volnux.backends.connection import BackendConnectorBase, ConnectionError

logger = logging.getLogger(__name__)


class PostgresConnector(BackendConnectorBase[Cursor]):
    """PostgreSQL backend connector with connection pooling.

    Example:
        >>> connector = PostgresConnector(
        ...     host="localhost",
        ...     port=5432,
        ...     database="mydb",
        ...     username="user",
        ...     password="secret",
        ...     pool_size=10
        ... )
        >>> with connector:
        ...     cursor = connector.get_cursor()
        ...     cursor.execute("SELECT * FROM users WHERE status = %s", ("active",))
        ...     results = cursor.fetchall()
    """

    # PostgreSQL-specific defaults
    DEFAULT_PORT = 5432
    DEFAULT_TIMEOUT = 30
    DEFAULT_POOL_SIZE = 10
    DEFAULT_MIN_POOL_SIZE = 2
    DEFAULT_MAX_POOL_SIZE = 20

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_PORT,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        pool_size: int = DEFAULT_POOL_SIZE,
        min_pool_size: int = DEFAULT_MIN_POOL_SIZE,
        max_pool_size: int = DEFAULT_MAX_POOL_SIZE,
        use_pooling: bool = True,
        row_factory: str = "tuple",  # "tuple", "dict", "namedtuple"
        application_name: str = "volnux",
        ssl_mode: str = "prefer",
        connect_timeout: int = 10,
        keepalives: bool = True,
        keepalives_idle: int = 30,
        keepalives_interval: int = 10,
        keepalives_count: int = 5,
        autocommit: bool = False,
        **kwargs: Any,
    ):
        """Initialize the PostgreSQL connector.

        Args:
            host: PostgreSQL server hostname or IP address.
            port: PostgreSQL server port (default: 5432).
            database: Database name to connect to.
            username: Username for authentication.
            password: Password for authentication.
            timeout: General operation timeout in seconds.
            pool_size: Target number of connections in the pool.
            min_pool_size: Minimum number of connections to maintain.
            max_pool_size: Maximum number of connections allowed.
            use_pooling: Enable connection pooling (recommended for production).
            row_factory: Row format - "tuple", "dict", or "namedtuple".
            application_name: Application name for PostgreSQL monitoring.
            ssl_mode: SSL mode - "disable", "allow", "prefer", "require", "verify-ca", "verify-full".
            connect_timeout: Timeout for establishing connections.
            keepalives: Enable TCP keepalive.
            keepalives_idle: Seconds before sending keepalive probes.
            keepalives_interval: Seconds between keepalive probes.
            keepalives_count: Number of keepalive probes before giving up.
            autocommit: Enable autocommit mode.
            **kwargs: Additional PostgreSQL connection parameters.
        """
        super().__init__(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            timeout=timeout,
            pool_size=pool_size,
            min_pool_size=min_pool_size,
            max_pool_size=max_pool_size,
            use_pooling=use_pooling,
            row_factory=row_factory,
            application_name=application_name,
            ssl_mode=ssl_mode,
            connect_timeout=connect_timeout,
            keepalives=keepalives,
            keepalives_idle=keepalives_idle,
            keepalives_interval=keepalives_interval,
            keepalives_count=keepalives_count,
            autocommit=autocommit,
            **kwargs,
        )

        self._timeout = timeout
        self._use_pooling = use_pooling
        self._row_factory = row_factory
        self._application_name = application_name
        self._ssl_mode = ssl_mode
        self._connect_timeout = connect_timeout
        self._autocommit = autocommit

        # Connection pool
        self._connection_pool: Optional[ConnectionPool] = None
        self._pool_config = {
            "min_size": min_pool_size,
            "max_size": max_pool_size,
            "timeout": timeout,
            "kwargs": {
                "keepalives": keepalives,
                "keepalives_idle": keepalives_idle,
                "keepalives_interval": keepalives_interval,
                "keepalives_count": keepalives_count,
            },
        }

    def _get_connection_string(self) -> str:
        """Build PostgreSQL connection string.

        Returns:
            PostgreSQL connection string (libpq format).
        """
        return self.config.get_connection_string("postgresql")

    def _get_connection_params(self) -> Dict[str, Any]:
        """Build connection parameters dictionary.

        Returns:
            Dictionary of PostgreSQL connection parameters.
        """
        params = {
            "host": self.config.host,
            "port": self.config.port,
            "dbname": self.config.database,
            "user": self.config.username,
            "password": self.config.password,
            "connect_timeout": self._connect_timeout,
            "application_name": self._application_name,
            "sslmode": self._ssl_mode,
            "autocommit": self._autocommit,
        }

        # Add keepalive settings
        if self.config.extra_params.get("keepalives", True):
            params.update(
                {
                    "keepalives": 1,
                    "keepalives_idle": self.config.extra_params.get(
                        "keepalives_idle", 30
                    ),
                    "keepalives_interval": self.config.extra_params.get(
                        "keepalives_interval", 10
                    ),
                    "keepalives_count": self.config.extra_params.get(
                        "keepalives_count", 5
                    ),
                }
            )

        # Add any extra parameters from config
        for key, value in self.config.extra_params.items():
            if key not in params and key not in [
                "use_pooling",
                "row_factory",
                "min_pool_size",
                "max_pool_size",
                "keepalives",
                "keepalives_idle",
                "keepalives_interval",
                "keepalives_count",
            ]:
                params[key] = value

        # Remove None values
        return {k: v for k, v in params.items() if v is not None}

    def _get_row_factory(self):
        """Get the appropriate row factory based on configuration.

        Returns:
            Row factory function for psycopg.
        """
        if self._row_factory == "dict":
            return dict_row
        elif self._row_factory == "namedtuple":
            from psycopg.rows import namedtuple_row

            return namedtuple_row
        else:
            return tuple_row

    def connect(self) -> None:
        """Establish connection to PostgreSQL server.

        Raises:
            ConnectionError: If connection cannot be established.
        """
        if self._is_connected:
            logger.debug("Already connected to PostgreSQL")
            return

        try:
            conninfo = self._get_connection_string()

            if self._use_pooling:
                # Create connection pool
                self._connection_pool = ConnectionPool(
                    conninfo,
                    min_size=self._pool_config["min_size"],
                    max_size=self._pool_config["max_size"],
                    timeout=self._pool_config["timeout"],
                    kwargs=self._pool_config["kwargs"],
                    open=True,  # Open the pool immediately
                )

                logger.info(
                    f"Created PostgreSQL connection pool for {self.config.host}:{self.config.port}/{self.config.database} "
                    f"(min={self._pool_config['min_size']}, max={self._pool_config['max_size']})"
                )

                # Get a connection from pool to test
                with self._connection_pool.connection() as conn:
                    conn.execute("SELECT 1")

            else:
                params = self._get_connection_params()
                self._connection = psycopg.connect(**params)
                self._connection.row_factory = self._get_row_factory()

                self._cursor = self._connection.cursor()

                logger.info(
                    f"Created PostgreSQL connection for {self.config.host}:{self.config.port}/{self.config.database}"
                )

            self._is_connected = True

            try:
                with self._get_raw_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()[0]
                    logger.debug(f"PostgreSQL version: {version}")
                    cursor.close()
            except Exception as e:
                logger.warning(f"Could not retrieve PostgreSQL version: {e}")

        except OperationalError as e:
            logger.error(f"PostgreSQL operational error: {e}")
            raise ConnectionError(f"Connection failed: {e}")
        except InterfaceError as e:
            logger.error(f"PostgreSQL interface error: {e}")
            raise ConnectionError(f"Interface error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error connecting to PostgreSQL: {e}")
            raise ConnectionError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        """Close the PostgreSQL connection and clean up resources.

        This method is idempotent and safe to call multiple times.
        """
        if not self._is_connected:
            return

        try:
            # Close cursor if exists
            if self._cursor is not None:
                try:
                    self._cursor.close()
                    logger.debug("Closed PostgreSQL cursor")
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")

            # Close connection or pool
            if self._use_pooling and self._connection_pool is not None:
                try:
                    self._connection_pool.close()
                    logger.info("Closed PostgreSQL connection pool")
                except Exception as e:
                    logger.warning(f"Error closing connection pool: {e}")
            elif self._connection is not None:
                try:
                    # Commit any pending transactions
                    if not self._autocommit and not self._connection.closed:
                        self._connection.commit()

                    self._connection.close()
                    logger.info(
                        f"Disconnected from PostgreSQL at {self.config.host}:{self.config.port}"
                    )
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

        finally:
            self._connection = None
            self._cursor = None
            self._connection_pool = None
            self._is_connected = False

    def is_connected(self) -> bool:
        """Check if the connector is currently connected to PostgreSQL.

        Returns:
            True if connected and connection is healthy, False otherwise.
        """
        if not self._is_connected:
            return False

        try:
            with self._get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
        except (OperationalError, InterfaceError, DatabaseError, AttributeError) as e:
            logger.debug(f"Connection check failed: {e}")
            self._is_connected = False
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking connection: {e}")
            self._is_connected = False
            return False

    def ping(self) -> bool:
        """Test if the PostgreSQL connection is alive and responsive.

        Returns:
            True if the connection responds successfully, False otherwise.
        """
        return self.is_connected()

    def _get_raw_connection(self) -> Connection:
        """Get a raw connection (from pool or direct).

        Returns:
            A psycopg Connection object.

        Raises:
            ConnectionError: If not connected.
        """
        if not self._is_connected:
            raise ConnectionError("Not connected to PostgreSQL")

        if self._use_pooling and self._connection_pool:
            return self._connection_pool.connection()
        elif self._connection:
            # Return a context manager wrapper for consistency
            class ConnectionWrapper:
                def __init__(self, conn):
                    self.conn = conn

                def __enter__(self):
                    return self.conn

                def __exit__(self, *args):
                    pass  # Don't close single connection

            return ConnectionWrapper(self._connection)
        else:
            raise ConnectionError("No connection available")

    def get_cursor(self) -> Cursor:
        """Create and return a new cursor.

        Returns:
            A new PostgreSQL cursor object.

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()

        try:
            if self._use_pooling:
                # For pooled connections, return cursor from pool connection
                # Note: Caller must manage connection lifecycle
                conn = self._connection_pool.getconn()
                cursor = conn.cursor()
                cursor._pool_connection = conn  # Store for return
                return cursor
            else:
                return self._connection.cursor()
        except DatabaseError as e:
            logger.error(f"Failed to create cursor: {e}")
            raise ConnectionError(f"Failed to create cursor: {e}")

    def return_cursor(self, cursor: Cursor) -> None:
        """Return a cursor and its connection to the pool.

        Args:
            cursor: The cursor to return (and its connection).
        """
        if hasattr(cursor, "_pool_connection"):
            try:
                cursor.close()
                self._connection_pool.putconn(cursor._pool_connection)
            except Exception as e:
                logger.warning(f"Error returning cursor to pool: {e}")

    # Transaction Support

    def begin_transaction(self) -> None:
        """Begin a new transaction.

        Raises:
            ConnectionError: If not connected or transaction cannot start.
        """
        self.ensure_connected()

        if self._autocommit:
            logger.warning(
                "Autocommit is enabled; explicit transactions have no effect"
            )
            return

        try:
            with self._get_raw_connection() as conn:
                if not conn.info.transaction_status:
                    # No transaction in progress, this is implicit in PostgreSQL
                    logger.debug("Transaction implicitly started")
        except DatabaseError as e:
            logger.error(f"Failed to begin transaction: {e}")
            raise ConnectionError(f"Failed to begin transaction: {e}")

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            ConnectionError: If not connected or commit fails.
        """
        self.ensure_connected()

        if self._autocommit:
            logger.debug("Autocommit enabled; explicit commit has no effect")
            return

        try:
            with self._get_raw_connection() as conn:
                conn.commit()
            logger.debug("Committed PostgreSQL transaction")
        except DatabaseError as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise ConnectionError(f"Failed to commit transaction: {e}")

    def rollback(self) -> None:
        """Roll back the current transaction.

        Raises:
            ConnectionError: If not connected or rollback fails.
        """
        self.ensure_connected()

        if self._autocommit:
            logger.debug("Autocommit enabled; explicit rollback has no effect")
            return

        try:
            with self._get_raw_connection() as conn:
                conn.rollback()
            logger.debug("Rolled back PostgreSQL transaction")
        except DatabaseError as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise ConnectionError(f"Failed to rollback transaction: {e}")

    def execute_query(
        self,
        query: str,
        params: Optional[Union[Tuple, Dict]] = None,
        fetch: bool = True,
    ) -> Optional[List[Any]]:
        """Execute a query and optionally fetch results.

        Args:
            query: SQL query to execute.
            params: Query parameters (tuple for positional, dict for named).
            fetch: If True, fetch and return results.

        Returns:
            List of results if fetch is True, None otherwise.

        Raises:
            ConnectionError: If query execution fails.
        """
        self.ensure_connected()

        try:
            with self._get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)

                if fetch:
                    results = cursor.fetchall()
                    cursor.close()
                    return results
                else:
                    cursor.close()
                    return None
        except DatabaseError as e:
            logger.error(f"Query execution failed: {e}")
            raise ConnectionError(f"Query failed: {e}")

    def execute_batch(self, query: str, params_list: List[Union[Tuple, Dict]]) -> None:
        """Execute a query multiple times with different parameters.

        Args:
            query: SQL query to execute.
            params_list: List of parameter sets.

        Raises:
            ConnectionError: If batch execution fails.
        """
        self.ensure_connected()

        try:
            with self._get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, params_list)
                conn.commit()

            logger.debug(f"Executed batch query with {len(params_list)} parameter sets")
        except DatabaseError as e:
            logger.error(f"Batch execution failed: {e}")
            raise ConnectionError(f"Batch execution failed: {e}")

    def create_savepoint(self, name: str) -> None:
        """Create a savepoint in the current transaction.

        Args:
            name: Name of the savepoint.

        Raises:
            ConnectionError: If savepoint creation fails.
        """
        self.ensure_connected()

        try:
            with self._get_raw_connection() as conn:
                conn.execute(f"SAVEPOINT {name}")
            logger.debug(f"Created savepoint: {name}")
        except DatabaseError as e:
            logger.error(f"Failed to create savepoint: {e}")
            raise ConnectionError(f"Failed to create savepoint: {e}")

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a savepoint.

        Args:
            name: Name of the savepoint.

        Raises:
            ConnectionError: If rollback to savepoint fails.
        """
        self.ensure_connected()

        try:
            with self._get_raw_connection() as conn:
                conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
            logger.debug(f"Rolled back to savepoint: {name}")
        except DatabaseError as e:
            logger.error(f"Failed to rollback to savepoint: {e}")
            raise ConnectionError(f"Failed to rollback to savepoint: {e}")

    def release_savepoint(self, name: str) -> None:
        """Release a savepoint.

        Args:
            name: Name of the savepoint.

        Raises:
            ConnectionError: If savepoint release fails.
        """
        self.ensure_connected()

        try:
            with self._get_raw_connection() as conn:
                conn.execute(f"RELEASE SAVEPOINT {name}")
            logger.debug(f"Released savepoint: {name}")
        except DatabaseError as e:
            logger.error(f"Failed to release savepoint: {e}")
            raise ConnectionError(f"Failed to release savepoint: {e}")

    def get_server_version(self) -> Tuple[int, ...]:
        """Get PostgreSQL server version.

        Returns:
            Tuple of version numbers (major, minor, patch).

        Raises:
            ConnectionError: If version retrieval fails.
        """
        self.ensure_connected()

        try:
            with self._get_raw_connection() as conn:
                return conn.info.server_version
        except DatabaseError as e:
            logger.error(f"Failed to get server version: {e}")
            raise ConnectionError(f"Failed to get server version: {e}")

    def vacuum(self, table: Optional[str] = None, analyze: bool = True) -> None:
        """Run VACUUM on database or specific table.

        Args:
            table: Optional table name. If None, vacuums entire database.
            analyze: If True, also run ANALYZE.

        Raises:
            ConnectionError: If vacuum fails.
        """
        self.ensure_connected()

        try:
            cmd = "VACUUM"
            if analyze:
                cmd += " ANALYZE"
            if table:
                cmd += f" {table}"

            with self._get_raw_connection() as conn:
                # VACUUM cannot run inside a transaction
                old_autocommit = conn.autocommit
                conn.autocommit = True
                conn.execute(cmd)
                conn.autocommit = old_autocommit

            logger.info(f"Vacuumed {'table ' + table if table else 'database'}")
        except DatabaseError as e:
            logger.error(f"Vacuum failed: {e}")
            raise ConnectionError(f"Vacuum failed: {e}")

    def get_connection_info(self) -> Dict[str, Any]:
        """Get detailed connection information.

        Returns:
            Dictionary containing connection status and server metadata.
        """
        info = super().get_connection_info()

        info.update(
            {
                "pooling_enabled": self._use_pooling,
                "row_factory": self._row_factory,
                "ssl_mode": self._ssl_mode,
                "autocommit": self._autocommit,
            }
        )

        if self._is_connected:
            try:
                version = self.get_server_version()
                info["server_version"] = ".".join(map(str, version))

                with self._get_raw_connection() as conn:
                    cursor = conn.cursor()

                    # Get current database size
                    cursor.execute("SELECT pg_database_size(current_database())")
                    info["database_size_bytes"] = cursor.fetchone()[0]

                    # Get connection count
                    cursor.execute(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                    )
                    info["active_connections"] = cursor.fetchone()[0]

                    cursor.close()

            except Exception as e:
                logger.warning(f"Could not retrieve PostgreSQL info: {e}")

        return info

    def __repr__(self) -> str:
        """String representation of the PostgreSQL connector."""
        status = "connected" if self._is_connected else "disconnected"
        pool_type = "pooled" if self._use_pooling else "single"
        return (
            f"<PostgresConnector {self.config.host}:{self.config.port}/{self.config.database} "
            f"[{pool_type}, {status}]>"
        )
