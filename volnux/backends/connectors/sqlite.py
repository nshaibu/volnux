import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any, Optional, Union

from volnux.backends.connection import BackendConnectorBase, ConnectionError

logger = logging.getLogger(__name__)


class SqliteConnector(BackendConnectorBase[sqlite3.Cursor]):
    """
    SQLite backend connector with thread safety and transaction support.

    This connector provides a robust interface to SQLite databases with
    automatic connection management, proper transaction handling, and
    thread-local connections for concurrent usage.

    Example:
        >>> connector = SqliteConnector(
        ...     database="myapp.db",
        ...     timeout=30,
        ...     enable_wal=True
        ... )
        >>> with connector:
        ...     cursor = connector.get_cursor()
        ...     cursor.execute("SELECT * FROM users")
        ...     results = cursor.fetchall()
    """

    # SQLite-specific defaults
    DEFAULT_TIMEOUT = 30.0
    DEFAULT_ISOLATION_LEVEL = None  # Autocommit mode

    def __init__(
        self,
        database: Union[str, Path],
        timeout: float = DEFAULT_TIMEOUT,
        check_same_thread: bool = False,
        isolation_level: Optional[str] = DEFAULT_ISOLATION_LEVEL,
        enable_foreign_keys: bool = True,
        enable_wal: bool = True,
        detect_types: int = 0,
        cached_statements: int = 128,
        uri: bool = False,
        **kwargs: Any,
    ):
        """Initialize the SQLite connector.

        Args:
            database: Path to the SQLite database file. Use ':memory:' for in-memory DB.
            timeout: How long to wait for locks (in seconds).
            check_same_thread: If False, allow usage from multiple threads (default: False).
            isolation_level: Transaction isolation level. None for autocommit.
            enable_foreign_keys: Enable foreign key constraint enforcement.
            enable_wal: Enable Write-Ahead Logging for better concurrency.
            detect_types: Type detection flags (PARSE_DECLTYPES | PARSE_COLNAMES).
            cached_statements: Number of statements to cache.
            uri: If True, interpret database parameter as URI.
            **kwargs: Additional SQLite connection parameters.
        """
        # SQLite doesn't use host/port, so we set sensible defaults
        super().__init__(
            host="localhost",
            port=0,
            database=str(database),
            timeout=int(timeout),
            check_same_thread=check_same_thread,
            isolation_level=isolation_level,
            enable_foreign_keys=enable_foreign_keys,
            enable_wal=enable_wal,
            detect_types=detect_types,
            cached_statements=cached_statements,
            uri=uri,
            **kwargs,
        )

        self.database_path = Path(database) if database != ":memory:" else None
        self._timeout = timeout
        self._check_same_thread = check_same_thread
        self._isolation_level = isolation_level
        self._enable_foreign_keys = enable_foreign_keys
        self._enable_wal = enable_wal
        self._detect_types = detect_types
        self._cached_statements = cached_statements
        self._uri = uri

        self._local = threading.local()
        self._lock = threading.RLock()

        # Validate database path
        if self.database_path and not self.database_path.parent.exists():
            logger.warning(
                f"Database directory does not exist: {self.database_path.parent}"
            )

    def _get_connection_params(self) -> dict:
        """Build connection parameters dictionary.

        Returns:
            Dictionary of SQLite connection parameters.
        """
        params = {
            "database": self.config.database,
            "timeout": self._timeout,
            "check_same_thread": self._check_same_thread,
            "isolation_level": self._isolation_level,
            "detect_types": self._detect_types,
            "cached_statements": self._cached_statements,
            "uri": self._uri,
        }

        # Add any extra parameters from config
        for key, value in self.config.extra_params.items():
            if key not in params:
                params[key] = value

        return params

    def _configure_connection(self, connection: sqlite3.Connection) -> None:
        """Configure connection with pragmas and settings.

        Args:
            connection: The SQLite connection to configure.
        """
        cursor = connection.cursor()

        try:
            # Enable foreign key constraints
            if self._enable_foreign_keys:
                cursor.execute("PRAGMA foreign_keys = ON")
                logger.debug("Enabled foreign key constraints")

            # Enable WAL mode for better concurrency
            if self._enable_wal:
                cursor.execute("PRAGMA journal_mode = WAL")
                logger.debug("Enabled Write-Ahead Logging (WAL) mode")

            # Set synchronous mode for balance of safety and performance
            cursor.execute("PRAGMA synchronous = NORMAL")

            # Enable memory-mapped I/O for better performance
            cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB

            # Set temp store to memory
            cursor.execute("PRAGMA temp_store = MEMORY")

            connection.commit()
        except sqlite3.Error as e:
            logger.warning(f"Failed to configure connection: {e}")
        finally:
            cursor.close()

    def connect(self) -> None:
        """Establish connection to the SQLite database.

        Raises:
            ConnectionError: If connection cannot be established.
        """
        if self._is_connected:
            logger.debug("Already connected to SQLite database")
            return

        with self._lock:
            try:
                params = self._get_connection_params()

                # Create connection
                self._connection = sqlite3.connect(**params)
                self._connection.row_factory = sqlite3.Row  # Enable named columns

                # Configure connection
                self._configure_connection(self._connection)

                # Create initial cursor
                self._cursor = self._connection.cursor()
                self._is_connected = True

                logger.info(f"Connected to SQLite database: {self.config.database}")

                # Log database info
                cursor = self._connection.cursor()
                cursor.execute("SELECT sqlite_version()")
                version = cursor.fetchone()[0]
                logger.debug(f"SQLite version: {version}")
                cursor.close()

            except sqlite3.Error as e:
                logger.error(f"Failed to connect to SQLite database: {e}")
                raise ConnectionError(f"SQLite connection failed: {e}")
            except Exception as e:
                logger.error(f"Unexpected error connecting to SQLite: {e}")
                raise ConnectionError(f"Unexpected connection error: {e}")

    def disconnect(self) -> None:
        """Close the SQLite connection and clean up resources.

        This method is idempotent and safe to call multiple times.
        """
        if not self._is_connected and self._connection is None:
            return

        with self._lock:
            try:
                if self._cursor is not None:
                    try:
                        self._cursor.close()
                        logger.debug("Closed SQLite cursor")
                    except sqlite3.Error as e:
                        logger.warning(f"Error closing cursor: {e}")

                if self._connection is not None:
                    try:
                        # Commit any pending transactions
                        if self._isolation_level is not None:
                            self._connection.commit()

                        self._connection.close()
                        logger.info(
                            f"Disconnected from SQLite database: {self.config.database}"
                        )
                    except sqlite3.Error as e:
                        logger.warning(f"Error closing connection: {e}")

            finally:
                self._connection = None
                self._cursor = None
                self._is_connected = False

    def is_connected(self) -> bool:
        """Check if the connector is currently connected to SQLite.

        Returns:
            True if connected and connection is healthy, False otherwise.
        """
        if not self._is_connected or self._connection is None:
            return False

        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except (sqlite3.Error, AttributeError) as e:
            logger.debug(f"Connection check failed: {e}")
            self._is_connected = False
            return False
        except Exception as e:
            logger.warning(f"Unexpected error checking connection: {e}")
            self._is_connected = False
            return False

    def ping(self) -> bool:
        """Test if the SQLite connection is alive and responsive.

        Returns:
            True if the connection responds successfully, False otherwise.
        """
        return self.is_connected()

    def get_cursor(self) -> sqlite3.Cursor:
        """Create and return a new cursor.

        Returns:
            A new SQLite cursor object.

        Raises:
            ConnectionError: If not connected.
        """
        self.ensure_connected()

        try:
            cursor = self._connection.cursor()
            return cursor
        except sqlite3.Error as e:
            logger.error(f"Failed to create cursor: {e}")
            raise ConnectionError(f"Failed to create cursor: {e}")

    def begin_transaction(self) -> None:
        """Begin a new transaction.

        Raises:
            ConnectionError: If not connected or transaction cannot start.
        """
        self.ensure_connected()

        try:
            if self._isolation_level is None:
                self._connection.execute("BEGIN")
                logger.debug("Began SQLite transaction")
            else:
                logger.debug("Transaction automatically started")
        except sqlite3.Error as e:
            logger.error(f"Failed to begin transaction: {e}")
            raise ConnectionError(f"Failed to begin transaction: {e}")

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            ConnectionError: If not connected or commit fails.
        """
        self.ensure_connected()

        try:
            self._connection.commit()
            logger.debug("Committed SQLite transaction")
        except sqlite3.Error as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise ConnectionError(f"Failed to commit transaction: {e}")

    def rollback(self) -> None:
        """Roll back the current transaction.

        Raises:
            ConnectionError: If not connected or rollback fails.
        """
        self.ensure_connected()

        try:
            self._connection.rollback()
            logger.debug("Rolled back SQLite transaction")
        except sqlite3.Error as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise ConnectionError(f"Failed to rollback transaction: {e}")

    def execute_script(self, script: str) -> None:
        """Execute a SQL script containing multiple statements.

        Args:
            script: SQL script to execute.

        Raises:
            ConnectionError: If not connected or execution fails.
        """
        self.ensure_connected()

        try:
            self._connection.executescript(script)
            self._connection.commit()
            logger.debug("Executed SQL script")
        except sqlite3.Error as e:
            logger.error(f"Failed to execute script: {e}")
            raise ConnectionError(f"Failed to execute script: {e}")

    def backup(self, target_database: Union[str, Path]) -> None:
        """Backup the database to another file.

        Args:
            target_database: Path to the backup database file.

        Raises:
            ConnectionError: If not connected or backup fails.
        """
        self.ensure_connected()

        try:
            target_path = str(target_database)
            with sqlite3.connect(target_path) as target_conn:
                self._connection.backup(target_conn)

            logger.info(f"Backed up database to {target_path}")
        except sqlite3.Error as e:
            logger.error(f"Failed to backup database: {e}")
            raise ConnectionError(f"Failed to backup database: {e}")

    def vacuum(self) -> None:
        """Rebuild the database file, reclaiming unused space.

        Raises:
            ConnectionError: If not connected or vacuum fails.
        """
        self.ensure_connected()

        try:
            self._connection.execute("VACUUM")
            logger.info("Vacuumed database")
        except sqlite3.Error as e:
            logger.error(f"Failed to vacuum database: {e}")
            raise ConnectionError(f"Failed to vacuum database: {e}")

    def analyze(self) -> None:
        """Gather statistics about tables and indexes for query optimization.

        Raises:
            ConnectionError: If not connected or analyze fails.
        """
        self.ensure_connected()

        try:
            self._connection.execute("ANALYZE")
            logger.info("Analyzed database")
        except sqlite3.Error as e:
            logger.error(f"Failed to analyze database: {e}")
            raise ConnectionError(f"Failed to analyze database: {e}")

    def get_table_info(self, table_name: str) -> list:
        """Get information about a table's columns.

        Args:
            table_name: Name of the table to inspect.

        Returns:
            List of tuples containing column information.

        Raises:
            ConnectionError: If not connected or query fails.
        """
        self.ensure_connected()

        try:
            cursor = self._connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            info = cursor.fetchall()
            cursor.close()
            return info
        except sqlite3.Error as e:
            logger.error(f"Failed to get table info: {e}")
            raise ConnectionError(f"Failed to get table info: {e}")

    def get_database_size(self) -> int:
        """Get the size of the database file in bytes.

        Returns:
            Database file size in bytes, or 0 for in-memory databases.
        """
        if self.database_path and self.database_path.exists():
            return self.database_path.stat().st_size
        return 0

    def get_connection_info(self) -> dict:
        """Get detailed connection information.

        Returns:
            Dictionary containing connection status and database metadata.
        """
        info = super().get_connection_info()

        info.update(
            {
                "database_path": (
                    str(self.database_path) if self.database_path else ":memory:"
                ),
                "wal_enabled": self._enable_wal,
                "foreign_keys_enabled": self._enable_foreign_keys,
            }
        )

        if self._is_connected:
            try:
                cursor = self._connection.cursor()

                # Get SQLite version
                cursor.execute("SELECT sqlite_version()")
                info["sqlite_version"] = cursor.fetchone()[0]

                # Get database size
                info["database_size_bytes"] = self.get_database_size()

                # Get page count and size
                cursor.execute("PRAGMA page_count")
                info["page_count"] = cursor.fetchone()[0]

                cursor.execute("PRAGMA page_size")
                info["page_size"] = cursor.fetchone()[0]

                cursor.close()
            except Exception as e:
                logger.warning(f"Could not retrieve database info: {e}")

        return info

    def __repr__(self) -> str:
        """String representation of the SQLite connector."""
        status = "connected" if self._is_connected else "disconnected"
        db_path = self.database_path or ":memory:"
        return f"<SqliteConnector {db_path} [{status}]>"
