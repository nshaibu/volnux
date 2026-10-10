import logging
import typing
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg
from psycopg import Connection, Cursor
from psycopg.conninfo import make_conninfo
from psycopg.errors import DatabaseError, InterfaceError, OperationalError
from psycopg.rows import dict_row, tuple_row
from psycopg_pool import ConnectionPool

from volnux.backends.connection import BackendConnectorBase

logger = logging.getLogger(__name__)


class PostgresConnector(BackendConnectorBase[Connection]):
    """PostgreSQL backend connector with consistent connection and transaction handling."""

    DEFAULT_PORT = 5432
    DEFAULT_TIMEOUT = 30
    DEFAULT_POOL_SIZE = 10
    DEFAULT_MIN_POOL_SIZE = 2
    DEFAULT_MAX_POOL_SIZE = 20

    # uri scheme
    scheme = "postgres"

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
        row_factory: str = "tuple",
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

        self._use_pooling = use_pooling
        self._row_factory = row_factory
        self._application_name = application_name
        self._ssl_mode = ssl_mode
        self._connect_timeout = connect_timeout
        self._autocommit = autocommit

        self._connection_pool: Optional[ConnectionPool] = None
        self._connection: Optional[Connection] = None
        self._cursor: Optional[Cursor] = None

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
        return self.config.get_connection_string("postgresql")

    def _get_connection_params(self) -> Dict[str, Any]:
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

        excluded = {
            "use_pooling",
            "pool_size",
            "ssl_mode",
            "row_factory",
            "min_pool_size",
            "max_pool_size",
            "keepalives",
            "keepalives_idle",
            "keepalives_interval",
            "keepalives_count",
        }
        for key, value in self.config.extra_params.items():
            if key not in params and key not in excluded:
                params[key] = value

        return {k: v for k, v in params.items() if v is not None}

    def _get_row_factory(self):
        if self._row_factory == "dict":
            return dict_row
        if self._row_factory == "namedtuple":
            from psycopg.rows import namedtuple_row

            return namedtuple_row
        return tuple_row

    def _initialize_pool(self) -> None:
        try:
            conninfo = self._get_connection_string()
            self._connection_pool = ConnectionPool(
                conninfo,
                min_size=self._pool_config["min_size"],
                max_size=self._pool_config["max_size"],
                timeout=self._pool_config["timeout"],
                kwargs=self._pool_config["kwargs"],
                open=True,
            )
            logger.info(
                "Initialized PostgreSQL connection pool for %s:%s/%s (min=%s, max=%s)",
                self.config.host,
                self.config.port,
                self.config.database,
                self._pool_config["min_size"],
                self._pool_config["max_size"],
            )
        except Exception as e:
            logger.error("Failed to initialize PostgreSQL connection pool: %s", e)
            raise ConnectionError(f"Failed to initialize connection pool: {e}")

    def connect(self) -> None:
        if self._is_connected:
            logger.debug("Already connected to PostgresSQL")
            return

        try:
            if self._use_pooling:
                if self._connection_pool is None:
                    self._initialize_pool()

                with self._connection_pool.connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
            else:
                params = self._get_connection_params()
                params.pop("pool_size", None)

                self._connection = psycopg.connect(**params)
                self._connection.row_factory = self._get_row_factory()
                self._connection.autocommit = self._autocommit

                with self._connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

            self._is_connected = True
            logger.info(
                "Successfully connected to PostgresSQL at %s:%s (%s)",
                self.config.host,
                self.config.port,
                self.config.database,
            )
        except (OperationalError, InterfaceError) as e:
            logger.error("PostgresSQL connection failed: %s", e)
            raise ConnectionError(f"Connection failed: {e}")
        except Exception as e:
            logger.error("Unexpected error connecting to PostgresSQL: %s", e)
            raise ConnectionError(f"Unexpected connection error: {e}")

    def disconnect(self) -> None:
        if (
            not self._is_connected
            and self._connection is None
            and self._connection_pool is None
        ):
            return

        try:
            self._cursor = None

            if self._connection is not None:
                try:
                    if not self._autocommit and not self._connection.closed:
                        self._connection.commit()
                except Exception:
                    pass
                try:
                    self._connection.close()
                except Exception as e:
                    logger.warning("Error closing PostgreSQL connection: %s", e)

            if self._connection_pool is not None:
                try:
                    self._connection_pool.close()
                except Exception as e:
                    logger.warning("Error closing PostgreSQL connection pool: %s", e)

        finally:
            self._connection = None
            self._connection_pool = None
            self._is_connected = False

    def is_connected(self) -> bool:
        if not self._is_connected:
            return False

        try:
            with self._connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            return True
        except (OperationalError, InterfaceError, DatabaseError, AttributeError):
            self._is_connected = False
            return False
        except Exception:
            self._is_connected = False
            return False

    def ping(self) -> bool:
        return self.is_connected()

    @contextmanager
    def _connection_context(self):
        if not self._is_connected:
            raise ConnectionError("Not connected to PostgreSQL")

        if self._use_pooling:
            if self._connection_pool is None:
                raise ConnectionError("Connection pool is not initialized")
            with self._connection_pool.connection() as conn:
                yield conn
        else:
            if self._connection is None:
                raise ConnectionError("No connection available")
            yield self._connection

    @contextmanager
    def _cursor_context(self):
        with self._connection_context() as conn:
            with conn.cursor() as cursor:
                yield cursor

    def get_cursor(self) -> Cursor:
        self.ensure_connected()
        try:
            if self._use_pooling:
                conn = self._connection_pool.getconn()
                cursor = conn.cursor()
                cursor._pool_connection = conn  # compatibility bridge
                return cursor
            return self._connection.cursor()
        except DatabaseError as e:
            logger.error("Failed to create cursor: %s", e)
            raise ConnectionError(f"Failed to create cursor: {e}")

    def return_cursor(self, cursor: Cursor) -> None:
        if hasattr(cursor, "_pool_connection"):
            try:
                pool_conn = cursor._pool_connection
                cursor.close()
                self._connection_pool.putconn(pool_conn)
            except Exception as e:
                logger.warning("Error returning cursor to pool: %s", e)

    def begin_transaction(self) -> None:
        """Initiate a transaction lifecycle for the base context manager."""
        self.ensure_connected()

        if self._autocommit:
            logger.warning(
                "Autocommit is enabled; explicit transactions have no effect"
            )
            return

        try:
            with self._connection_context() as conn:
                if conn.autocommit:
                    conn.autocommit = False
                conn.execute("BEGIN")
                logger.debug("PostgreSQL transaction initiated")
        except DatabaseError as e:
            logger.error("Failed to begin transaction: %s", e)
            raise ConnectionError(f"Failed to begin transaction: {e}")

    def commit(self) -> None:
        self.ensure_connected()

        if self._autocommit:
            logger.debug("Autocommit enabled; explicit commit has no effect")
            return

        try:
            with self._connection_context() as conn:
                conn.commit()
            logger.debug("Committed PostgreSQL transaction")
        except DatabaseError as e:
            logger.error("Failed to commit transaction: %s", e)
            raise ConnectionError(f"Failed to commit transaction: {e}")

    def rollback(self) -> None:
        self.ensure_connected()

        if self._autocommit:
            logger.debug("Autocommit enabled; explicit rollback has no effect")
            return

        try:
            with self._connection_context() as conn:
                conn.rollback()
            logger.debug("Rolled back PostgreSQL transaction")
        except DatabaseError as e:
            logger.error("Failed to rollback transaction: %s", e)
            raise ConnectionError(f"Failed to rollback transaction: {e}")

    def execute_query(
        self,
        query: str,
        params: Optional[Union[Tuple, Dict]] = None,
        fetch: bool = True,
    ) -> Optional[List[Any]]:
        self.ensure_connected()

        try:
            with self._cursor_context() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                return None
        except DatabaseError as e:
            logger.error("Query execution failed: %s", e)
            raise ConnectionError(f"Query failed: {e}")

    def execute_batch(self, query: str, params_list: List[Union[Tuple, Dict]]) -> None:
        self.ensure_connected()

        try:
            with self._connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, params_list)
                if not self._autocommit:
                    conn.commit()
            logger.debug(
                "Executed batch query with %s parameter sets", len(params_list)
            )
        except DatabaseError as e:
            logger.error("Batch execution failed: %s", e)
            raise ConnectionError(f"Batch execution failed: {e}")

    def create_savepoint(self, name: str) -> None:
        self.ensure_connected()
        try:
            with self._connection_context() as conn:
                conn.execute(f"SAVEPOINT {name}")
            logger.debug("Created savepoint: %s", name)
        except DatabaseError as e:
            logger.error("Failed to create savepoint: %s", e)
            raise ConnectionError(f"Failed to create savepoint: {e}")

    def rollback_to_savepoint(self, name: str) -> None:
        self.ensure_connected()
        try:
            with self._connection_context() as conn:
                conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
            logger.debug("Rolled back to savepoint: %s", name)
        except DatabaseError as e:
            logger.error("Failed to rollback to savepoint: %s", e)
            raise ConnectionError(f"Failed to rollback to savepoint: {e}")

    def release_savepoint(self, name: str) -> None:
        self.ensure_connected()
        try:
            with self._connection_context() as conn:
                conn.execute(f"RELEASE SAVEPOINT {name}")
            logger.debug("Released savepoint: %s", name)
        except DatabaseError as e:
            logger.error("Failed to release savepoint: %s", e)
            raise ConnectionError(f"Failed to release savepoint: {e}")

    def get_server_version(self) -> Tuple[int, ...]:
        self.ensure_connected()
        try:
            with self._connection_context() as conn:
                return conn.info.server_version
        except DatabaseError as e:
            logger.error("Failed to get server version: %s", e)
            raise ConnectionError(f"Failed to get server version: {e}")

    def vacuum(self, table: Optional[str] = None, analyze: bool = True) -> None:
        self.ensure_connected()

        cmd = "VACUUM"
        if analyze:
            cmd += " ANALYZE"
        if table:
            cmd += f" {table}"

        try:
            with self._connection_context() as conn:
                old_autocommit = conn.autocommit
                conn.autocommit = True
                try:
                    conn.execute(cmd)
                finally:
                    conn.autocommit = old_autocommit
            logger.info("Vacuumed %s", f"table {table}" if table else "database")
        except DatabaseError as e:
            logger.error("Vacuum failed: %s", e)
            raise ConnectionError(f"Vacuum failed: {e}")

    def get_connection_info(self) -> Dict[str, Any]:
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

                with self._connection_context() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT pg_database_size(current_database())")
                        info["database_size_bytes"] = cursor.fetchone()[0]

                        cursor.execute(
                            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                        )
                        info["active_connections"] = cursor.fetchone()[0]
            except Exception as e:
                logger.warning("Could not retrieve PostgreSQL info: %s", e)

        return info

    def __repr__(self) -> str:
        status = "connected" if self._is_connected else "disconnected"
        pool_type = "pooled" if self._use_pooling else "single"
        return (
            f"<PostgresConnector {self.config.host}:{self.config.port}/{self.config.database} "
            f"[{pool_type}, {status}]>"
        )
