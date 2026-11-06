import functools
import logging
import threading
import typing
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg
from psycopg import Connection, Cursor
from psycopg.conninfo import make_conninfo
from psycopg.errors import DatabaseError, InterfaceError, OperationalError
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row, tuple_row
from psycopg_pool import ConnectionPool

from volnux.backends.connection import BackendConnectorBase

logger = logging.getLogger(__name__)


class _ManagedCursor(Cursor):
    """A cursor that runs a cleanup callback when closed.

    With pooling the callback hands the connection back to the pool; with a
    single shared connection it clears an aborted transaction.

    ``psycopg.Cursor`` defines ``__slots__``, so state cannot be attached to a
    plain cursor; subclassing gives the cursor a ``__dict__`` and lets
    ``close()`` (what ``with`` blocks and ``finally`` clauses already call)
    release the connection, instead of every caller having to remember to.
    """

    _release: Optional[typing.Callable[[], None]] = None

    def close(self) -> None:
        try:
            super().close()
        finally:
            release, self._release = self._release, None
            if release is not None:
                release()


class _Transaction:
    """The connection a thread's open transaction is bound to."""

    __slots__ = ("conn", "owned", "depth", "rollback_only")

    def __init__(self, conn: Connection, owned: bool) -> None:
        self.conn = conn
        self.owned = owned  # checked out of the pool for this transaction
        self.depth = 1  # nested transaction() blocks share the outer one
        self.rollback_only = False


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
        health_check_interval: float = 1.0,
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

        # Single-connection mode only: how long a successful liveness probe is
        # trusted. A pool needs none (it validates connections on checkout).
        self.health_check_interval = health_check_interval
        self._use_pooling = use_pooling
        self._row_factory = row_factory
        self._application_name = application_name
        self._ssl_mode = ssl_mode
        self._connect_timeout = connect_timeout
        self._autocommit = autocommit

        self._connection_pool: Optional[ConnectionPool] = None
        # The open transaction of the *current thread*. A transaction must run
        # on one connection from BEGIN to COMMIT; with a pool, successive
        # checkouts would otherwise land on different connections.
        self._local = threading.local()
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

    def _pool_conninfo(self) -> str:
        """libpq connection string carrying every connection option.

        Built from the same parameters as a direct connection so pooled
        connections honour ``sslmode``, ``application_name``, ``connect_timeout``
        and the keepalive settings (the bare URI used before dropped them).
        """
        params = self._get_connection_params()
        params.pop("autocommit", None)  # a psycopg option, not a libpq one
        return make_conninfo("", **params)

    def _initialize_pool(self) -> None:
        try:
            self._connection_pool = ConnectionPool(
                self._pool_conninfo(),
                min_size=self._pool_config["min_size"],
                max_size=self._pool_config["max_size"],
                timeout=self._pool_config["timeout"],
                # psycopg options applied to every pooled connection
                kwargs={
                    "autocommit": self._autocommit,
                    "row_factory": self._get_row_factory(),
                },
                # validate on checkout, so connections the server dropped
                # (restart, idle timeout) are replaced instead of handed out
                check=ConnectionPool.check_connection,
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
                if self._connection_pool is None or self._connection_pool.closed:
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
            self._mark_verified()
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
        """Whether the connector believes it is connected. Cheap: no round trip
        per call (this runs before every store operation).

        * Pooled: true while the pool is open. Dead connections are replaced by
          the pool's validation on checkout, so nothing else is needed here.
        * Single connection: true unless the connection is known to be closed or
          broken; the server is probed at most once per ``health_check_interval``
          (default 1s) to notice a connection it dropped silently.

        Use ``ping()`` to verify the server explicitly.
        """
        if not self._is_connected:
            return False

        if self._use_pooling:
            pool = self._connection_pool
            if pool is None or pool.closed:
                self._is_connected = False
                return False
            return True

        conn = self._connection
        # `is True` (not truthiness): only a real known-bad state counts
        if (
            conn is None
            or conn.closed is True
            or getattr(conn, "broken", False) is True
        ):
            self._is_connected = False
            return False
        return True if self._recently_verified() else self._probe()

    def _probe(self) -> bool:
        """Run ``SELECT 1`` and record the outcome."""
        try:
            with self._connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
        except Exception:
            self._is_connected = False
            self._mark_verified(False)
            return False
        self._mark_verified()
        return True

    def ping(self) -> bool:
        """Verify the server answers (a real round trip, unlike ``is_connected``)."""
        return self._is_connected and self._probe()

    @contextmanager
    def _connection_context(self):
        if not self._is_connected:
            raise ConnectionError("Not connected to PostgreSQL")

        transaction = self._transaction_state()
        if transaction is not None:
            yield transaction.conn  # stay on the transaction's connection
            return

        if self._use_pooling:
            if self._connection_pool is None:
                raise ConnectionError("Connection pool is not initialized")
            with self._connection_pool.connection() as conn:
                yield conn
        else:
            if self._connection is None:
                raise ConnectionError("No connection available")
            try:
                yield self._connection
            finally:
                self._recover_if_aborted(self._connection)

    @contextmanager
    def _cursor_context(self):
        with self._connection_context() as conn:
            with conn.cursor() as cursor:
                yield cursor

    def get_cursor(self) -> Cursor:
        """Return a cursor.

        Inside a transaction the cursor runs on the transaction's connection.
        Otherwise, with pooling, it borrows a pooled connection that is
        returned when the cursor is closed.
        """
        self.ensure_connected()
        try:
            transaction = self._transaction_state()
            if transaction is not None:
                return transaction.conn.cursor()
            if self._use_pooling:
                pool = self._connection_pool
                conn = pool.getconn()
                try:
                    cursor = _ManagedCursor(conn, row_factory=conn.row_factory)
                except BaseException:
                    pool.putconn(conn)
                    raise
                cursor._release = functools.partial(self._release_connection, conn)
                return cursor
            conn = self._connection
            cursor = _ManagedCursor(conn, row_factory=conn.row_factory)
            cursor._release = functools.partial(self._recover_if_aborted, conn)
            return cursor
        except DatabaseError as e:
            logger.error("Failed to create cursor: %s", e)
            raise ConnectionError(f"Failed to create cursor: {e}")

    def _recover_if_aborted(self, conn: Optional[Connection]) -> None:
        """Roll back a transaction PostgreSQL has put in the error state.

        After a failed statement every further command is refused with "current
        transaction is aborted" until a rollback. A shared connection would stay
        poisoned for all later operations. (Nothing else is touched: a healthy
        open transaction is left alone, and one already in the error state can
        only be rolled back, so this is safe even for another thread's.)
        """
        try:
            if (
                conn is not None
                and conn.closed is not True
                and conn.info.transaction_status == TransactionStatus.INERROR
            ):
                conn.rollback()
        except Exception as e:
            logger.debug("Could not clear aborted transaction: %s", e)

    def _release_connection(self, conn: Connection) -> None:
        """Give a borrowed connection back, leaving it in a clean state."""
        try:
            if (
                not conn.closed
                and conn.info.transaction_status != TransactionStatus.IDLE
            ):
                # Reads leave an implicit transaction open (autocommit off);
                # writes go through transaction(), so there is nothing to keep.
                conn.rollback()
        except Exception as e:
            logger.debug("Could not reset pooled connection: %s", e)
        finally:
            pool = self._connection_pool
            if pool is not None:
                try:
                    pool.putconn(conn)
                except Exception as e:
                    logger.warning("Error returning connection to pool: %s", e)

    def return_cursor(self, cursor: Cursor) -> None:
        """Release a cursor from ``get_cursor`` (``cursor.close()`` does the same)."""
        try:
            cursor.close()
        except Exception as e:
            logger.warning("Error closing cursor: %s", e)

    # Transactions

    def _transaction_state(self) -> Optional[_Transaction]:
        return getattr(self._local, "transaction", None)

    def _end_transaction(self, transaction: _Transaction) -> None:
        self._local.transaction = None
        if transaction.owned:
            self._release_connection(transaction.conn)

    def begin_transaction(self) -> None:
        """Start a transaction bound to one connection for the current thread.

        Nested calls join the outer transaction: only the outermost
        ``commit``/``rollback`` ends it, so an inner block can no longer commit
        its caller's work early.
        """
        self.ensure_connected()

        if self._autocommit:
            logger.warning(
                "Autocommit is enabled; explicit transactions have no effect"
            )
            return

        transaction = self._transaction_state()
        if transaction is not None:
            transaction.depth += 1
            return

        owned = self._use_pooling
        conn = None
        try:
            conn = self._connection_pool.getconn() if owned else self._connection
            if conn.autocommit:
                conn.autocommit = False
            # psycopg issues BEGIN with the first statement (autocommit is off)
            self._local.transaction = _Transaction(conn, owned)
            logger.debug("PostgreSQL transaction initiated")
        except DatabaseError as e:
            if owned and conn is not None:
                self._connection_pool.putconn(conn)
            logger.error("Failed to begin transaction: %s", e)
            raise ConnectionError(f"Failed to begin transaction: {e}")

    def commit(self) -> None:
        self.ensure_connected()

        if self._autocommit:
            logger.debug("Autocommit enabled; explicit commit has no effect")
            return

        transaction = self._transaction_state()
        if transaction is None:  # nothing begun through begin_transaction()
            try:
                with self._connection_context() as conn:
                    conn.commit()
            except DatabaseError as e:
                logger.error("Failed to commit transaction: %s", e)
                raise ConnectionError(f"Failed to commit transaction: {e}")
            return

        if transaction.depth > 1:
            transaction.depth -= 1
            return

        try:
            if transaction.rollback_only:
                logger.warning("Rolling back: a nested transaction block failed")
                transaction.conn.rollback()
            else:
                transaction.conn.commit()
            logger.debug("Committed PostgreSQL transaction")
        except DatabaseError as e:
            logger.error("Failed to commit transaction: %s", e)
            raise ConnectionError(f"Failed to commit transaction: {e}")
        finally:
            self._end_transaction(transaction)

    def rollback(self) -> None:
        self.ensure_connected()

        if self._autocommit:
            logger.debug("Autocommit enabled; explicit rollback has no effect")
            return

        transaction = self._transaction_state()
        if transaction is None:
            try:
                with self._connection_context() as conn:
                    conn.rollback()
            except DatabaseError as e:
                logger.error("Failed to rollback transaction: %s", e)
                raise ConnectionError(f"Failed to rollback transaction: {e}")
            return

        if transaction.depth > 1:
            # The outer block decides; it will roll back instead of committing.
            transaction.depth -= 1
            transaction.rollback_only = True
            return

        try:
            transaction.conn.rollback()
            logger.debug("Rolled back PostgreSQL transaction")
        except DatabaseError as e:
            logger.error("Failed to rollback transaction: %s", e)
            raise ConnectionError(f"Failed to rollback transaction: {e}")
        finally:
            self._end_transaction(transaction)

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
                if not self._autocommit and self._transaction_state() is None:
                    conn.commit()  # inside transaction() the caller commits
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
