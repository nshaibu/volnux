import logging
from enum import Enum
from contextlib import contextmanager
from types import UnionType
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    TypeVar,
    TYPE_CHECKING,
)

from psycopg.types.json import Jsonb
from yoyo import get_backend

from volnux.backends.store import (
    KeyValueStoreBackendBase,
    YoyoMigrationsMixin,
    SQLDialect,
)
from volnux.exceptions import (
    ObjectDoesNotExist,
    ObjectExistError,
    SqlOperationError,
    SerializationError,
)
from volnux.backends.connectors.postgres import PostgresConnector
from volnux.backends.db_utils import _safe_name

logger = logging.getLogger("volnux.backends.postgres")


def _column_list(columns: List[str]) -> str:
    """Comma-separated quoted column names for INSERT column lists."""
    return ", ".join(_safe_name(col) for col in columns)


if TYPE_CHECKING:
    from volnux.result.stream import ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

T = TypeVar("T", bound="KeyValueStoreIntegrationMixin")


class PostgresStoreBackend(
    YoyoMigrationsMixin,
    KeyValueStoreBackendBase,
):
    """PostgreSQL-backed key-value store implementation.

    This backend uses PostgreSQL tables to store records, with automatic schema
    creation and yoyo migration support. Records are serialized and stored in a
    JSONB column for fidelity, while individual fields are also stored as typed
    columns for efficient querying. Each model class maps to a table named
    after the schema name (usually 'volnux_{ClassName}').

    Features:
        - JSONB for serialized record state (with GIN indexing)
        - Typed columns for filterable fields
        - Native UPSERT (INSERT ... ON CONFLICT)
        - Connection pooling via PostgresConnector
        - Transaction support with savepoints
        - Django-style filter lookups (__gt, __contains, __in, etc.)
        - Yoyo migration support for schema evolution
        - Schema caching to avoid repeated information_schema queries

    Example:
        >>> backend = PostgresStoreBackend(
        ...     host="localhost",
        ...     port=5432,
        ...     database="volnux",
        ...     username="volnux",
        ...     password="secure_password",
        ...     use_pooling=True,
        ...     min_pool_size=2,
        ...     max_pool_size=10,
        ... )
        >>> backend.insert("volnux_Workflow", "wf_1", workflow_record)
        >>> workflow = backend.get("volnux_Workflow", "wf_1", Workflow)
        >>> active = backend.filter("volnux_Workflow", Workflow, status="active")
    """

    NAMESPACE_SEPARATOR = "_"

    connector_klass = PostgresConnector

    dialect = SQLDialect.POSTGRES

    # PostgreSQL-specific type mapping
    TYPE_MAPPING = {
        bool: "BOOLEAN",
        int: "BIGINT",
        float: "DOUBLE PRECISION",
        str: "TEXT",
        bytes: "BYTEA",
        dict: "JSONB",
        list: "JSONB",
    }

    def __init__(self, **connector_config: Any) -> None:
        """Initialize the PostgreSQL store backend.

        Args:
            **connector_config: Configuration passed to PostgresConnector.
                Required: host, database, username, password
                Optional: port (default 5432), use_pooling (default True),
                         min_pool_size, max_pool_size, ssl_mode, etc.
        """
        super().__init__(**connector_config)

        self._ensure_connected()
        self._id_field_length = 255

        # Schema existence cache to avoid repeated information_schema queries
        self._schema_cache: Dict[str, bool] = {}

    @contextmanager
    def _get_cursor(self):
        """Context manager for obtaining a cursor from the connector.

        Yields:
            A psycopg cursor. The cursor is closed after the context exits.
        """
        self._ensure_connected()
        cursor = self.connector.get_cursor()
        try:
            yield cursor
        finally:
            # self.connector.return_cursor(cursor)
            cursor.close()

    @contextmanager
    def _transaction(self):
        """Context manager for explicit transactions.

        Uses the connector's native transaction support. If autocommit is
        enabled, the context manager still functions but without explicit
        BEGIN/COMMIT semantics.
        """
        self._ensure_connected()
        with self.connector.transaction():
            yield

    def _prepare_record_data(
        self, record: "KeyValueStoreIntegrationMixin", record_key: str
    ) -> Dict[str, Any]:
        """Prepare record data, wrapping dicts/lists for JSONB columns.

        psycopg 3 has no default adapter for dict, and adapts list to an
        ARRAY, so both are wrapped in ``Jsonb`` to match the JSONB columns
        compile_schema creates for them.
        """
        record_data = super()._prepare_record_data(record, record_key)
        for field_name, value in record_data.items():
            if isinstance(value, (dict, list)):
                record_data[field_name] = Jsonb(value)
        return record_data

    def execute_query(
        self, query: str, *args: Any, fetch_method: Optional[str] = "fetchall"
    ) -> Any:
        """Execute a query in a transaction and fetch its result.

        Args:
            query: SQL with ``%s`` placeholders.
            *args: Arguments forwarded to ``cursor.execute`` (e.g. a params tuple).
            fetch_method: ``"fetchall"``, ``"fetchone"``, or None for no fetch.

        Raises:
            SqlOperationError: If the query fails.
        """
        try:
            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(query, *args)
                    if fetch_method == "fetchall":
                        return cursor.fetchall()
                    if fetch_method == "fetchone":
                        return cursor.fetchone()
                    return None
        except Exception as e:
            logger.error("Error executing query: %s", e)
            raise SqlOperationError(f"Error executing query: {e}") from e

    def _get_migration_backend(self) -> Any:
        # The connector uses psycopg 3; yoyo's plain postgres:// backend
        # requires psycopg2, so select yoyo's psycopg 3 backend explicitly.
        uri = self.connector.get_uri()
        scheme, sep, rest = uri.partition("://")
        if sep and scheme in ("postgres", "postgresql", "psql"):
            uri = f"postgresql+psycopg://{rest}"
        return get_backend(uri)

    def _invalidate_schema_cache(self, schema_name: Optional[str] = None) -> None:
        """Invalidate the schema cache.

        Args:
            schema_name: Specific schema to invalidate, or None for all.
        """
        if schema_name:
            self._schema_cache.pop(schema_name, None)
        else:
            self._schema_cache.clear()

    def get_cached_table_columns(self, schema_name: str) -> Dict[str, str]:
        """
        Queries the backend system catalogs to fetch the live columns
        and data types for a given table name.

        Returns:
            A dictionary mapping column name string to data type string.
            Returns an empty dict {} if the table does not exist.
        """
        self._ensure_connected()
        columns_map: Dict[str, str] = {}

        try:
            postgres_catalog_sql = """
                                   SELECT column_name, data_type, character_maximum_length
                                   FROM information_schema.columns
                                   WHERE table_name = %s \
                                     AND table_schema = current_schema(); \
                                   """

            rows = (
                self.execute_query(
                    postgres_catalog_sql, (schema_name,), fetch_method="fetchall"
                )
                or []
            )

            for row in rows:
                if isinstance(row, dict):
                    col_name = row["column_name"]
                    base_type = row["data_type"].upper()
                    max_len = row["character_maximum_length"]
                else:
                    col_name = row[0]
                    base_type = row[1].upper()
                    max_len = row[2]

                # Standardize structural types like VARCHAR(N) for accurate string comparison
                if base_type in ("CHARACTER VARYING", "VARCHAR") and max_len:
                    col_type = f"VARCHAR({max_len})"
                else:
                    col_type = base_type

                columns_map[col_name] = col_type

            return columns_map
        except Exception as e:
            logger.error(
                "Failed to extract schema system catalog column specs for '%s': %s",
                schema_name,
                e,
            )
            return {}

    def schema_exists(self, schema_name: str) -> bool:
        """Check if a schema (table) exists in the database.

        Uses PostgreSQL's information_schema for reliable cross-version
        compatibility. Results are cached to avoid repeated queries.

        Args:
            schema_name: The name of the schema (table) to check.

        Returns:
            True if the schema exists, False otherwise.
        """
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]

        self._ensure_connected()

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s",
                    (schema_name,),
                )
                exists = cursor.fetchone() is not None

            self._schema_cache[schema_name] = exists
            return exists
        except Exception as e:
            logger.error("Error checking schema existence for '%s': %s", schema_name, e)
            return False

    def map_python_type_to_sql(self, field_type: Any) -> str:
        """Map a Python/Formax field type to a PostgreSQL column type.

        Handles Optional types by unwrapping Union/Optional before mapping.

        Args:
            field_type: The type annotation from the model field.

        Returns:
            PostgreSQL column type string (e.g., 'TEXT', 'BIGINT', 'JSONB').
        """
        # Unwrap Optional[X] / Union[X, None]
        origin = get_origin(field_type)
        if origin in (Union, UnionType):
            args = get_args(field_type)
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                field_type = non_none[0]

        # Map to PostgreSQL type
        mapped = self.TYPE_MAPPING.get(field_type, "TEXT")

        # For enums, use TEXT (native PostgreSQL enums require CREATE TYPE)
        if isinstance(field_type, type) and issubclass(field_type, Enum):
            mapped = "TEXT"

        return mapped

    # def _is_optional_field(self, field_type: Any) -> bool:
    #     """Check if a field type allows NULL values.
    #
    #     Args:
    #         field_type: The type annotation from the model field.
    #
    #     Returns:
    #         True if the field can be NULL, False otherwise.
    #     """
    #     origin = get_origin(field_type)
    #     if origin in (Union, UnionType):
    #         return type(None) in get_args(field_type)
    #     return False

    # def create_schema(
    #     self,
    #     schema_name: str,
    #     record_class: Type["KeyValueStoreIntegrationMixin"],
    #     **kwargs,
    # ) -> None:
    #     """Create a table with native FK, datetime, and list column support.
    #
    #     Uses depth-first creation for FK dependencies. For a true mutual
    #     reference (A <-> B), the native FK constraint on *both* sides is
    #     deferred and added via ALTER TABLE once every schema in the
    #     dependency tree exists, so the resulting schema is symmetric no
    #     matter which model was compiled first. No _record_state column.
    #     """
    #     from ..fields import OnDelete
    #
    #     is_top_level = "_creating" not in kwargs
    #     _creating: Set[str] = kwargs.get("_creating", set())
    #     _deferred_fks: List[Tuple[str, str, str, str]] = kwargs.get("_deferred_fks", [])
    #     _mutual_deferred: Dict[str, Set[str]] = kwargs.get("_mutual_deferred", {})
    #
    #     self._ensure_connected()
    #
    #     if schema_name in _creating:
    #         return
    #
    #     _creating.add(schema_name)
    #
    #     try:
    #         columns = [
    #             f"{_safe_name('id')} VARCHAR({self._id_field_length}) PRIMARY KEY"
    #         ]
    #         fk_constraints: List[str] = []
    #         chk_constraints: List[str] = []
    #         fk_columns_to_index: List[str] = []
    #         jsonb_columns: List[str] = []
    #
    #         record_type_hints = get_type_hints(record_class)
    #
    #         for field_name, field_type in record_type_hints.items():
    #             if field_name.startswith("_"):
    #                 continue
    #
    #             field_type, attrib = self.decompose_field_type(field_type)
    #             metadata = attrib.metadata if attrib else {}
    #             meta_field_type = metadata.get("type")
    #
    #             is_unique = metadata.get("unique", False)
    #             is_optional = self._is_optional_field(field_type) or metadata.get(
    #                 "nullable", False
    #             )
    #
    #             if meta_field_type == "foreignkey":
    #                 has_native_fk = metadata.get("has_native_fk", False)
    #
    #                 target_model: Optional[Type["KeyValueStoreIntegrationMixin"]] = (
    #                     metadata.get("target_model")
    #                 )
    #                 if target_model is None:
    #                     raise ValueError(f"Target model missing for FK '{field_name}'")
    #
    #                 col_name = f"{field_name}_object_id"
    #                 fk_columns_to_index.append(col_name)
    #
    #                 if has_native_fk:
    #                     target_schema = self.resolve_physical_target(
    #                         target_model.get_storage_route()
    #                     )
    #                     col_def = f"{col_name} VARCHAR({self._id_field_length})" + (
    #                         "" if is_optional else " NOT NULL"
    #                     )
    #                     columns.append(col_def)
    #
    #                     on_delete: "OnDelete" = metadata.get(
    #                         "on_delete", OnDelete.PROTECT
    #                     )
    #                     on_delete_sql = self._on_delete_constraints_map().get(
    #                         on_delete, "NO ACTION"
    #                     )
    #
    #                     if target_schema in _creating:
    #                         logger.warning(
    #                             "Mutual FK dependency: %s <-> %s. Native FK for "
    #                             "'%s' will be added via ALTER TABLE once both "
    #                             "schemas exist.",
    #                             schema_name,
    #                             target_schema,
    #                             field_name,
    #                         )
    #                         _deferred_fks.append(
    #                             (schema_name, col_name, target_schema, on_delete_sql)
    #                         )
    #                         _mutual_deferred.setdefault(target_schema, set()).add(
    #                             schema_name
    #                         )
    #                         continue
    #
    #                     if not self.schema_exists(target_schema):
    #                         self.create_schema(
    #                             target_schema,
    #                             target_model,
    #                             _creating=_creating,
    #                             _deferred_fks=_deferred_fks,
    #                             _mutual_deferred=_mutual_deferred,
    #                         )
    #                         logger.info(
    #                             "Auto-created schema '%s' required by FK '%s.%s'",
    #                             target_schema,
    #                             record_class.__name__,
    #                             field_name,
    #                         )
    #
    #                     if target_schema in _mutual_deferred.get(schema_name, ()):
    #                         # The nested call above discovered that this pair
    #                         # forms a mutual FK cycle; defer this direction too
    #                         # so both sides get their native FK symmetrically
    #                         # via ALTER TABLE instead of only one of them.
    #                         _deferred_fks.append(
    #                             (schema_name, col_name, target_schema, on_delete_sql)
    #                         )
    #                         continue
    #
    #                     fk_constraints.append(
    #                         f"FOREIGN KEY ({_safe_name(col_name)}) REFERENCES {_safe_name(target_schema)}({_safe_name('id')}) "
    #                         f"ON DELETE {on_delete_sql}"
    #                     )
    #                 else:
    #                     # Software-enforced FK: Formax-Py descriptor serializes
    #
    #                     columns.append(
    #                         f"{_safe_name(col_name)} VARCHAR({self._id_field_length})"
    #                         + ("" if is_optional else " NOT NULL")
    #                     )
    #                 continue
    #
    #             if meta_field_type == "list":
    #                 col_def = f"{_safe_name(field_name)} JSONB" + (
    #                     "" if is_optional else " NOT NULL"
    #                 )
    #                 columns.append(col_def)
    #                 jsonb_columns.append(field_name)
    #                 continue
    #
    #             if meta_field_type == "datetime":
    #                 is_timezone_aware: bool = metadata.get("tz_aware", False)
    #                 pg_type = "TIMESTAMPTZ" if is_timezone_aware else "TIMESTAMP"
    #             elif meta_field_type == "date":
    #                 pg_type = "DATE"
    #             else:
    #                 pg_type = self._map_field_to_pg_type(field_type)
    #
    #                 if pg_type == "TEXT":
    #                     if attrib:
    #                         if attrib.min_length and attrib.max_length:
    #                             pg_type = f"VARCHAR({attrib.max_length})"
    #                         elif attrib.min_length:
    #                             pg_type = f"TEXT"
    #                         elif attrib.max_length:
    #                             pg_type = f"VARCHAR({attrib.max_length})"
    #
    #             col_def = f"{_safe_name(field_name)} {pg_type}"
    #             if not is_optional:
    #                 col_def += " NOT NULL"
    #             if is_unique:
    #                 col_def += " UNIQUE"
    #
    #             columns.append(col_def)
    #
    #             # Gather attribute constraints
    #             if attrib:
    #                 if pg_type in ("TEXT", "VARCHAR") or pg_type.startswith("VARCHAR("):
    #                     if attrib.min_length and attrib.max_length:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'minmax_length')} CHECK (LENGTH({_safe_name(field_name)}) >= {attrib.min_length} AND LENGTH({_safe_name(field_name)}) <= {attrib.max_length})"
    #                         )
    #                     elif attrib.min_length:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'min_length')} CHECK (LENGTH({_safe_name(field_name)}) >= {attrib.min_length})"
    #                         )
    #                     elif attrib.max_length:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'max_length')} CHECK (LENGTH({_safe_name(field_name)}) <= {attrib.max_length})"
    #                         )
    #
    #                     # Handle Regex Pattern Matching
    #                     if attrib.pattern is not None:
    #                         # Extract raw string pattern if it is a compiled re.Pattern object
    #                         if hasattr(attrib.pattern, "pattern"):
    #                             raw_pattern = attrib.pattern.pattern
    #                             # Check if re.IGNORECASE (value 2) flag is applied
    #                             is_case_insensitive = bool(attrib.pattern.flags & 2)
    #                         else:
    #                             raw_pattern = str(attrib.pattern)
    #                             is_case_insensitive = False
    #
    #                         # Clean Python anchoring syntax (\A, \Z) to match POSIX standard (^, $)
    #                         raw_pattern = raw_pattern.replace(r"\A", "^").replace(
    #                             r"\Z", "$"
    #                         )
    #
    #                         # Escape single quotes in pattern strings to maintain valid SQL structures
    #                         escaped_pattern = raw_pattern.replace("'", "''")
    #                         regex_op = "~*" if is_case_insensitive else "~"
    #
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'pattern')} CHECK ({_safe_name(field_name)} {regex_op} '{escaped_pattern}')"
    #                         )
    #
    #                 elif pg_type in (
    #                     "INT",
    #                     "INTEGER",
    #                     "BIGINT",
    #                     "SMALLINT",
    #                     "NUMERIC",
    #                     "REAL",
    #                     "DOUBLE PRECISION",
    #                 ):
    #                     numeric_checks = []
    #
    #                     if attrib.gt is not None:
    #                         numeric_checks.append(
    #                             f"{_safe_name(field_name)} > {attrib.gt}"
    #                         )
    #                     if attrib.ge is not None:
    #                         numeric_checks.append(
    #                             f"{_safe_name(field_name)} >= {attrib.ge}"
    #                         )
    #                     if attrib.lt is not None:
    #                         numeric_checks.append(
    #                             f"{_safe_name(field_name)} < {attrib.lt}"
    #                         )
    #                     if attrib.le is not None:
    #                         numeric_checks.append(
    #                             f"{_safe_name(field_name)} <= {attrib.le}"
    #                         )
    #
    #                     if numeric_checks:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'numeric')} CHECK ({' AND '.join(numeric_checks)})"
    #                         )
    #
    #         columns.extend(chk_constraints)
    #         columns.extend(fk_constraints)
    #         create_sql = f"CREATE TABLE IF NOT EXISTS {_safe_name(schema_name)} ({', '.join(columns)})"
    #
    #         with self._transaction():
    #             with self._get_cursor() as cursor:
    #                 cursor.execute(create_sql)
    #
    #                 # Proactively provision indexes for all gathered FK columns
    #                 for col in fk_columns_to_index:
    #                     try:
    #                         idx_name = _generate_constraint_name(
    #                             schema_name, "idx", col
    #                         )
    #                         cursor.execute(
    #                             f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema_name} ({_safe_name(col)})"
    #                         )
    #                     except Exception:
    #                         pass  # ignore index creation
    #
    #                 # Index all JSONB
    #                 for col in jsonb_columns:
    #                     try:
    #                         idx_name = _generate_constraint_name(
    #                             schema_name, "idx", col
    #                         )
    #                         index_sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema_name} USING GIN ({_safe_name(col)})"
    #                         cursor.execute(index_sql)
    #                     except Exception:
    #                         pass
    #
    #         self._invalidate_schema_cache(schema_name)
    #         logger.info(
    #             "Created schema '%s' with %d columns", schema_name, len(columns)
    #         )
    #
    #     except Exception as e:
    #         logger.error("Error creating schema '%s': %s", schema_name, e)
    #         raise SqlOperationError(
    #             f"Error creating schema '{schema_name}': {e}"
    #         ) from e
    #
    #     finally:
    #         _creating.discard(schema_name)
    #
    #     if is_top_level and _deferred_fks:
    #         self._apply_deferred_foreign_keys(_deferred_fks)
    #         _deferred_fks.clear()

    # def _apply_deferred_foreign_keys(
    #     self, deferred_fks: List[Tuple[str, str, str, str]]
    # ) -> None:
    #     """Add native FK constraints deferred due to mutual schema cycles.
    #
    #     Called once every schema in a create_schema() call tree has been
    #     created, so both sides of a mutual FK reference (A <-> B) end up
    #     with a real, symmetric database-enforced constraint via ALTER
    #     TABLE rather than one side silently falling back to software
    #     enforcement.
    #
    #     Args:
    #         deferred_fks: List of (schema_name, col_name, target_schema,
    #             on_delete_sql) tuples describing the FK to add.
    #
    #     Raises:
    #         SqlOperationError: If adding a constraint fails.
    #     """
    #     for schema_name, col_name, target_schema, on_delete_sql in deferred_fks:
    #         constraint_name = f"fk_{schema_name}_{col_name}"
    #         try:
    #             with self._transaction():
    #                 with self._get_cursor() as cursor:
    #                     cursor.execute(
    #                         f"ALTER TABLE {schema_name} "
    #                         f"ADD CONSTRAINT {constraint_name} "
    #                         f"FOREIGN KEY ({col_name}) REFERENCES {target_schema}(id) "
    #                         f"ON DELETE {on_delete_sql}"
    #                     )
    #             logger.info(
    #                 "Added deferred FK constraint '%s' on %s.%s -> %s",
    #                 constraint_name,
    #                 schema_name,
    #                 col_name,
    #                 target_schema,
    #             )
    #         except Exception as e:
    #             logger.error(
    #                 "Error adding deferred FK constraint on %s.%s -> %s: %s",
    #                 schema_name,
    #                 col_name,
    #                 target_schema,
    #                 e,
    #             )
    #             raise SqlOperationError(
    #                 f"Error adding deferred FK constraint on "
    #                 f"{schema_name}.{col_name}: {e}"
    #             ) from e

    def drop_schema(self, schema_name: str) -> None:
        """Drop a schema (table) from the database.

        This is a destructive operation. Use with caution.

        Args:
            schema_name: The name of the schema to drop.

        Raises:
            SqlOperationError: If schema drop fails.
        """
        self._ensure_connected()

        try:
            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(
                        f"DROP TABLE IF EXISTS {_safe_name(schema_name)} CASCADE"
                    )

            self._invalidate_schema_cache(schema_name)
            logger.info("Dropped schema '%s'", schema_name)

        except Exception as e:
            logger.error("Error dropping schema '%s': %s", schema_name, e)
            raise SqlOperationError(f"Error dropping schema '{schema_name}': {e}")

    def list_schemas(self) -> List[str]:
        """List all Volnux schemas (tables) in the database.

        Returns:
            List of schema names matching the 'volnux_%' pattern.
        """
        self._ensure_connected()

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name LIKE 'volnux_%' "
                    "ORDER BY table_name"
                )
                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.error("Error listing schemas: %s", e)
            raise SqlOperationError(f"Error listing schemas: {e}")

    def supports_foreign_keys(self) -> bool:
        """Check if the backend supports foreign key constraints.

        Returns:
            True if foreign key constraints are supported, False otherwise.
        """
        return True

    def has_fk_constraint(self, schema_name: str, column_name: str) -> bool:
        """Check if a foreign key constraint exists for a given column.

        Args:
            schema_name: The schema containing the column.
            column_name: The name of the column to check.

        Returns:
            True if a foreign key constraint exists for the column, False otherwise.
        """
        with self._get_cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM information_schema.key_column_usage kcu "
                "JOIN information_schema.table_constraints tc "
                "ON kcu.constraint_name = tc.constraint_name "
                "WHERE tc.table_name = %s AND kcu.column_name = %s "
                "AND tc.constraint_type = 'FOREIGN KEY' LIMIT 1",
                (schema_name, column_name),
            )
            return cursor.fetchone() is not None

    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to check.

        Returns:
            True if the record exists, False otherwise.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            return False

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    f"SELECT 1 FROM {_safe_name(schema_name)} WHERE id = %s LIMIT 1",
                    (record_key,),
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error("Error checking record existence: %s", e)
            return False

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """
        Inserts a record into the specified schema in the database. If the record already exists, it raises
        an `ObjectExistError`. This operation ensures that the schema is prepared before the insertion and
        executes the insertion atomically. Logs warnings and errors as required.

        :param schema_name: The name of the schema where the record should be inserted.
        :param record_key: The unique key associated with the record.
        :param record: The record instance to be inserted, adhering to the KeyValueStoreIntegrationMixin.
        :param ttl: Optional time-to-live for the record. Ignored as TTL is not supported for PostgreSQL.
        :return: None
        :raises ObjectExistError: If the record with the given key already exists in the specified schema.
        :raises SerializationError: If an unexpected serialization error occurs during the operation.
        :raises SqlOperationError: For any other errors encountered during the insert operation.
        """
        if ttl is not None:
            logger.warning("TTL not supported for PostgreSQL; ignoring")

        try:
            self.ensure_schema(schema_name, record.__class__)
            record_data = self._prepare_record_data(record, record_key)
            cols = list(record_data.keys())
            placeholders = [f"%({c})s" for c in cols]

            sql = (
                f"INSERT INTO {_safe_name(schema_name)} ({_column_list(cols)}) "
                f"VALUES ({', '.join(placeholders)}) "
                f"ON CONFLICT (id) DO NOTHING"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(sql, record_data)
                    if cursor.rowcount == 0:
                        raise ObjectExistError(
                            f"Record '{record_key}' already exists in schema '{schema_name}'"
                        )

            logger.debug("Inserted '%s' into '%s'", record_key, schema_name)
        except ObjectExistError:
            raise
        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error inserting record: %s", e)
            raise SqlOperationError(f"Error inserting record: {e}") from e

    def update(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
    ) -> None:
        """Update an existing record in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to update.
            record: The updated record object.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If serialization fails.
            SqlOperationError: If the update fails.
        """
        self._ensure_connected()

        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            record_data = self._prepare_record_data(record, record_key)

            # Build SET clause excluding the id column
            set_columns = [col for col in record_data.keys() if col != "id"]
            set_clause = ", ".join(
                [f"{_safe_name(col)} = %({col})s" for col in set_columns]
            )

            update_sql = (
                f"UPDATE {_safe_name(schema_name)} SET {set_clause} WHERE id = %(id)s"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(update_sql, record_data)

                    if cursor.rowcount == 0:
                        raise ObjectDoesNotExist(
                            f"Record '{record_key}' does not exist in schema '{schema_name}'"
                        )

            logger.debug("Updated record '%s' in schema '%s'", record_key, schema_name)

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error updating record: %s", e)
            raise SqlOperationError(f"Error updating record: {e}")

    def upsert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
    ) -> None:
        """Insert or update a record (upsert operation).

        Uses PostgreSQL's native INSERT ... ON CONFLICT for atomic upsert.

        Args:
            schema_name: The schema for the operation.
            record_key: The key of the record.
            record: The record object to upsert.

        Raises:
            SerializationError: If serialization fails.
            SqlOperationError: If upsert fails.
        """
        self._ensure_connected()

        try:
            # Auto-create schema if it doesn't exist
            self.ensure_schema(schema_name, record.__class__)

            record_data = self._prepare_record_data(record, record_key)
            columns = list(record_data.keys())
            placeholders = [f"%({col})s" for col in columns]

            # Build UPDATE SET for conflict resolution
            update_columns = [col for col in columns if col != "id"]
            update_clause = ", ".join(
                [
                    f"{_safe_name(col)} = EXCLUDED.{_safe_name(col)}"
                    for col in update_columns
                ]
            )

            upsert_sql = (
                f"INSERT INTO {_safe_name(schema_name)} ({_column_list(columns)}) "
                f"VALUES ({', '.join(placeholders)}) "
                f"ON CONFLICT (id) DO UPDATE SET {update_clause}"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(upsert_sql, record_data)

            logger.debug("Upserted record '%s' in schema '%s'", record_key, schema_name)

        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error upserting record: %s", e)
            raise SqlOperationError(f"Error upserting record: {e}")

    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SqlOperationError: If deletion fails.
        """
        self._ensure_connected()

        try:
            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM {_safe_name(schema_name)} WHERE id = %s",
                        (record_key,),
                    )

                    if cursor.rowcount == 0:
                        raise ObjectDoesNotExist(
                            f"Record '{record_key}' does not exist in schema '{schema_name}'"
                        )

            logger.debug(
                "Deleted record '%s' from schema '%s'", record_key, schema_name
            )

        except ObjectDoesNotExist:
            raise
        except Exception as e:
            logger.error("Error deleting record: %s", e)
            raise SqlOperationError(f"Error deleting record: {e}")

    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Retrieve a single record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to retrieve.
            record_klass: The class to instantiate the record with.

        Returns:
            The record instance.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If retrieval fails.
        """
        self._ensure_connected()

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    f"SELECT * FROM {_safe_name(schema_name)} WHERE id = %s",
                    (record_key,),
                )
                row = cursor.fetchone()
                # Map columns while the cursor is open: psycopg clears
                # cursor.description once the cursor is closed.
                row_dict = (
                    None
                    if row is None
                    else self._sqlite_row_and_tuple_to_dict(row, cursor)
                )

            if row_dict is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            return self._deserialize_record(row_dict, record_klass)
        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error getting record: %s", e)
            raise SqlOperationError(f"Error getting record: {e}")

    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> "KeyValueStoreIntegrationMixin":
        """Reload a record's data from the backend.

        Args:
            schema_name: The schema containing the record.
            record: The record to reload.

        Returns:
            The reloaded record instance.

        Raises:
            ObjectDoesNotExist: If the record no longer exists.
            SerializationError: If deserialization fails.
            SqlOperationError: If reload fails.
        """
        fresh = self.get(schema_name, record.id, record.__class__)
        if not fresh:
            raise ObjectDoesNotExist(
                f"Record '{record.id}' no longer exists in schema '{schema_name}'"
            )
        record.__setstate__(fresh.__getstate__())
        return record

    @staticmethod
    def _escape_like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _build_sql_filter(
        self, filter_kwargs: Dict[str, Any], table: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """Build a WHERE clause from Django-style lookups.

        Column names are quoted (and qualified with ``table`` when given, to
        stay unambiguous in JOINs). Quoting also preserves case and keyword
        names, and stops filter keys from injecting SQL.
        """
        if not filter_kwargs:
            return "TRUE", []

        conditions: List[str] = []
        parameters: List[Any] = []

        def column(name: str) -> str:
            quoted = _safe_name(name)
            return f"{_safe_name(table)}.{quoted}" if table else quoted

        for key, value in filter_kwargs.items():
            if "__" in key:
                raw_field, operator = key.rsplit("__", 1)
                field = column(raw_field)

                if operator == "in":
                    if not isinstance(value, (list, tuple)) or not value:
                        conditions.append("FALSE")
                        continue
                    placeholders = ", ".join(["%s"] * len(value))
                    conditions.append(f"{field} IN ({placeholders})")
                    parameters.extend(value)

                elif operator == "isnull":
                    conditions.append(f"{field} IS {'NULL' if value else 'NOT NULL'}")

                elif operator in (
                    "contains",
                    "icontains",
                    "startswith",
                    "istartswith",
                    "endswith",
                    "iendswith",
                ):
                    escaped = self._escape_like(str(value))
                    ilike = operator.startswith("i")
                    kw = "ILIKE" if ilike else "LIKE"

                    if "startswith" in operator:
                        pattern = f"{escaped}%"
                    elif "endswith" in operator:
                        pattern = f"%{escaped}"
                    else:
                        pattern = f"%{escaped}%"

                    conditions.append(f"{field} {kw} %s ESCAPE '\\'")
                    parameters.append(pattern)

                elif operator == "exact":
                    conditions.append(f"{field} = %s")
                    parameters.append(value)
                elif operator == "gt":
                    conditions.append(f"{field} > %s")
                    parameters.append(value)
                elif operator == "gte":
                    conditions.append(f"{field} >= %s")
                    parameters.append(value)
                elif operator == "lt":
                    conditions.append(f"{field} < %s")
                    parameters.append(value)
                elif operator == "lte":
                    conditions.append(f"{field} <= %s")
                    parameters.append(value)
                elif operator == "ne":
                    conditions.append(f"{field} != %s")
                    parameters.append(value)
                else:
                    logger.warning("Unknown filter operator: %s", operator)
                    conditions.append(f"{field} = %s")
                    parameters.append(value)
            else:
                if isinstance(value, Enum):
                    value = value.value
                conditions.append(f"{column(key)} = %s")
                parameters.append(value)

        return " AND ".join(conditions), parameters

    @staticmethod
    def _order_by_clause(order_by: Optional[str]) -> str:
        """``"-age"`` → ``ORDER BY "age" DESC``; quoted so it can't inject SQL."""
        if not order_by:
            return ""
        if order_by.startswith("-"):
            return f" ORDER BY {_safe_name(order_by[1:])} DESC"
        return f" ORDER BY {_safe_name(order_by)} ASC"

    def filter(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        **filter_kwargs: Any,
    ) -> "ResultStream[KeyValueStoreIntegrationMixin]":
        """Filter records matching the specified criteria.

        Results are deserialized from the _record_state JSONB column.

        Args:
            schema_name: The schema to filter within.
            record_klass: The class to instantiate records with.
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            order_by: Field to order by. Prefix with '-' for descending.
            **filter_kwargs: Field lookup filters.

        Returns:
            List of matching record instances.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If the query fails.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:
            where_clause, parameters = self._build_sql_filter(filter_kwargs)

            query = f"SELECT id FROM {_safe_name(schema_name)} WHERE {where_clause}"
            query += self._order_by_clause(order_by)

            if limit is not None:
                query += f" LIMIT %s"
                parameters.append(limit)
            if offset is not None:
                query += f" OFFSET %s"
                parameters.append(offset)

            with self._get_cursor() as cursor:
                cursor.execute(query, parameters)
                rows = cursor.fetchall()

            return self._create_result_stream(
                record_keys=[row[0] for row in rows], record_klass=record_klass
            )

        except ObjectDoesNotExist:
            raise
        except Exception as e:
            logger.error("Error filtering records: %s", e)
            raise SqlOperationError(f"Error filtering records: {e}")

    def filter_with_joins(
        self,
        schema_name: str,
        model_class: Type["KeyValueStoreIntegrationMixin"],
        join_fields: List[str],
        **filter_kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Resolve FK fields via SQL JOINs for native FKs, batch fetch for software FKs."""
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        # Classify each join field
        native_joins: List[Tuple[str, str, str]] = (
            []
        )  # (field_name, fk_column, target_schema)
        software_fk_columns: List[Tuple[str, str]] = []  # (field_name, fk_column)

        for field_name in join_fields:
            # Same column naming as compile_schema: "<field>_object_id"
            fk_col = (
                f"{field_name}_object_id"
                if not field_name.endswith("_object_id")
                else field_name
            )
            has_native = self.has_fk_constraint(schema_name, fk_col)

            if has_native:
                # Resolve target schema from FK constraint metadata
                target_schema = self._get_fk_target_schema(schema_name, fk_col)
                if target_schema:
                    native_joins.append((field_name, fk_col, target_schema))
                else:
                    software_fk_columns.append((field_name, fk_col))
            else:
                software_fk_columns.append((field_name, fk_col))

        # Build query with native JOINs. Postgres can't alias ``alias.*``, so
        # each target column is selected under ``<field>__resolved_<col>``.
        main = _safe_name(schema_name)
        select_parts = [f"{main}.*"]
        join_clauses: List[str] = []
        alias_map: Dict[str, str] = {}

        for i, (field_name, fk_col, target_schema) in enumerate(native_joins):
            alias = _safe_name(f"j_{i}")
            alias_map[field_name] = alias
            join_clauses.append(
                f"LEFT JOIN {_safe_name(target_schema)} AS {alias} "
                f'ON {main}.{_safe_name(fk_col)} = {alias}."id"'
            )
            for col in self.get_cached_table_columns(target_schema):
                select_parts.append(
                    f"{alias}.{_safe_name(col)} "
                    f"AS {_safe_name(f'{field_name}__resolved_{col}')}"
                )

        # Qualify filter columns: "id" etc. exist on both sides of a join
        where_clause, parameters = self._build_sql_filter(
            filter_kwargs, table=schema_name
        )

        query = f"SELECT {', '.join(select_parts)} FROM {main}"
        if join_clauses:
            query += " " + " ".join(join_clauses)
        if where_clause != "TRUE":
            query += f" WHERE {where_clause}"

        with self._get_cursor() as cursor:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

        results: List[Dict[str, Any]] = []
        for row in rows:
            record_dict = dict(zip(col_names, row))

            # Extract joined columns into nested resolved dicts. Popping them
            # before assigning "<field>__resolved" keeps the result from being
            # removed along with the flattened columns.
            for field_name in alias_map:
                prefix = f"{field_name}__resolved_"
                resolved_cols = {
                    k[len(prefix) :]: record_dict.pop(k)
                    for k in list(record_dict)
                    if k.startswith(prefix)
                }
                record_dict[f"{field_name}__resolved"] = (
                    resolved_cols if resolved_cols.get("id") is not None else None
                )

            results.append(record_dict)

        # Batch-resolve software FKs (single additional query per field)
        for field_name, fk_col in software_fk_columns:
            target_ids = list(
                {r.get(fk_col) for r in results if r.get(fk_col) is not None}
            )
            if not target_ids:
                continue

            # Determine target model from first non-null FK value's stored metadata
            # For software FKs, the column contains the self-describing dict
            target_model_class = self.resolve_target_model_for_field(
                model_class, field_name
            )
            if target_model_class is None:
                continue

            # get_schema_name() is async; resolve the route synchronously here
            target_schema = self.resolve_physical_target(
                target_model_class.get_storage_route()
            )
            placeholders = ", ".join(["%s"] * len(target_ids))

            with self._get_cursor() as cursor:
                cursor.execute(
                    f"SELECT * FROM {_safe_name(target_schema)} WHERE id IN ({placeholders})",
                    target_ids,
                )
                target_rows = cursor.fetchall()
                target_col_names = [desc[0] for desc in cursor.description]

            target_map = {
                dict(zip(target_col_names, tr))["id"]: dict(zip(target_col_names, tr))
                for tr in target_rows
            }

            for record_dict in results:
                fk_value = record_dict.get(fk_col)
                if isinstance(fk_value, dict):
                    obj_id = fk_value.get("data")
                elif isinstance(fk_value, str):
                    obj_id = fk_value
                else:
                    obj_id = None

                record_dict[f"{field_name}__resolved"] = target_map.get(obj_id)

        return results

    def _get_fk_target_schema(self, schema_name: str, fk_column: str) -> Optional[str]:
        """Get the target table name from a native FK constraint."""
        with self._get_cursor() as cursor:
            cursor.execute(
                "SELECT ccu.table_name "
                "FROM information_schema.key_column_usage kcu "
                "JOIN information_schema.table_constraints tc "
                "ON kcu.constraint_name = tc.constraint_name "
                "JOIN information_schema.constraint_column_usage ccu "
                "ON ccu.constraint_name = tc.constraint_name "
                "WHERE tc.table_name = %s AND kcu.column_name = %s "
                "AND tc.constraint_type = 'FOREIGN KEY' LIMIT 1",
                (schema_name, fk_column),
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def count(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            record_klass: The class to instantiate the record with.
            **filter_kwargs: Optional field lookup filters.

        Returns:
            The number of matching records.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SqlOperationError: If the count fails.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:
            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = (
                f"SELECT COUNT(*) FROM {_safe_name(schema_name)} WHERE {where_clause}"
            )

            with self._get_cursor() as cursor:
                cursor.execute(query, parameters)
                result = cursor.fetchone()

            return int(result[0]) if result else 0

        except Exception as e:
            logger.error("Error counting records: %s", e)
            raise SqlOperationError(f"Error counting records: {e}")

    def bulk_insert(
        self,
        schema_name: str,
        records: Dict[str, "KeyValueStoreIntegrationMixin"],
        ttl: Optional[int] = None,
    ) -> int:
        """Insert multiple records in a single transaction.

        Args:
            schema_name: The schema to insert into.
            records: Dictionary mapping record keys to record objects.
            ttl: Optional TTL (not implemented for PostgreSQL).

        Returns:
            Number of records inserted.

        Raises:
            SqlOperationError: If bulk insert fails.
        """
        self._ensure_connected()

        if not records:
            return 0

        if ttl is not None:
            logger.warning("TTL is not supported for PostgreSQL backend; ignoring")

        try:
            # Use the first record to ensure schema exists
            first_record = next(iter(records.values()))
            self.ensure_schema(schema_name, first_record.__class__)

            # Prepare all records
            all_data = []
            for record_key, record in records.items():
                record_data = self._prepare_record_data(record, record_key)
                all_data.append(record_data)

            if not all_data:
                return 0

            columns = list(all_data[0].keys())
            placeholders = [f"%({col})s" for col in columns]
            insert_sql = (
                f"INSERT INTO {_safe_name(schema_name)} ({_column_list(columns)}) "
                f"VALUES ({', '.join(placeholders)}) "
                f"ON CONFLICT (id) DO NOTHING"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.executemany(insert_sql, all_data)

            logger.info(
                "Bulk inserted %d records into schema '%s'", len(all_data), schema_name
            )
            return len(all_data)

        except Exception as e:
            logger.error("Error bulk inserting records: %s", e)
            raise SqlOperationError(f"Error bulk inserting records: {e}")

    def bulk_delete(self, schema_name: str, record_keys: List[str]) -> int:
        """Delete multiple records in a single transaction.

        Args:
            schema_name: The schema containing the records.
            record_keys: List of record keys to delete.

        Returns:
            Number of records deleted.

        Raises:
            SqlOperationError: If bulk delete fails.
        """
        self._ensure_connected()

        if not record_keys:
            return 0

        try:
            placeholders = ", ".join(["%s"] * len(record_keys))
            delete_sql = (
                f"DELETE FROM {_safe_name(schema_name)} WHERE id IN ({placeholders})"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(delete_sql, record_keys)
                    deleted = cursor.rowcount

            logger.info(
                "Bulk deleted %d records from schema '%s'", deleted, schema_name
            )
            return deleted

        except Exception as e:
            logger.error("Error bulk deleting records: %s", e)
            raise SqlOperationError(f"Error bulk deleting records: {e}")

    def clear_schema(self, schema_name: str) -> int:
        """Delete all records from a schema without dropping the table.

        Args:
            schema_name: The schema to clear.

        Returns:
            Number of records deleted.

        Raises:
            SqlOperationError: If the operation fails.
        """
        self._ensure_connected()

        try:
            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(f"DELETE FROM {_safe_name(schema_name)}")
                    deleted = cursor.rowcount

            logger.warning(
                "Cleared all %d records from schema '%s'", deleted, schema_name
            )
            return deleted

        except Exception as e:
            logger.error("Error clearing schema '%s': %s", schema_name, e)
            raise SqlOperationError(f"Error clearing schema '{schema_name}': {e}")

    def get_table_info(self, schema_name: str) -> Dict[str, Any]:
        """Get information about a schema/table.

        Args:
            schema_name: The schema name.

        Returns:
            Dictionary with table metadata.

        Raises:
            SqlOperationError: If the query fails.
        """
        self._ensure_connected()

        try:
            info: Dict[str, Any] = {"schema": schema_name, "exists": False}

            with self._get_cursor() as cursor:
                # Check existence
                cursor.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s",
                    (schema_name,),
                )
                if cursor.fetchone() is None:
                    return info

                info["exists"] = True

                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {_safe_name(schema_name)}")
                info["row_count"] = cursor.fetchone()[0]

                # Get column information
                cursor.execute(
                    "SELECT column_name, data_type, is_nullable, column_default "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (schema_name,),
                )
                info["columns"] = [
                    {
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] == "YES",
                        "default": row[3],
                    }
                    for row in cursor.fetchall()
                ]

                # Get index information
                cursor.execute(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE schemaname = 'public' AND tablename = %s",
                    (schema_name,),
                )
                info["indexes"] = [
                    {"name": row[0], "definition": row[1]} for row in cursor.fetchall()
                ]

                # Get table size
                cursor.execute(
                    "SELECT pg_total_relation_size(%s::regclass)",
                    (_safe_name(schema_name),),
                )
                info["total_size_bytes"] = cursor.fetchone()[0]

            return info

        except Exception as e:
            logger.error("Error getting table info for '%s': %s", schema_name, e)
            raise SqlOperationError(f"Error getting table info: {e}")

    def __repr__(self) -> str:
        connector_info = self.connector.get_connection_info() if self.connector else {}
        host = connector_info.get("host", "unknown")
        port = connector_info.get("port", "unknown")
        database = connector_info.get("database", "unknown")
        pooling = "pooled" if connector_info.get("pooling_enabled", False) else "direct"
        status = (
            "connected" if connector_info.get("is_connected", False) else "disconnected"
        )
        return f"<PostgresStoreBackend {host}:{port}/{database} [{pooling}, {status}]>"
