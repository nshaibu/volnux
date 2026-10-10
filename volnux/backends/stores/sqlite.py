import logging
import sqlite3
import orjson as json
from typing import (
    Any,
    Dict,
    List,
    Set,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    TYPE_CHECKING,
)

from formax.typing import get_type
from yoyo.backends.core.sqlite3 import SQLiteBackend
from yoyo.connections import default_migration_table, parse_uri

from volnux.backends.connectors.sqlite import (
    SqliteConnector,
    register_sqlite_functions,
)
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
from volnux.backends.db_utils import _safe_name

if TYPE_CHECKING:
    from volnux.result.stream import ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


logger = logging.getLogger(__name__)


class _SQLiteMigrationBackend(SQLiteBackend):
    """yoyo SQLite backend whose connections can evaluate generated schemas."""

    def init_connection(self, connection: sqlite3.Connection) -> None:
        super().init_connection(connection)
        register_sqlite_functions(connection)


class SqliteStoreBackend(YoyoMigrationsMixin, KeyValueStoreBackendBase):
    """SQLite-backed key-value store implementation.

    This backend uses SQLite tables to store records, with automatic schema
    creation and migration support. Records are serialized and
    stored in a dedicated column, while individual fields are also stored
    for efficient querying.

    Example:
        >>> backend = SqliteStoreBackend(database="myapp.db")
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", UserModel, "user_1")
        >>> active_users = backend.filter("users", UserModel, status="active")
    """

    NAMESPACE_SEPARATOR = "_"

    connector_klass = SqliteConnector

    dialect = SQLDialect.SQLITE

    def __init__(self, **connector_config: Any):
        """Initialize the SQLite store backend.

        Args:
            **connector_config: Configuration passed to SqliteConnector.
        """
        super().__init__(**connector_config)

        self._ensure_connected()
        self._id_field_length = 255

        self._schema_cache: Dict[str, bool] = {}

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
            # PRAGMA table_info returns rows: (cid, name, type, notnull, dflt_value, pk)
            pragma_sql = f"PRAGMA table_info({_safe_name(schema_name)});"

            rows = self.execute_query(pragma_sql, fetch_method="fetchall") or []

            for row in rows:
                # Support both tuple/list indices and row dict objects depending on row_factory
                if isinstance(row, dict):
                    col_name = row["name"]
                    col_type = row["type"].upper()
                else:
                    col_name = row[1]
                    col_type = row[2].upper()

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

        Args:
            schema_name: The name of the schema to check.

        Returns:
            True if the schema exists, False otherwise.
        """
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]

        self._ensure_connected()

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (schema_name,),
            )
            exists = cursor.fetchone() is not None
            cursor.close()

            self._schema_cache[schema_name] = exists
            return exists
        except sqlite3.Error as e:
            logger.error(f"Error checking schema existence: {e}")
            return False

    def map_python_type_to_sql(self, field_type: Any) -> str:
        """Map Python types to SQLite types.

        Args:
            field_type: The Python type annotation.

        Returns:
            The corresponding SQLite type as a string.
        """
        field_type = get_type(field_type)

        type_mapping = {
            bool: "BOOLEAN",
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bytes: "BLOB",
        }

        return type_mapping.get(field_type, "TEXT")  # Default to TEXT for JSON

    def _get_migration_backend(self) -> Any:
        # yoyo runs each migration on a fresh connection (backend.copy()), and
        # SQLite resolves CHECK functions such as REGEXP at CREATE TABLE time,
        # so register them via the per-connection init hook.
        backend = _SQLiteMigrationBackend(
            parse_uri(self.connector.get_uri()), default_migration_table
        )
        backend.init_database()
        return backend

    def _prepare_record_data(
        self, record: "KeyValueStoreIntegrationMixin", record_key: str
    ) -> Dict[str, Any]:
        """Prepare record data, JSON-encoding lists/dicts for TEXT columns.

        sqlite3 cannot bind list/dict parameters; _deserialize_record parses
        the JSON text back into Python objects on read.
        """
        record_data = super()._prepare_record_data(record, record_key)
        for field_name, value in record_data.items():
            if isinstance(value, (dict, list)):
                record_data[field_name] = json.dumps(value, default=str).decode()
        return record_data

    # def create_schema(
    #     self,
    #     schema_name: str,
    #     record_class: Type["KeyValueStoreIntegrationMixin"],
    #     **kwargs,
    # ) -> None:
    #     """
    #     Create a schema (table) based on a record's structure.
    #
    #     Uses a two-phase approach for FK dependencies: dependent schemas are
    #     created first (columns only, no FKs), then FK constraints are added
    #     inline when the referencing table is created. If a mutual reference
    #     is detected (A -> B -> A), the back-reference FK is omitted and
    #     falls back to software-level enforcement via _on_delete_hook.
    #
    #     Args:
    #         schema_name: The name of the schema to create.
    #         record_class: A sample record to derive the schema from.
    #         kwargs: Additional keyword arguments.
    #                 _creating: Internal set tracking schemas currently being created,
    #                             used to detect and break mutual FK dependency cycles.
    #
    #     Raises:
    #         SqlOperationError: If schema creation fails.
    #     """
    #     self._ensure_connected()
    #     self._ensure_foreign_keys_enabled()
    #
    #     # Create isolated tracking set at top-level call only
    #     _creating: Set[str] = kwargs.get("_creating", set())
    #
    #     # Mark this schema as in-progress before processing its fields,
    #     # so recursive calls for dependent schemas can detect the cycle.
    #     _creating.add(schema_name)
    #
    #     try:
    #         from ..fields import OnDelete
    #
    #         fields = [
    #             f"{_safe_name('id')} VARCHAR({self._id_field_length}) PRIMARY KEY"
    #         ]
    #         fk_constraints: List[str] = []
    #         chk_constraints: List[str] = []
    #         fk_columns_to_index: List[str] = []
    #
    #         record_type_hints = get_type_hints(record_class)
    #
    #         for field_name, field_type in record_type_hints.items():
    #             if field_name.startswith("_"):
    #                 continue
    #
    #             field_type, attrib = self.decompose_field_type(field_type)
    #
    #             metadata = attrib.metadata if attrib else {}
    #             meta_field_type = metadata.get("type")
    #             is_unique = metadata.get("unique", False)
    #             is_optional = self._is_optional_field(field_type) or metadata.get(
    #                 "nullable", False
    #             )
    #
    #             if meta_field_type == "foreignkey":
    #                 has_native_fk = metadata.get("has_native_fk", False)
    #                 col_name = (
    #                     f"{field_name}_object_id"
    #                     if not field_name.endswith("_object_id")
    #                     else field_name
    #                 )
    #                 fk_columns_to_index.append(col_name)
    #
    #                 if has_native_fk:
    #                     target_model: Optional[
    #                         Type["KeyValueStoreIntegrationMixin"]
    #                     ] = metadata.get("target_model")
    #                     if target_model is None:
    #                         raise ValueError(
    #                             f"Target model missing for FK '{field_name}'"
    #                         )
    #
    #                     target_schema = self.resolve_physical_target(
    #                         target_model.get_storage_route()
    #                     )
    #                     field_def = (
    #                         f"{_safe_name(col_name)} VARCHAR({self._id_field_length})"
    #                         + ("" if is_optional else " NOT NULL")
    #                     )
    #                     fields.append(field_def)
    #
    #                     if target_schema in _creating:
    #                         logger.warning(
    #                             "Mutual FK dependency: %s <-> %s. "
    #                             "Omitting native FK for '%s'; software enforcement active.",
    #                             schema_name,
    #                             target_schema,
    #                             field_name,
    #                         )
    #                         continue
    #
    #                     if not self.schema_exists(target_schema):
    #                         self.create_schema(
    #                             target_schema, target_model, _creating=_creating
    #                         )
    #                         logger.info(
    #                             f"Auto-created schema '{target_schema}' required by FK "
    #                             f"'{record_class.__name__}.{field_name}'"
    #                         )
    #
    #                     on_delete: "OnDelete" = metadata.get(
    #                         "on_delete", OnDelete.PROTECT
    #                     )
    #                     on_delete_sql = self._on_delete_constraints_map().get(
    #                         on_delete, "NO ACTION"
    #                     )
    #
    #                     fk_constraints.append(
    #                         f"FOREIGN KEY ({_safe_name(col_name)}) REFERENCES {_safe_name(target_schema)}({_safe_name('id')}) "
    #                         f"ON DELETE {on_delete_sql}"
    #                     )
    #                     continue
    #                 else:
    #                     # Software level Constraint. The data modeling layer will take care
    #                     # of all the data consistency rules set during modeling
    #                     field_def = (
    #                         f"{_safe_name(col_name)} VARCHAR({self._id_field_length})"
    #                         + ("" if is_optional else " NOT NULL")
    #                     )
    #                     fields.append(field_def)
    #
    #                 continue
    #             if meta_field_type == "list":
    #                 sql_type = "TEXT"
    #             elif meta_field_type in ["datetime", "date"]:
    #                 sql_type = "TEXT"
    #             else:
    #                 sql_type = self._map_python_type_to_sql(field_type)
    #
    #                 if sql_type == "TEXT":
    #                     if attrib:
    #                         if attrib.min_length and attrib.max_length:
    #                             sql_type = f"VARCHAR({attrib.max_length})"
    #                         elif attrib.min_length:
    #                             sql_type = f"TEXT"
    #                         elif attrib.max_length:
    #                             sql_type = f"VARCHAR({attrib.max_length})"
    #
    #             field_def = f"{_safe_name(field_name)} {sql_type}"
    #             if not is_optional:
    #                 field_def += " NOT NULL"
    #             if is_unique:
    #                 field_def += " UNIQUE"
    #             fields.append(field_def)
    #
    #             if attrib:
    #
    #                 safe_field = _safe_name(field_name)
    #
    #                 if sql_type == "TEXT":
    #                     if attrib.min_length and attrib.max_length:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'len_range')} CHECK (length({safe_field}) BETWEEN {attrib.min_length} AND {attrib.max_length})"
    #                         )
    #                     elif attrib.min_length:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'min_length')} CHECK (length({safe_field}) >= {attrib.min_length})"
    #                         )
    #                     elif attrib.max_length:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'max_length')} CHECK (length({safe_field}) <= {attrib.max_length})"
    #                         )
    #
    #                     if attrib.pattern is not None:
    #                         raw_pattern = (
    #                             attrib.pattern.pattern
    #                             if hasattr(attrib.pattern, "pattern")
    #                             else str(attrib.pattern)
    #                         )
    #                         # Convert anchors safely to SQL-standard sequences
    #                         raw_pattern = (
    #                             raw_pattern.replace(r"\A", "^")
    #                             .replace(r"\Z", "$")
    #                             .replace("'", "''")
    #                         )
    #
    #                         # Uses standard SQLite engine REGEXP evaluation operator
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'pattern')} CHECK ({safe_field} REGEXP '{raw_pattern}')"
    #                         )
    #
    #                 elif sql_type in ("INTEGER", "REAL", "NUMERIC"):
    #                     numeric_checks = []
    #                     if attrib.gt is not None:
    #                         numeric_checks.append(f"{safe_field} > {attrib.gt}")
    #                     if attrib.ge is not None:
    #                         numeric_checks.append(f"{safe_field} >= {attrib.ge}")
    #                     if attrib.lt is not None:
    #                         numeric_checks.append(f"{safe_field} < {attrib.lt}")
    #                     if attrib.le is not None:
    #                         numeric_checks.append(f"{safe_field} <= {attrib.le}")
    #
    #                     if numeric_checks:
    #                         chk_constraints.append(
    #                             f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'numeric')} CHECK ({' AND '.join(numeric_checks)})"
    #                         )
    #
    #         fields.extend(chk_constraints)
    #         fields.extend(fk_constraints)
    #         create_sql = f"CREATE TABLE IF NOT EXISTS {_safe_name(schema_name)} ({', '.join(fields)})"
    #         self.execute_query(create_sql, fetch_method=None)
    #
    #         # Auto-provision performance indexes on Foreign Keys to guarantee snappy deletes
    #         for fk_col in fk_columns_to_index:
    #             try:
    #                 idx_name = _generate_constraint_name(
    #                     f"{schema_name}_{fk_col}", "idx", "fk"
    #                 )
    #                 self.execute_query(
    #                     f"CREATE INDEX IF NOT EXISTS {idx_name} ON {_safe_name(schema_name)} ({_safe_name(fk_col)})",
    #                     fetch_method=None,
    #                 )
    #             except Exception:
    #                 pass  # Keep resilient if index generation hits environment variations
    #
    #         self._invalidate_schema_cache(schema_name)
    #
    #     except Exception as e:
    #         logger.error(f"Error creating schema '{schema_name}': {e}")
    #         raise SqlOperationError(f"Error creating schema: {e}") from e

    def drop_schema(self, schema_name: str) -> None:
        """Drop a schema (table) from the database.

        Args:
            schema_name: The name of the schema to drop.

        Raises:
            SqlOperationError: If a schema drop fails.
        """
        self._ensure_connected()

        try:
            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {_safe_name(schema_name)}")
                cursor.close()

            self._invalidate_schema_cache(schema_name)
            logger.info(f"Dropped schema '{schema_name}'")

        except sqlite3.Error as e:
            logger.error(f"Error dropping schema '{schema_name}': {e}")
            raise SqlOperationError(f"Error dropping schema: {e}")

    def list_schemas(self) -> List[str]:
        """List all schemas (tables) in the database.

        Returns:
            List of schema names.
        """
        self._ensure_connected()

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return schemas
        except sqlite3.Error as e:
            logger.error(f"Error listing schemas: {e}")
            raise SqlOperationError(f"Error listing schemas: {e}")

    def supports_foreign_keys(self) -> bool:
        return True

    def has_fk_constraint(self, schema_name: str, column_name: str) -> bool:
        """
        Determine if a column has a foreign key constraint in a given database schema.

        :param schema_name: Name of the schema to search for foreign key constraints.
        :type schema_name: str
        :param column_name: Name of the column to check for foreign key constraints.
        :type column_name: str
        :return: True if the column has a foreign key constraint, False otherwise.
        :rtype: bool
        """
        cursor = self.connector.get_cursor()
        try:
            cursor.execute(f"PRAGMA foreign_key_list({_safe_name(schema_name)})")
            for row in cursor.fetchall():
                # row[3] is the 'from' column name in PRAGMA output
                if row[3] == column_name:
                    return True
            return False
        finally:
            cursor.close()

    def execute_query(
        self, query: str, *args, fetch_method: Optional[str] = "fetchall", **kwargs
    ) -> Any:
        """
        Executes a SQL query using a database connector in a transaction-safe manner.

        :param query: The SQL query to execute.
        :type query: str
        :param args: Positional arguments to pass to the SQL query.
        :type args: tuple
        :param fetch_method: Specifies the fetch method, accepted values are 'fetchall',
            or 'fetchone'. Defaults to 'fetchall'.
        :type fetch_method: str
        :param kwargs: Optional keyword arguments to pass to the SQL execution.
        :type kwargs: dict
        :return: Query result based on the fetch method, or None if no results are found
            or the fetch method does not apply.
        :rtype: Any
        :raises SqlOperationError: If there is an issue executing the SQL query.
        """
        with self.connector.transaction():
            try:
                cursor = self.connector.get_cursor()
                cursor.execute(query, *args, **kwargs)
                if fetch_method == "fetchall":
                    return cursor.fetchall()
                elif fetch_method == "fetchone":
                    return cursor.fetchone()
                return None
            except sqlite3.Error as e:
                logger.error(f"Error executing query: {e}")
                raise SqlOperationError(f"Error executing query: {e}")
            finally:
                if cursor:
                    cursor.close()

    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to check.

        Returns:
            True if the record exists, False otherwise.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            return False

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                f"SELECT 1 FROM {_safe_name(schema_name)} WHERE id = ? LIMIT 1",
                (record_key,),
            )
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except sqlite3.Error as e:
            logger.error(f"Error checking record existence: {e}")
            return False

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """Insert a new record into the store.

        Args:
            schema_name: The schema to insert into.
            record_key: The unique key for the record.
            record: The record object to insert.
            ttl: Time to live in seconds for the record. If None, no TTL is set.

        Raises:
            ObjectExistError: If a record with the same key already exists.
            SerializationError: If serialization fails.
            SqlOperationError: If insertion fails.
        """
        self._ensure_connected()

        try:
            self.ensure_schema(schema_name, record.__class__)

            if self.exists(schema_name, record_key):
                raise ObjectExistError(
                    f"Record '{record_key}' already exists in schema '{schema_name}'"
                )

            record_data = self._prepare_record_data(record, record_key)

            fields = list(record_data.keys())
            placeholders = ["?" for _ in fields]
            values = [record_data[field] for field in fields]

            # Quote identifiers: fields like "order" are SQL keywords
            insert_sql = f"""
                INSERT INTO {_safe_name(schema_name)} ({', '.join(map(_safe_name, fields))})
                VALUES ({', '.join(placeholders)})
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(insert_sql, values)
                cursor.close()

            logger.debug(f"Inserted record '{record_key}' into schema '{schema_name}'")

        except ObjectExistError:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error inserting record: {e}")
            raise SqlOperationError(f"Error inserting record: {e}")

    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> Optional[int]:
        """Update an existing record in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to update.
            record: The updated record object.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If serialization fails.
            SqlOperationError: If the update fails.
        """
        self._ensure_connected()

        cursor = None

        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            record_data = self._prepare_record_data(record, record_key)

            set_fields = [
                f"{_safe_name(field)} = ?"
                for field in record_data.keys()
                if field != "id"
            ]
            values = [
                record_data[field] for field in record_data.keys() if field != "id"
            ]
            values.append(record_key)

            update_sql = f"""
                UPDATE {_safe_name(schema_name)}
                SET {', '.join(set_fields)}
                WHERE id = ?
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(update_sql, values)
                return cursor.rowcount

        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error updating record: {e}")
            raise SqlOperationError(f"Error updating record: {e}")
        finally:
            if cursor:
                cursor.close()

    def upsert(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> Optional[int]:
        """Insert or update a record (upsert operation).

        Args:
            schema_name: The schema for the operation.
            record_key: The key of the record.
            record: The record object to upsert.

        Raises:
            SerializationError: If serialization fails.
            SqlOperationError: If upsert fails.
        """
        self._ensure_connected()

        cursor = None

        try:
            self.ensure_schema(schema_name, record.__class__)

            record_data = self._prepare_record_data(record, record_key)

            fields = list(record_data.keys())
            placeholders = ["?" for _ in fields]
            update_fields = [
                f"{_safe_name(field)} = excluded.{_safe_name(field)}"
                for field in fields
                if field != "id"
            ]
            values = [record_data[field] for field in fields]

            upsert_sql = f"""
                INSERT INTO {_safe_name(schema_name)} ({', '.join(map(_safe_name, fields))})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT(id) DO UPDATE SET
                {', '.join(update_fields)}
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(upsert_sql, values)
                return cursor.rowcount

        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error upserting record: {e}")
            raise SqlOperationError(f"Error upserting record: {e}")
        finally:
            if cursor:
                cursor.close()

    def delete(self, schema_name: str, record_key: str) -> Optional[int]:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SqlOperationError: If deletion fails.
        """
        self._ensure_connected()

        cursor = None

        try:
            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(
                    f"DELETE FROM {_safe_name(schema_name)} WHERE id = ?", (record_key,)
                )

                return cursor.rowcount
        except ObjectDoesNotExist:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error deleting record: {e}")
            raise SqlOperationError(f"Error deleting record: {e}")
        finally:
            if cursor:
                cursor.close()

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

        cursor = None

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                f"SELECT * FROM {_safe_name(schema_name)} WHERE id = ?", (record_key,)
            )
            row = cursor.fetchone()

            if row is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            record = self._deserialize_record(
                self._sqlite_row_and_tuple_to_dict(row, cursor), record_klass
            )
            logger.debug(f"Retrieved record '{record_key}' from schema '{schema_name}'")
            return record

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error getting record: {e}")
            raise SqlOperationError(f"Error getting record: {e}")
        finally:
            if cursor:
                cursor.close()

    @staticmethod
    def _escape_like(value: str) -> str:
        """Escape SQL LIKE metacharacters to prevent wildcard injection."""
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _build_sql_filter(
        self, filter_kwargs: Dict[str, Any], table: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """Build a WHERE clause from Django-style lookups.

        Column names are quoted (and qualified with ``table`` when given, to
        stay unambiguous in JOINs), so keyword columns work and filter keys
        cannot inject SQL.
        """
        if not filter_kwargs:
            return "1", []

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
                    if not value:
                        conditions.append("0")  # Empty IN = no matches
                        continue
                    placeholders = ",".join(["?" for _ in value])
                    conditions.append(f"{field} IN ({placeholders})")
                    parameters.extend(value)

                elif operator in (
                    "contains",
                    "icontains",
                    "startswith",
                    "istartswith",
                    "endswith",
                    "iendswith",
                ):
                    escaped = self._escape_like(str(value))
                    nocase = " COLLATE NOCASE" if operator.startswith("i") else ""

                    if "startswith" in operator:
                        pattern = f"{escaped}%"
                    elif "endswith" in operator:
                        pattern = f"%{escaped}"
                    else:
                        pattern = f"%{escaped}%"

                    conditions.append(f"{field} LIKE ?{nocase} ESCAPE '\\'")
                    parameters.append(pattern)

                elif operator == "exact":
                    conditions.append(f"{field} = ?")
                    parameters.append(value)
                elif operator == "gt":
                    conditions.append(f"{field} > ?")
                    parameters.append(value)
                elif operator == "gte":
                    conditions.append(f"{field} >= ?")
                    parameters.append(value)
                elif operator == "lt":
                    conditions.append(f"{field} < ?")
                    parameters.append(value)
                elif operator == "lte":
                    conditions.append(f"{field} <= ?")
                    parameters.append(value)
                elif operator == "ne":
                    conditions.append(f"{field} != ?")
                    parameters.append(value)
                elif operator == "isnull":
                    conditions.append(f"{field} IS {'NULL' if value else 'NOT NULL'}")
                else:
                    logger.warning("Unknown filter operator: %s", operator)
                    conditions.append(f"{field} = ?")
                    parameters.append(value)
            else:
                conditions.append(f"{column(key)} = ?")
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

        Args:
            schema_name: The schema to filter within.
            record_klass: The class to instantiate records with.
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            order_by: Field to order by (prefix with '-' for descending).
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            List of matching record instances.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If a query fails.
        """
        self._ensure_connected()

        cursor = None

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:

            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = f"SELECT id FROM {_safe_name(schema_name)}"

            if where_clause != "1":
                query += f" WHERE {where_clause}"

            query += self._order_by_clause(order_by)

            # Bound rather than interpolated; SQLite requires LIMIT before
            # OFFSET, and LIMIT -1 means "no limit".
            if limit is not None or offset is not None:
                query += " LIMIT ?"
                parameters.append(int(limit) if limit is not None else -1)
            if offset is not None:
                query += " OFFSET ?"
                parameters.append(int(offset))

            cursor = self.connector.get_cursor()
            cursor.execute(query, parameters)
            rows = cursor.fetchall()

            return self._create_result_stream(
                record_keys=[row[0] for row in rows], record_klass=record_klass
            )

        except ObjectDoesNotExist:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error filtering records: {e}")
            raise SqlOperationError(f"Error filtering records: {e}")
        finally:
            if cursor is not None:
                cursor.close()

    def _ensure_foreign_keys_enabled(self) -> None:
        """Enable FK enforcement on the current connection.

        SQLite disables FK enforcement by default. This must be called
        on every new connection, not just once at startup, because PRAGMA
        settings are per-connection, not per-database.
        """
        cursor = self.connector.get_cursor()
        try:
            cursor.execute("PRAGMA foreign_keys")
            row = cursor.fetchone()
            if not row or row[0] != 1:
                cursor.execute("PRAGMA foreign_keys = ON")
                logger.debug("Enabled foreign key enforcement for SQLite connection")
        finally:
            cursor.close()

    def _get_fk_target_schema_sqlite(
        self, schema_name: str, fk_column: str
    ) -> Optional[str]:
        """Get FK target table from PRAGMA foreign_key_list().

        SQLite has no information_schema. PRAGMA foreign_key_list returns:
            (id, seq, table, from, to, on_update, on_delete, match)
        Column index 3 is 'from' (source column), index 2 is 'table' (target).
        """
        cursor = self.connector.get_cursor()
        try:
            cursor.execute(f"PRAGMA foreign_key_list({_safe_name(schema_name)})")
            for row in cursor.fetchall():
                if row[3] == fk_column:  # row[3] = 'from' column name
                    return row[2]  # row[2] = target table name
            return None
        finally:
            cursor.close()

    def filter_with_joins(
        self,
        schema_name: str,
        model_class: Type["KeyValueStoreIntegrationMixin"],
        join_fields: List[str],
        **filter_kwargs: Any,
    ) -> List[Dict[str, Any]]:
        # TODO: connect the implementation of join push down to result stream
        """Resolve FK fields via SQL JOINs (native) or batch fetch (software).

        Ensures FK enforcement is enabled before executing JOINs.
        Uses PRAGMA foreign_key_list() for target schema resolution
        since SQLite lacks information_schema.
        """
        self._ensure_connected()
        self._ensure_foreign_keys_enabled()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        # Classify each join field
        native_joins: List[Tuple[str, str, str]] = (
            []
        )  # (field_name, fk_column, target_schema)
        software_fk_columns: List[Tuple[str, str]] = []  # (field_name, fk_column)

        for field_name in join_fields:
            fk_col = (
                f"{field_name}_object_id"
                if not field_name.endswith("_object_id")
                else field_name
            )
            has_native = self.has_fk_constraint(schema_name, fk_col)

            if has_native:
                target_schema = self._get_fk_target_schema_sqlite(schema_name, fk_col)
                if target_schema:
                    native_joins.append((field_name, fk_col, target_schema))
                else:
                    # Constraint exists but target unresolvable → treat as software
                    software_fk_columns.append((field_name, fk_col))
            else:
                software_fk_columns.append((field_name, fk_col))

        # Build query with native LEFT JOINs. ``SELECT alias.*`` can't be
        # prefixed, so each target column is selected explicitly under a
        # ``<field>__resolved_<col>`` alias (id included).
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
        if where_clause != "1":
            query += f" WHERE {where_clause}"

        cursor = self.connector.get_cursor()
        try:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()

        # Assemble results with nested resolved dicts
        results: List[Dict[str, Any]] = []
        for row in rows:
            record_dict = dict(zip(col_names, row))

            # Extract joined columns into nested resolved dicts (iterate over a
            # snapshot of the keys: the dict is modified inside the loop)
            for field_name in alias_map:
                prefix = f"{field_name}__resolved_"
                resolved = {
                    k[len(prefix) :]: record_dict.pop(k)
                    for k in list(record_dict)
                    if k.startswith(prefix)
                }

                record_dict[f"{field_name}__resolved"] = (
                    resolved if resolved.get("id") is not None else None
                )

            results.append(record_dict)

        # Batch-resolve software FKs (single additional query per field)
        for field_name, fk_col in software_fk_columns:
            target_ids: List[str] = []
            for r in results:
                fk_value = r.get(fk_col)
                if isinstance(fk_value, dict):
                    obj_id = fk_value.get("data")
                elif isinstance(fk_value, str):
                    obj_id = fk_value
                else:
                    obj_id = None
                if obj_id is not None:
                    target_ids.append(obj_id)

            if not target_ids:
                continue

            target_model_class = self.resolve_target_model_for_field(
                model_class, field_name
            )
            if target_model_class is None:
                continue

            # get_schema_name() is async; resolve the route synchronously here
            target_schema = self.resolve_physical_target(
                target_model_class.get_storage_route()
            )
            placeholders = ", ".join(["?"] * len(target_ids))

            cursor = self.connector.get_cursor()
            try:
                cursor.execute(
                    f"SELECT * FROM {_safe_name(target_schema)} WHERE id IN ({placeholders})",
                    target_ids,
                )
                target_rows = cursor.fetchall()
                target_col_names = [desc[0] for desc in cursor.description]
            finally:
                cursor.close()

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

    def count(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            record_klass: The record class to count.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SqlOperationError: If the count fails.
        """
        self._ensure_connected()

        cursor = None

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:
            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = f"SELECT COUNT(*) FROM {_safe_name(schema_name)}"

            if where_clause != "1":
                query += f" WHERE {where_clause}"

            cursor = self.connector.get_cursor()
            cursor.execute(query, parameters)
            result = cursor.fetchone()

            return int(result[0]) if result else 0

        except sqlite3.Error as e:
            logger.error(f"Error counting records: {e}")
            raise SqlOperationError(f"Error counting records: {e}")
        finally:
            if cursor:
                cursor.close()

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
        if not hasattr(record, "id"):
            raise ValueError("Record must have an 'id' attribute for reload")

        record_key = record.id
        self._ensure_connected()

        cursor = None

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                f"SELECT * FROM {_safe_name(schema_name)} WHERE id = ?", (record_key,)
            )
            row = cursor.fetchone()

            if row is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' no longer exists in schema '{schema_name}'"
                )

            return self._deserialize_record(
                self._sqlite_row_and_tuple_to_dict(row, cursor), record.__class__
            )

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error reloading record: {e}")
            raise SqlOperationError(f"Error reloading record: {e}")
        finally:
            if cursor:
                cursor.close()
