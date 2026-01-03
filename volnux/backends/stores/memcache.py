import logging
import json
from typing import Any, Dict, List, Optional, Type, Union

from pydantic_mini import BaseModel
from pymemcache.exceptions import MemcacheError

from volnux.backends.connectors.memcache import MemcacheConnector
from volnux.backends.store import KeyValueStoreBackendBase
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError, SerializationError

logger = logging.getLogger(__name__)


class MemcacheStoreBackend(KeyValueStoreBackendBase):
    """Memcache-backed key-value store implementation.

    This backend uses Memcache for high-performance caching and temporary
    storage. Each schema is represented as a key namespace, and records
    are serialized using pickle for efficient storage.

    Example:
        >>> backend = MemcacheStoreBackend(
        ...     host="localhost",
        ...     port=11211,
        ...     default_ttl=3600
        ... )
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", UserModel, "user_1")
        >>> users = backend.filter("users", UserModel, status="active")
    """

    connector_klass = MemcacheConnector

    DEFAULT_TTL = 0  # 0 means no expiration
    NAMESPACE_SEPARATOR = ":"
    INDEX_SUFFIX = "_index"

    def __init__(
        self,
        default_ttl: int = DEFAULT_TTL,
        namespace_prefix: str = "",
        enable_indexing: bool = True,
        **connector_config: Any,
    ):
        """Initialize the Memcache store backend.

        Args:
            default_ttl: Default time-to-live in seconds (0 for no expiration).
            namespace_prefix: Global prefix for all keys (useful for multi-tenant).
            enable_indexing: Enable schema-level indexing for filtering.
            **connector_config: Configuration passed to MemcacheConnector.
        """
        super().__init__(**connector_config)

        self.default_ttl = default_ttl
        self.namespace_prefix = namespace_prefix
        self.enable_indexing = enable_indexing

        # Ensure connection on initialization
        if not self.connector.is_connected():
            self.connector.connect()

    def _ensure_connected(self) -> None:
        """Ensure the Memcache connection is active.

        Raises:
            ConnectionError: If connection cannot be established.
        """
        if not self.connector.is_connected():
            logger.warning("Memcache connection lost, attempting to reconnect...")
            self.connector.connect()

    def _build_index_key(self, schema_name: str) -> str:
        """Build the key for a schema's index.

        Args:
            schema_name: The schema name.

        Returns:
            Index key string.
        """
        parts = []
        if self.namespace_prefix:
            parts.append(self.namespace_prefix)
        parts.append(schema_name + self.INDEX_SUFFIX)
        return self.NAMESPACE_SEPARATOR.join(parts)

    # def _serialize_record(self, record: BaseModel) -> bytes:
    #     """Serialize a record to bytes.
    #
    #     Args:
    #         record: The record to serialize.
    #
    #     Returns:
    #         Serialized record as bytes.
    #
    #     Raises:
    #         SerializationError: If serialization fails.
    #     """
    #     try:
    #         state = record.__getstate__()
    #         return pickle.dumps(state, protocol=self.PICKLE_PROTOCOL)
    #     except Exception as e:
    #         logger.error(f"Failed to serialize record: {e}")
    #         raise SerializationError(f"Serialization failed: {e}")
    #
    # def _deserialize_record(
    #     self, data: bytes, record_klass: Type[BaseModel]
    # ) -> BaseModel:
    #     """Deserialize bytes to a record object.
    #
    #     Args:
    #         data: Serialized record data.
    #         record_klass: The class to instantiate.
    #
    #     Returns:
    #         Deserialized record instance.
    #
    #     Raises:
    #         SerializationError: If deserialization fails.
    #     """
    #     try:
    #         state = pickle.loads(data)
    #         record = record_klass.__new__(record_klass)
    #         record.__setstate__(state)
    #         return record
    #     except Exception as e:
    #         logger.error(f"Failed to deserialize record: {e}")
    #         raise SerializationError(f"Deserialization failed: {e}")

    def _get_schema_index(self, schema_name: str) -> set:
        """Get the set of record keys in a schema.

        Args:
            schema_name: The schema name.

        Returns:
            Set of record keys.
        """
        if not self.enable_indexing:
            return set()

        try:
            self._ensure_connected()
            index_key = self._build_index_key(schema_name)
            index_data = self.connector.cursor.get(index_key)

            if index_data is None:
                return set()

            return json.loads(index_data)
        except Exception as e:
            logger.warning(f"Failed to get schema index: {e}")
            return set()

    def _update_schema_index(
        self, schema_name: str, record_key: str, operation: str = "add"
    ) -> None:
        """Update the schema index.

        Args:
            schema_name: The schema name.
            record_key: The record key to add or remove.
            operation: Either "add" or "remove".
        """
        if not self.enable_indexing:
            return

        try:
            self._ensure_connected()
            index_key = self._build_index_key(schema_name)

            # Get current index
            index = self._get_schema_index(schema_name)

            # Update index
            if operation == "add":
                index.add(record_key)
            elif operation == "remove":
                index.discard(record_key)

            # Save updated index (with no expiration)
            serialized_index = json.dumps(index)
            self.connector.cursor.set(index_key, serialized_index, expire=0)

        except Exception as e:
            logger.warning(f"Failed to update schema index: {e}")

    # Core CRUD Operations

    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to check.

        Returns:
            True if the record exists, False otherwise.
        """
        try:
            self._ensure_connected()
            full_key = self._build_key(schema_name, record_key)
            result = self.connector.cursor.get(full_key)
            return result is not None
        except MemcacheError as e:
            logger.error(f"Memcache error checking existence: {e}")
            return False

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: BaseModel,
        ttl: Optional[int] = None,
    ) -> None:
        """Insert a new record into the store.

        Args:
            schema_name: The schema to insert into.
            record_key: The unique key for the record.
            record: The record object to insert.
            ttl: Time-to-live in seconds (None uses default_ttl).

        Raises:
            ObjectExistError: If a record with the same key already exists.
            SerializationError: If serialization fails.
            ConnectionError: If Memcache operation fails.
        """
        if self.exists(schema_name, record_key):
            raise ObjectExistError(
                f"Record '{record_key}' already exists in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()
            full_key = self._build_key(schema_name, record_key)
            serialized = self._serialize_record(record)
            expire = ttl if ttl is not None else self.default_ttl

            # Use 'add' to ensure atomicity (fails if key exists)
            success = self.connector.cursor.add(full_key, serialized, expire=expire)

            if not success:
                raise ObjectExistError(
                    f"Record '{record_key}' already exists in schema '{schema_name}'"
                )

            # Update index
            self._update_schema_index(schema_name, record_key, operation="add")

            logger.debug(
                f"Inserted record '{record_key}' into schema '{schema_name}' (ttl={expire}s)"
            )

        except ObjectExistError:
            raise
        except SerializationError:
            raise
        except MemcacheError as e:
            logger.error(f"Memcache error during insert: {e}")
            raise ConnectionError(f"Failed to insert record: {e}")

    def update(
        self,
        schema_name: str,
        record_key: str,
        record: BaseModel,
        ttl: Optional[int] = None,
    ) -> None:
        """Update an existing record in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to update.
            record: The updated record object.
            ttl: Time-to-live in seconds (None uses default_ttl).

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If serialization fails.
            ConnectionError: If Memcache operation fails.
        """
        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()
            full_key = self._build_key(schema_name, record_key)
            serialized = self._serialize_record(record)
            expire = ttl if ttl is not None else self.default_ttl

            # Use 'replace' to ensure key exists
            success = self.connector.cursor.replace(full_key, serialized, expire=expire)

            if not success:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            logger.debug(
                f"Updated record '{record_key}' in schema '{schema_name}' (ttl={expire}s)"
            )

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except MemcacheError as e:
            logger.error(f"Memcache error during update: {e}")
            raise ConnectionError(f"Failed to update record: {e}")

    def upsert(
        self,
        schema_name: str,
        record_key: str,
        record: BaseModel,
        ttl: Optional[int] = None,
    ) -> None:
        """Insert or update a record (upsert operation).

        Args:
            schema_name: The schema for the operation.
            record_key: The key of the record.
            record: The record object to upsert.
            ttl: Time-to-live in seconds (None uses default_ttl).

        Raises:
            SerializationError: If serialization fails.
            ConnectionError: If Memcache operation fails.
        """
        try:
            self._ensure_connected()
            full_key = self._build_key(schema_name, record_key)
            serialized = self._serialize_record(record)
            expire = ttl if ttl is not None else self.default_ttl

            # Use 'set' for upsert behavior
            self.connector.cursor.set(full_key, serialized, expire=expire)

            # Update index
            self._update_schema_index(schema_name, record_key, operation="add")

            logger.debug(
                f"Upserted record '{record_key}' in schema '{schema_name}' (ttl={expire}s)"
            )

        except SerializationError:
            raise
        except MemcacheError as e:
            logger.error(f"Memcache error during upsert: {e}")
            raise ConnectionError(f"Failed to upsert record: {e}")

    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            ConnectionError: If Memcache operation fails.
        """
        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()
            full_key = self._build_key(schema_name, record_key)

            success = self.connector.cursor.delete(full_key)

            if not success:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            # Update index
            self._update_schema_index(schema_name, record_key, operation="remove")

            logger.debug(f"Deleted record '{record_key}' from schema '{schema_name}'")

        except ObjectDoesNotExist:
            raise
        except MemcacheError as e:
            logger.error(f"Memcache error during delete: {e}")
            raise ConnectionError(f"Failed to delete record: {e}")

    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type[BaseModel],
    ) -> Optional[BaseModel]:
        """Retrieve a single record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to retrieve.
            record_klass: The class to instantiate the record with.

        Returns:
            The record instance if found.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If deserialization fails.
            ConnectionError: If Memcache operation fails.
        """
        record_key_str = str(record_key)

        try:
            self._ensure_connected()
            full_key = self._build_key(schema_name, record_key_str)
            serialized = self.connector.cursor.get(full_key)

            if serialized is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key_str}' does not exist in schema '{schema_name}'"
                )

            record = self._deserialize_record(serialized, record_klass)
            logger.debug(
                f"Retrieved record '{record_key_str}' from schema '{schema_name}'"
            )
            return record

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except MemcacheError as e:
            logger.error(f"Memcache error during get: {e}")
            raise ConnectionError(f"Failed to get record: {e}")

    # Query Operations

    def filter(
        self, schema_name: str, record_klass: Type[BaseModel], **filter_kwargs: Any
    ) -> List[BaseModel]:
        """Filter records matching the specified criteria.

        Note: Filtering in Memcache requires fetching all records in the schema
        and filtering in memory. For large datasets, this can be slow.

        Args:
            schema_name: The schema to filter within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            List of matching record instances.

        Raises:
            ConnectionError: If Memcache operation fails.
        """
        if not self.enable_indexing:
            logger.warning(
                "Indexing is disabled; filtering may be incomplete. "
                "Enable indexing for accurate results."
            )
            return []

        try:
            self._ensure_connected()

            # Get all record keys from index
            index = self._get_schema_index(schema_name)

            if not index:
                return []

            # Build full keys
            full_keys = [self._build_key(schema_name, key) for key in index]

            # Fetch all records
            records_data = self.connector.cursor.get_many(full_keys)

            # Create filter predicate
            predicate = self._create_filter_predicate(**filter_kwargs)

            # Deserialize and filter records
            matching_records = []
            for full_key, serialized in records_data.items():
                if serialized is None:
                    continue

                try:
                    record = self._deserialize_record(serialized, record_klass)
                    if predicate(record):
                        matching_records.append(record)
                except SerializationError as e:
                    # Extract original key from full key
                    original_key = full_key.decode().split(self.NAMESPACE_SEPARATOR)[-1]
                    logger.warning(
                        f"Skipping corrupted record '{original_key}' in schema '{schema_name}': {e}"
                    )
                    continue

            logger.debug(
                f"Filtered {len(matching_records)} records from schema '{schema_name}'"
            )
            return matching_records

        except MemcacheError as e:
            logger.error(f"Memcache error during filter: {e}")
            raise ConnectionError(f"Failed to filter records: {e}")

    def count(self, schema_name: str, **filter_kwargs: Any) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.

        Raises:
            ConnectionError: If Memcache operation fails.
        """
        if not filter_kwargs:
            # Fast path: just count index entries
            if self.enable_indexing:
                index = self._get_schema_index(schema_name)
                return len(index)
            else:
                logger.warning("Indexing disabled; cannot count without filtering")
                return 0

        # Slow path: filter and count
        matching_records = self.filter(schema_name, BaseModel, **filter_kwargs)
        return len(matching_records)

    # Record Lifecycle Operations

    @staticmethod
    def load_record(record_state: bytes, record_klass: Type[BaseModel]) -> BaseModel:
        """Load a record from its serialized state.

        Args:
            record_state: The serialized record data.
            record_klass: The class to instantiate the record with.

        Returns:
            The instantiated record object.

        Raises:
            SerializationError: If deserialization fails.
        """
        try:
            state = pickle.loads(record_state)
            record = record_klass.__new__(record_klass)
            record.__setstate__(state)
            return record
        except Exception as e:
            raise SerializationError(f"Failed to load record: {e}")

    def reload(self, schema_name: str, record: BaseModel) -> BaseModel:
        """Reload a record's data from the backend.

        Args:
            schema_name: The schema containing the record.
            record: The record to reload.

        Returns:
            The reloaded record instance.

        Raises:
            ObjectDoesNotExist: If the record no longer exists.
            SerializationError: If deserialization fails.
            ConnectionError: If Memcache operation fails.
        """
        if not hasattr(record, "id"):
            raise ValueError("Record must have an 'id' attribute for reload")

        record_key = str(record.id)

        try:
            self._ensure_connected()
            full_key = self._build_key(schema_name, record_key)
            serialized = self.connector.cursor.get(full_key)

            if serialized is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' no longer exists in schema '{schema_name}'"
                )

            state = pickle.loads(serialized)
            record.__setstate__(state)

            logger.debug(f"Reloaded record '{record_key}' from schema '{schema_name}'")
            return record

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except MemcacheError as e:
            logger.error(f"Memcache error during reload: {e}")
            raise ConnectionError(f"Failed to reload record: {e}")

    # Batch Operations

    def bulk_insert(
        self, schema_name: str, records: Dict[str, BaseModel], ttl: Optional[int] = None
    ) -> None:
        """Insert multiple records in a single operation.

        Args:
            schema_name: The schema to insert into.
            records: Dictionary mapping record keys to record objects.
            ttl: Time-to-live in seconds (None uses default_ttl).

        Raises:
            SerializationError: If serialization fails.
            ConnectionError: If Memcache operation fails.
        """
        if not records:
            return

        try:
            self._ensure_connected()
            expire = ttl if ttl is not None else self.default_ttl

            # Serialize all records
            serialized_data = {}
            for key, record in records.items():
                full_key = self._build_key(schema_name, key)
                serialized_data[full_key] = self._serialize_record(record)

            # Bulk set
            failed_keys = self.connector.cursor.set_many(
                serialized_data, expire=expire, noreply=False
            )

            # Update index for successful inserts
            if self.enable_indexing:
                for key in records.keys():
                    if self._build_key(schema_name, key) not in failed_keys:
                        self._update_schema_index(schema_name, key, operation="add")

            if failed_keys:
                logger.warning(f"Failed to insert {len(failed_keys)} records")

            logger.info(
                f"Bulk inserted {len(records) - len(failed_keys)} records into schema '{schema_name}'"
            )

        except SerializationError:
            raise
        except MemcacheError as e:
            logger.error(f"Memcache error during bulk insert: {e}")
            raise ConnectionError(f"Failed to bulk insert records: {e}")

    def bulk_delete(self, schema_name: str, record_keys: List[str]) -> None:
        """Delete multiple records in a single operation.

        Args:
            schema_name: The schema containing the records.
            record_keys: List of record keys to delete.

        Raises:
            ConnectionError: If Memcache operation fails.
        """
        if not record_keys:
            return

        try:
            self._ensure_connected()

            # Build full keys
            full_keys = [self._build_key(schema_name, key) for key in record_keys]

            # Bulk delete
            self.connector.cursor.delete_many(full_keys, noreply=False)

            # Update index
            if self.enable_indexing:
                for key in record_keys:
                    self._update_schema_index(schema_name, key, operation="remove")

            logger.info(
                f"Bulk deleted {len(record_keys)} records from schema '{schema_name}'"
            )

        except MemcacheError as e:
            logger.error(f"Memcache error during bulk delete: {e}")
            raise ConnectionError(f"Failed to bulk delete records: {e}")

    # Schema Management

    def clear_schema(self, schema_name: str) -> None:
        """Delete all records in a schema.

        Args:
            schema_name: The schema to clear.

        Raises:
            ConnectionError: If Memcache operation fails.
        """
        if not self.enable_indexing:
            logger.warning("Indexing disabled; cannot clear schema efficiently")
            return

        try:
            # Get all keys
            index = self._get_schema_index(schema_name)

            if not index:
                logger.debug(f"Schema '{schema_name}' is already empty")
                return

            # Delete all records
            self.bulk_delete(schema_name, list(index))

            # Clear the index
            index_key = self._build_index_key(schema_name)
            self.connector.cursor.delete(index_key)

            logger.info(f"Cleared all records from schema '{schema_name}'")

        except MemcacheError as e:
            logger.error(f"Memcache error during clear schema: {e}")
            raise ConnectionError(f"Failed to clear schema: {e}")

    def list_all(
        self, schema_name: str, record_klass: Type[BaseModel]
    ) -> List[BaseModel]:
        """List all records in a schema.

        Args:
            schema_name: The schema to list from.
            record_klass: The class to instantiate records with.

        Returns:
            List of all records in the schema.
        """
        return self.filter(schema_name, record_klass)
