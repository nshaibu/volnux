import logging
from typing import Any, Dict, List, Optional, Type, Union

from pydantic_mini import BaseModel
from redis.exceptions import RedisError

from volnux.backends.connectors.redis import RedisConnector
from volnux.backends.store import KeyValueStoreBackendBase
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError, SerializationError

logger = logging.getLogger(__name__)


class RedisStoreBackend(KeyValueStoreBackendBase):
    """Redis-backed key-value store implementation.

    Example:
        >>> backend = RedisStoreBackend(host="localhost", port=6379, database=0)
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", UserModel, "user_1")
        >>> users = backend.filter("users", UserModel, status="active")
    """

    connector_klass = RedisConnector

    #  Number of items to fetch per HSCAN iteration
    DEFAULT_SCAN_COUNT = 100

    def __init__(
        self,
        scan_count: int = DEFAULT_SCAN_COUNT,
        **connector_config: Any,
    ):
        """Initialize the Redis store backend.

        Args:
            scan_count: Number of items to fetch per HSCAN iteration.
            **connector_config: Configuration passed to RedisConnector.
        """
        super().__init__(**connector_config)
        self.scan_count = scan_count

        # Ensure connection on initialization
        if not self.connector.is_connected():
            self.connector.connect()

    def _ensure_connected(self) -> None:
        """Ensure the Redis connection is active.

        Raises:
            ConnectionError: If connection cannot be established.
        """
        if not self.connector.is_connected():
            logger.warning("Redis connection lost, attempting to reconnect...")
            self.connector.connect()

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
            return bool(self.connector.cursor.hexists(schema_name, record_key))
        except RedisError as e:
            logger.error(f"Redis error checking existence: {e}")
            raise ConnectionError(f"Failed to check record existence: {e}")

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
            ttl: Optional TTL for the new record.

        Raises:
            ObjectExistError: If a record with the same key already exists.
            SerializationError: If serialization fails.
            ConnectionError: If Redis operation fails.
        """
        if self.exists(schema_name, record_key):
            raise ObjectExistError(
                f"Record '{record_key}' already exists in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()
            serialized = self._serialize_record(record)

            with self.connector.get_pipeline(transaction=True) as pipe:
                pipe.hset(schema_name, record_key, serialized)
                if ttl is not None:
                    pipe.expire(schema_name, ttl)

                pipe.execute()

            logger.debug(f"Inserted record '{record_key}' into schema '{schema_name}'")
        except SerializationError:
            raise
        except RedisError as e:
            logger.error(f"Redis error during insert: {e}")
            raise ConnectionError(f"Failed to insert record: {e}")

    def update(self, schema_name: str, record_key: str, record: BaseModel) -> None:
        """Update an existing record in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to update.
            record: The updated record object.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If serialization fails.
            ConnectionError: If Redis operation fails.
        """
        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()
            serialized = self._serialize_record(record)

            # Use pipeline for atomic operation
            with self.connector.get_pipeline(transaction=True) as pipe:
                pipe.hset(schema_name, record_key, serialized)
                pipe.execute()

            logger.debug(f"Updated record '{record_key}' in schema '{schema_name}'")
        except SerializationError:
            raise
        except RedisError as e:
            logger.error(f"Redis error during update: {e}")
            raise ConnectionError(f"Failed to update record: {e}")

    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            ConnectionError: If Redis operation fails.
        """
        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()

            with self.connector.get_pipeline(transaction=True) as pipe:
                pipe.hdel(schema_name, record_key)
                pipe.execute()

            logger.debug(f"Deleted record '{record_key}' from schema '{schema_name}'")
        except RedisError as e:
            logger.error(f"Redis error during delete: {e}")
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
            The record instance if found, None otherwise.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If deserialization fails.
            ConnectionError: If Redis operation fails.
        """
        record_key_str = str(record_key)

        if not self.exists(schema_name, record_key_str):
            raise ObjectDoesNotExist(
                f"Record '{record_key_str}' does not exist in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()
            serialized = self.connector.cursor.hget(schema_name, record_key_str)

            if serialized is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key_str}' not found in schema '{schema_name}'"
                )

            record = self._deserialize_record(serialized, record_klass)
            logger.debug(
                f"Retrieved record '{record_key_str}' from schema '{schema_name}'"
            )
            return record
        except SerializationError:
            raise
        except RedisError as e:
            logger.error(f"Redis error during get: {e}")
            raise ConnectionError(f"Failed to get record: {e}")

    def filter(
        self, schema_name: str, record_klass: Type[BaseModel], **filter_kwargs: Any
    ) -> List[BaseModel]:
        """Filter records matching the specified criteria.

        Args:
            schema_name: The schema to filter within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            List of matching record instances.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()

            predicate = self._create_filter_predicate(**filter_kwargs)
            matching_records: List[BaseModel] = []

            # Use HSCAN for efficient iteration over large datasets
            cursor = 0
            while True:
                cursor, data = self.connector.cursor.hscan(
                    schema_name, cursor=cursor, count=self.scan_count
                )

                for key, value in data.items():
                    try:
                        record = self._deserialize_record(value, record_klass)
                        if predicate(record):
                            matching_records.append(record)
                    except SerializationError as e:
                        logger.warning(
                            f"Skipping corrupted record '{key}' in schema '{schema_name}': {e}"
                        )
                        continue

                if cursor == 0:
                    break

            logger.debug(
                f"Filtered {len(matching_records)} records from schema '{schema_name}'"
            )
            return matching_records
        except RedisError as e:
            logger.error(f"Redis error during filter: {e}")
            raise ConnectionError(f"Failed to filter records: {e}")

    def count(self, schema_name: str, **filter_kwargs: Any) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()

            # If no filters, use efficient HLEN
            if not filter_kwargs:
                return self.connector.cursor.hlen(schema_name)

            # Otherwise, filter and count
            matching_records = self.filter(schema_name, BaseModel, **filter_kwargs)
            return len(matching_records)
        except RedisError as e:
            logger.error(f"Redis error during count: {e}")
            raise ConnectionError(f"Failed to count records: {e}")

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
            ConnectionError: If Redis operation fails.
        """
        if not hasattr(record, "id"):
            raise ValueError("Record must have an 'id' attribute for reload")

        record_key = str(record.id)

        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' no longer exists in schema '{schema_name}'"
            )

        try:
            self._ensure_connected()
            serialized = self.connector.cursor.hget(schema_name, record_key)

            if serialized is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' not found in schema '{schema_name}'"
                )

            state = pickle.loads(serialized)
            record.__setstate__(state)

            logger.debug(f"Reloaded record '{record_key}' from schema '{schema_name}'")
            return record
        except SerializationError:
            raise
        except RedisError as e:
            logger.error(f"Redis error during reload: {e}")
            raise ConnectionError(f"Failed to reload record: {e}")

    # Batch Operations

    def bulk_insert(self, schema_name: str, records: Dict[str, BaseModel]) -> None:
        """Insert multiple records in a single operation.

        Args:
            schema_name: The schema to insert into.
            records: Dictionary mapping record keys to record objects.

        Raises:
            SerializationError: If serialization fails.
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()

            # Serialize all records first
            serialized_data = {}
            for key, record in records.items():
                serialized_data[key] = self._serialize_record(record)

            # Bulk insert using HMSET via pipeline
            with self.connector.get_pipeline(transaction=True) as pipe:
                for key, data in serialized_data.items():
                    pipe.hset(schema_name, key, data)
                pipe.execute()

            logger.info(
                f"Bulk inserted {len(records)} records into schema '{schema_name}'"
            )
        except SerializationError:
            raise
        except RedisError as e:
            logger.error(f"Redis error during bulk insert: {e}")
            raise ConnectionError(f"Failed to bulk insert records: {e}")

    def bulk_delete(self, schema_name: str, record_keys: List[str]) -> None:
        """Delete multiple records in a single operation.

        Args:
            schema_name: The schema containing the records.
            record_keys: List of record keys to delete.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()

            with self.connector.get_pipeline(transaction=True) as pipe:
                for key in record_keys:
                    pipe.hdel(schema_name, key)
                pipe.execute()

            logger.info(
                f"Bulk deleted {len(record_keys)} records from schema '{schema_name}'"
            )
        except RedisError as e:
            logger.error(f"Redis error during bulk delete: {e}")
            raise ConnectionError(f"Failed to bulk delete records: {e}")

    # Schema Management

    def clear_schema(self, schema_name: str) -> None:
        """Delete all records in a schema.

        Args:
            schema_name: The schema to clear.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()
            self.connector.cursor.delete(schema_name)
            logger.info(f"Cleared all records from schema '{schema_name}'")
        except RedisError as e:
            logger.error(f"Redis error during clear schema: {e}")
            raise ConnectionError(f"Failed to clear schema: {e}")

    def list_schemas(self) -> List[str]:
        """List all schema names in the store.

        Returns:
            List of schema names (hash keys).

        Raises:
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()
            # Get all keys that are hashes
            all_keys = self.connector.cursor.keys("*")

            schemas = []
            for key in all_keys:
                if self.connector.cursor.type(key) == b"hash":
                    schemas.append(key.decode() if isinstance(key, bytes) else key)

            return schemas
        except RedisError as e:
            logger.error(f"Redis error listing schemas: {e}")
            raise ConnectionError(f"Failed to list schemas: {e}")
