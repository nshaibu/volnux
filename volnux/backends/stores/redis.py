import logging
from typing import Any, Dict, List, Optional, Type, Union, TYPE_CHECKING, cast

from redis.exceptions import RedisError

from volnux.backends.connectors.redis import RedisConnector
from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.messaging.stores.redis import (
    RedisStorePubSubMixin,
    RedisStorePushPopMixin,
    RedisStreamCapabilityMixin,
)
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError, SerializationError
from volnux.backends.q_compiler import create_filter_predicate

if TYPE_CHECKING:
    from volnux.result.stream import ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


logger = logging.getLogger(__name__)


class RedisStoreBackend(
    RedisStreamCapabilityMixin,
    RedisStorePubSubMixin,
    RedisStorePushPopMixin,
    KeyValueStoreBackendBase,
):
    """Redis-backed key-value store implementation.

    Example:
        >>> backend = RedisStoreBackend(host="localhost", port=6379, database=0)
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", UserModel, "user_1")
        >>> users = backend.filter("users", UserModel, status="active")
        >>>
        >>> # Pub/sub operations
        >>> await backend.publish("events:user", {"action": "created", "id": "user_1"})
        >>> async with backend.subscribe("events:user", EventClass) as messages:
        ...     async for msg in messages:
        ...         print(msg["data"])
        >>>
        >>> # Queue operations
        >>> await backend.push("tasks", task_1, task_2, side=QueueSide.RIGHT)
        >>> task = await backend.pop("tasks", side=QueueSide.LEFT)
    """

    NAMESPACE_SEPARATOR = ":"

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

        self._ensure_connected()

    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to check.

        Returns:
            True if the record exists, False otherwise.

        Raises:
            ConnectionError: If connection cannot be established.
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
        record: "KeyValueStoreIntegrationMixin",
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
        try:
            self._ensure_connected()

            serialized = self._serialize_record(record)
            if not isinstance(serialized, str):
                serialized = serialized.decode()

            was_set = self.connector.cursor.hsetnx(schema_name, record_key, serialized)
            if not was_set:
                raise ObjectExistError(
                    f"Record '{record_key}' already exists in schema '{schema_name}'"
                )

            if ttl is not None:
                self.connector.cursor.expire(schema_name, ttl)

            logger.debug("Inserted '%s' into '%s'", record_key, schema_name)
        except ObjectExistError:
            raise
        except SerializationError:
            raise
        except RedisError as e:
            logger.error("Redis error during insert: %s", e)
            raise ConnectionError(f"Failed to insert record: {e}") from e

    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
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
        try:
            self._ensure_connected()
            serialized = self._serialize_record(record)
            if not isinstance(serialized, str):
                serialized = serialized.decode()

            pipe = self.connector.cursor.pipeline(transaction=True)
            pipe.hexists(schema_name, record_key)
            pipe.hset(schema_name, record_key, serialized)
            results = pipe.execute()

            if not results[0]:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            logger.debug("Updated '%s' in '%s'", record_key, schema_name)
        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except RedisError as e:
            logger.error("Redis error during update: %s", e)
            raise ConnectionError(f"Failed to update record: {e}") from e

    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            ConnectionError: If Redis operation fails.
        """

        try:
            self._ensure_connected()
            deleted = self.connector.cursor.hdel(schema_name, record_key)
            if deleted == 0:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )
            logger.debug("Deleted '%s' from '%s'", record_key, schema_name)
        except ObjectDoesNotExist:
            raise
        except RedisError as e:
            logger.error("Redis error during delete: %s", e)
            raise ConnectionError(f"Failed to delete record: {e}") from e

    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> "KeyValueStoreIntegrationMixin":
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
        key = str(record_key)
        try:
            self._ensure_connected()
            serialized = self.connector.cursor.hget(schema_name, key)
            if serialized is None:
                raise ObjectDoesNotExist(
                    f"Record '{key}' does not exist in schema '{schema_name}'"
                )
            return self._deserialize_record(serialized, record_klass)
        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except RedisError as e:
            logger.error("Redis error during get: %s", e)
            raise ConnectionError(f"Failed to get record: {e}") from e

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
            limit: Maximum number of records to return (default: None, no limit).
            offset: Number of records to skip (default: None, no offset).
            order_by: Attribute to order results by (default: None, no order).
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            List of matching record instances.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()

            predicate = create_filter_predicate(**filter_kwargs)
            matching_records: List[str] = []

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
                            matching_records.append(key)
                    except SerializationError as e:
                        logger.warning(
                            f"Skipping corrupted record '{key}' in schema '{schema_name}': {e}"
                        )

                if cursor == 0:
                    break

            logger.debug(
                f"Filtered {len(matching_records)} records from schema '{schema_name}'"
            )
            return self._create_result_stream(
                record_keys=matching_records, record_klass=record_klass
            )
        except RedisError as e:
            logger.error(f"Redis error during filter: {e}")
            raise ConnectionError(f"Failed to filter records: {e}")

    def count(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()

            if not filter_kwargs:
                return cast(int, self.connector.cursor.hlen(schema_name))

            matching_records = self.filter(schema_name, record_klass, **filter_kwargs)
            return len(matching_records)
        except RedisError as e:
            logger.error(f"Redis error during count: {e}")
            raise ConnectionError(f"Failed to count records: {e}")

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
            serialized: bytes = self.connector.cursor.hget(schema_name, record_key)  # type: ignore

            if serialized is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' not found in schema '{schema_name}'"
                )

            fresh = self._deserialize_record(serialized, record.__class__)
            record.__setstate__(fresh.__getstate__())
            return record
        except SerializationError:
            raise
        except RedisError as e:
            logger.error(f"Redis error during reload: {e}")
            raise ConnectionError(f"Failed to reload record: {e}")

    def bulk_insert(
        self,
        schema_name: str,
        records: Dict[str, "KeyValueStoreIntegrationMixin"],
        ttl: Optional[int] = None,
    ) -> int:
        """Insert multiple records in a single operation.

        Args:
            schema_name: The schema to insert into.
            records: Dictionary mapping record keys to record objects.
            ttl: Time-to-live in seconds for the records. If None, records will not expire.

        Returns:
            Number of records inserted.

        Raises:
            SerializationError: If serialization fails.
            ConnectionError: If Redis operation fails.
        """
        try:
            self._ensure_connected()
            pipe = self.connector.cursor.pipeline(transaction=True)
            for key, record in records.items():
                serialized = self._serialize_record(record)
                pipe.hsetnx(schema_name, key, serialized)
            if ttl is not None:
                pipe.expire(schema_name, ttl)
            results = pipe.execute()

            hsetnx_results = results[:-1] if ttl else results
            inserted = sum(1 for r in hsetnx_results if r)
            logger.info(
                "Bulk inserted %d/%d into '%s'", inserted, len(records), schema_name
            )
            return inserted
        except SerializationError:
            raise
        except RedisError as e:
            logger.error("Redis error during bulk insert: %s", e)
            raise ConnectionError(f"Failed to bulk insert: {e}") from e

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
            pipe = self.connector.cursor.pipeline(transaction=True)
            for key in record_keys:
                pipe.hdel(schema_name, key)
            pipe.execute()
            logger.info("Bulk deleted %d from '%s'", len(record_keys), schema_name)
        except RedisError as e:
            logger.error("Redis error during bulk delete: %s", e)
            raise ConnectionError(f"Failed to bulk delete: {e}") from e

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

            all_keys = self.connector.cursor.keys("*")

            schemas = []
            for key in all_keys:
                if self.connector.cursor.type(key) == b"hash":
                    schemas.append(key.decode() if isinstance(key, bytes) else key)

            return schemas
        except RedisError as e:
            logger.error(f"Redis error listing schemas: {e}")
            raise ConnectionError(f"Failed to list schemas: {e}")
