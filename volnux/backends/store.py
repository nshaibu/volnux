"""
Key-Value Store Backend Interface.

This module provides the abstract base class for implementing key-value store backends
with support for CRUD operations, filtering, and record management.
"""

import abc
import typing
import zlib
import json
import threading
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Optional, Type, Union

from .connection import BackendConnectorBase
from volnux.exceptions import SerializationError

if TYPE_CHECKING:
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


logger = logging.getLogger(__name__)


class KeyValueStoreBackendBase(abc.ABC):
    """Abstract base class for key-value store backends.

    This class defines the interface for backend storage implementations,
    providing thread-safe operations for managing records in a schema-based
    key-value store.

    Attributes:
        connector_klass: The connector class to use for backend connections.
        connector: The active backend connector instance.
    """

    connector_klass: Type[BackendConnectorBase]

    NAMESPACE_SEPARATOR = ":"

    def __init__(
        self, namespace_prefix: typing.Optional[str] = None, **connector_config: Any
    ) -> None:
        """Initialize the backend with connector configuration.

        Args:
            **connector_config: Configuration parameters passed to the connector.
        """
        self._namespace_prefix = namespace_prefix
        self.connector = self.connector_klass(**connector_config)
        self._connector_lock = threading.RLock()

    @contextmanager
    def _acquire_lock(self):
        """Context manager for thread-safe operations."""
        self._connector_lock.acquire()
        try:
            yield
        finally:
            self._connector_lock.release()

    @staticmethod
    def _create_filter_predicate(**filter_kwargs: Any) -> Callable[[Any], bool]:
        """Create a filter predicate function from keyword arguments.

        Args:
            **filter_kwargs: Attribute-value pairs to match against records.

        Returns:
            A predicate function that returns True if a record matches all criteria.

        Example:
            >>> predicate = _create_filter_predicate(status="active", age=25)
            >>> predicate(record)  # Returns True if record.status == "active" and record.age == 25
        """

        def predicate(record: Any) -> bool:
            return all(
                hasattr(record, key) and getattr(record, key) == value
                for key, value in filter_kwargs.items()
            )

        return predicate

    def close(self) -> None:
        """Close the backend connection and release resources."""
        with self._acquire_lock():
            if hasattr(self, "connector") and self.connector is not None:
                self.connector.disconnect()

    def __enter__(self) -> "KeyValueStoreBackendBase":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit with automatic cleanup."""
        self.close()

    def _build_key(self, schema_name: str, record_key: str) -> str:
        """Build a fully qualified key with namespace.

        Args:
            schema_name: The schema namespace.
            record_key: The record key within the schema.

        Returns:
            Fully-qualified key string.
        """
        parts = []
        if self._namespace_prefix:
            parts.append(self._namespace_prefix)
        parts.extend([schema_name, record_key])
        return self.NAMESPACE_SEPARATOR.join(parts)

    def _serialize_record(self, record: "KeyValueStoreIntegrationMixin") -> str:
        """Serialize a record to bytes.

        Args:
            record: The record to serialize.

        Returns:
            Serialized record as bytes.

        Raises:
            SerializationError: If serialization fails.
        """
        try:
            state = record.__getstate__()
            return json.dumps(state)
        except Exception as e:
            logger.error(f"Failed to serialize record: {e}")
            raise SerializationError(f"Serialization failed: {e}")

    def _deserialize_record(
        self, data: bytes, record_klass: Type["KeyValueStoreIntegrationMixin"]
    ) -> "KeyValueStoreIntegrationMixin":
        """Deserialize bytes to a record object.

        Args:
            data: Serialized record data.
            record_klass: The class to instantiate.

        Returns:
            Deserialized record instance.

        Raises:
            SerializationError: If deserialization fails.
        """
        try:
            state = json.loads(data)
            record = record_klass.__new__(record_klass)
            record.__setstate__(state)
            return record
        except Exception as e:
            logger.error(f"Failed to deserialize record: {e}")
            raise SerializationError(f"Deserialization failed: {e}")

    @abc.abstractmethod
    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The unique key identifying the record.

        Returns:
            True if the record exists, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """Insert a new record into the store.

        Args:
            schema_name: The schema/namespace to insert into.
            record_key: The unique key for the new record.
            record: The record object to insert.
            ttl: Optional TTL for the new record.

        Raises:
            KeyError: If a record with the same key already exists.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        """Update an existing record in the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record to update.
            record: The updated record object.

        Raises:
            KeyError: If the record does not exist.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record to delete.

        Raises:
            KeyError: If the record does not exist.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Retrieve a single record from the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record to retrieve.
            record_klass: The class to instantiate the record with.

        Returns:
            The record instance if found, None otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def filter(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> Iterable["KeyValueStoreIntegrationMixin"]:
        """Filter records matching the specified criteria.

        Args:
            schema_name: The schema/namespace to filter within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            An iterable of matching record instances.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def count(self, schema_name: str, record_klass: Type["KeyValueStoreIntegrationMixin"], **filter_kwargs: Any) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema/namespace to count within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.
        """
        raise NotImplementedError

    # @staticmethod
    # @abc.abstractmethod
    # def load_record(
    #     record_state: Dict[str, Any],
    #     record_klass: Type["KeyValueStoreIntegrationMixin"],
    # ) -> "KeyValueStoreIntegrationMixin":
    #     """Load a record from its serialized state.
    #
    #     Args:
    #         record_state: The serialized record data.
    #         record_klass: The class to instantiate the record with.
    #
    #     Returns:
    #         The instantiated record object.
    #     """
    #     raise NotImplementedError

    @abc.abstractmethod
    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> "KeyValueStoreIntegrationMixin":
        """Reload a record's data from the backend.

        Args:
            schema_name: The schema/namespace containing the record.
            record: The record to reload.

        Returns:
            The reloaded record instance.

        Raises:
            KeyError: If the record no longer exists.
        """
        raise NotImplementedError

    def upsert(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        """Insert or update a record (upsert operation).

        Args:
            schema_name: The schema/namespace for the operation.
            record_key: The key of the record.
            record: The record object to upsert.
        """
        if self.exists(schema_name, record_key):
            self.update(schema_name, record_key, record)
        else:
            self.insert(schema_name, record_key, record)

    def list_all(
        self, schema_name: str, record_klass: Type["KeyValueStoreIntegrationMixin"]
    ) -> Iterable["KeyValueStoreIntegrationMixin"]:
        """List all records in a schema.

        Args:
            schema_name: The schema/namespace to list from.
            record_klass: The class to instantiate records with.

        Returns:
            An iterable of all records in the schema.
        """
        return self.filter(schema_name, record_klass)
