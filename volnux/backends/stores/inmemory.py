import copy
import typing

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.connection import BackendConnectorBase, ConnectionConfig
from volnux.backends.messaging.stores.inmemory import (
    InMemoryPubSubMixin,
    InMemoryPushPopMixin,
)
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError

if typing.TYPE_CHECKING:
    from volnux.result import ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


class DummyConnector(BackendConnectorBase):

    def __init__(self, **_: typing.Any):
        self.config = ConnectionConfig(
            host="",
            port=0,
            username="",
            password="",
            database="inmemory",
            timeout=0,
            extra_params={},
        )

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def ping(self) -> bool:
        return True

    def get_cursor(self) -> typing.Any:
        return {}

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def begin_transaction(self) -> None:
        pass


class InMemoryKeyValueStoreBackend(
    InMemoryPubSubMixin, InMemoryPushPopMixin, KeyValueStoreBackendBase
):
    """
    In-memory implementation of the KeyValueStoreBackend.

    This class provides a simple in-memory key-value storage solution. It acts
    as a backend for managing data associated with different schemas and offers
    CRUD functionalities. The data is stored in memory, and no persistence is
    provided. It is suitable for testing and scenarios where persistence is
    not required.

    Example:
        >>> backend = InMemoryKeyValueStoreBackend()
        >>>
        >>> # Key-value store operations
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", UserModel, "user_1")
        >>>
        >>> # Pub/sub operations
        >>> await backend.publish("events:user", {"action": "created"})
        >>> async with backend.subscribe("events:user") as messages:
        ...     async for msg in messages:
        ...         print(msg["data"])
        >>>
        >>> # Queue operations
        >>> await backend.push("tasks", task_1, task_2)
        >>> task = await backend.pop("tasks")

    :ivar connector_klass: Specifies the default connector class for the
        backend. This is set to `DummyConnector`.
    :type connector_klass: type
    """

    connector_klass = DummyConnector

    def __init__(self, namespace_prefix: typing.Optional[str] = None, **_: typing.Any):
        super().__init__(namespace_prefix)
        self._storage: typing.Dict[str, typing.Dict[str, KeyValueStoreBackendBase]] = {}

        # Initialize messaging mixins
        self.__post_init_pubsub__()
        self.__post_init_queues__()

    def close(self) -> None:
        with self._acquire_lock():
            self._storage.clear()

            # Cleanup pub/sub
            if hasattr(self, "_pubsub_channels"):
                self._pubsub_channels.clear()
            if hasattr(self, "_pubsub_patterns"):
                self._pubsub_patterns.clear()

            # Cleanup queues
            if hasattr(self, "_queues"):
                self._queues.clear()
            if hasattr(self, "_queue_events"):
                self._queue_events.clear()

    def create_filter_predicate(
        self, **filter_kwargs: typing.Any
    ) -> typing.Callable[[typing.Any], bool]:
        return self._create_filter_predicate(**filter_kwargs)

    def _get_schema_bucket(self, schema_name: str) -> typing.Dict[str, typing.Any]:
        return self._storage.setdefault(schema_name, {})

    def exists(self, schema_name: str, record_key: str) -> bool:
        with self._acquire_lock():
            return record_key in self._get_schema_bucket(schema_name)

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: typing.Optional[int] = None,
    ) -> None:
        del ttl
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            bucket[record_key] = record

    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            if record_key not in bucket:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )
            bucket[record_key] = record

    def delete(self, schema_name: str, record_key: str) -> None:
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            if record_key not in bucket:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )
            del bucket[record_key]

    def get(
        self,
        schema_name: str,
        record_key: typing.Union[str, int],
        record_klass: typing.Type["KeyValueStoreIntegrationMixin"],
    ) -> "KeyValueStoreIntegrationMixin":
        del record_klass
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            if str(record_key) not in bucket:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )
            return bucket[str(record_key)]

    def filter(
        self,
        schema_name: str,
        record_klass: typing.Type["KeyValueStoreIntegrationMixin"],
        limit: typing.Optional[int] = None,
        offset: typing.Optional[int] = None,
        order_by: typing.Optional[str] = None,
        **filter_kwargs: typing.Any,
    ) -> "ResultStream[KeyValueStoreIntegrationMixin]":
        predicate = self.create_filter_predicate(**filter_kwargs)

        keys = []
        bucket = self._get_schema_bucket(schema_name)
        for key, record in bucket.items():
            if predicate(record):
                keys.append(key)

        return self._create_result_stream(record_keys=keys, record_klass=record_klass)

    def count(
        self,
        schema_name: str,
        record_klass: typing.Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: typing.Any,
    ) -> int:
        return len(
            self.filter(
                schema_name,
                record_klass,
                **filter_kwargs,
            )
        )

    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> typing.Any:
        fresh = self.get(schema_name, record.id, record.__class__)
        record.__dict__.update(fresh.__dict__)
        return record

    def bulk_get(
        self,
        schema_name: str,
        record_keys: typing.List[str],
        record_klass: typing.Type["KeyValueStoreIntegrationMixin"],
    ) -> typing.List[typing.Any]:
        results: typing.List[typing.Any] = []
        for record_key in record_keys:
            try:
                results.append(self.get(schema_name, record_key, record_klass))
            except ObjectDoesNotExist:
                continue
        return results

    def bulk_delete(self, schema_name: str, record_keys: typing.List[str]) -> None:
        with self._acquire_lock():
            for record_key in record_keys:
                self._storage[schema_name].pop(record_key, None)
            self._storage.pop(schema_name, None)
