import logging
import threading
import time
import uuid
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    TYPE_CHECKING,
)

from confluent_kafka import KafkaException, TopicPartition

from volnux.backends.connectors.kafka import KafkaConnector
from volnux.backends.messaging.stores.kafka import (
    EXPIRES_HEADER,
    KIND_KV,
    KafkaStorePubSubMixin,
    KafkaStorePushPopMixin,
    KafkaStreamCapabilityMixin,
    _header,
    topic_name,
)
from volnux.backends.q_compiler import create_filter_predicate, resolve_field_value
from volnux.backends.store import KeyValueStoreBackendBase
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError, SerializationError

if TYPE_CHECKING:
    from volnux.result.stream import ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)


class _KVTable:
    """In-process materialisation of one compacted topic (a "table")."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.lock = threading.RLock()
        # key -> (serialized record, expiry in epoch ms or None)
        self.rows: Dict[str, Tuple[bytes, Optional[int]]] = {}
        self.positions: Dict[int, int] = {}  # partition -> next offset to read
        self.partitions: Tuple[int, ...] = ()
        self.consumer: Any = None

    def live(self, now_ms: int) -> Iterator[Tuple[str, bytes]]:
        for key, (raw, expires) in list(self.rows.items()):
            if expires is None or expires > now_ms:
                yield key, raw

    def get(self, key: str, now_ms: int) -> Optional[bytes]:
        row = self.rows.get(key)
        if row is None:
            return None
        raw, expires = row
        return raw if (expires is None or expires > now_ms) else None


class KafkaStoreBackend(
    KafkaStreamCapabilityMixin,
    KafkaStorePubSubMixin,
    KafkaStorePushPopMixin,
    KeyValueStoreBackendBase,
):
    """Kafka-backed store: key-value records plus pub/sub, queues and streams.

    Example:
        >>> backend = KafkaStoreBackend(host="localhost", port=9092)
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", "user_1", UserModel)
        >>>
        >>> await backend.publish("events.user", user_record)
        >>> await backend.push("tasks", task_1, task_2)
        >>> task = await backend.pop("tasks", TaskModel)
        >>> await backend.stream_append("audit", entry)

    **Key-value semantics.** Each schema is a log-compacted topic
    (``<schema>.kv``); the latest message per key is the record and a null
    message (tombstone) deletes it. Reads first catch an in-process copy of the
    table up to the topic's end, so a process always reads its own writes and
    other processes' writes that were acknowledged before the read began. This
    is a good fit for small, slowly changing tables (configuration, registries,
    workflow definitions), **not** for large or hot tables:

    * ``insert`` / ``update`` check-then-write under a process-local lock. Kafka
      has no compare-and-set, so two *processes* can both insert the same key
      (last write wins).
    * Every read costs a metadata round trip, and the table is held in memory.
    * ``ttl`` is enforced lazily on read; expired records are not physically
      removed until compaction.
    * Transactions are not supported.

    Backend options (keyword arguments, alongside the connector settings):
    ``num_partitions`` (1), ``replication_factor`` (1), ``topic_config`` ({}),
    ``pubsub_retention_ms`` (1h), ``group_join_timeout`` (15s),
    ``produce_timeout`` (30s), ``max_blocking`` (32), ``pattern_refresh_interval``
    (1s) and ``kv_sync_timeout`` (30s).
    """

    # "." is legal in topic names and ":" is not
    NAMESPACE_SEPARATOR = "."

    supports_cross_process = True

    connector_klass = KafkaConnector

    def __init__(self, **connector_config: Any):
        """Initialize the Kafka store backend.

        Args:
            **connector_config: Connector and backend options; see
                ``KafkaConnector`` and the class docstring.
        """
        super().__init__(**connector_config)
        self._tables: Dict[str, _KVTable] = {}
        self._tables_lock = threading.Lock()
        self._ensure_connected()

    def close(self) -> None:
        with self._tables_lock:
            tables = list(self._tables.values())
            self._tables.clear()
        for table in tables:
            with table.lock:
                if table.consumer is not None:
                    try:
                        table.consumer.close()
                    except Exception as exc:
                        logger.debug("Error closing Kafka table consumer: %s", exc)
                    table.consumer = None
        self.close_messaging()
        super().close()

    # table plumbing

    def _table(self, schema_name: str) -> _KVTable:
        with self._tables_lock:
            table = self._tables.get(schema_name)
            if table is None:
                table = _KVTable(topic_name(schema_name, KIND_KV))
                self._tables[schema_name] = table
            return table

    def _sync(self, table: _KVTable) -> None:
        """Catch ``table`` up to the end of its topic. Caller holds ``table.lock``."""
        self._ensure_connected()
        marks = self._watermarks(table.topic)
        if not marks:  # topic doesn't exist yet: an empty table
            return

        if table.consumer is None:
            table.consumer = self.connector.new_consumer(
                {
                    "group.id": f"volnux-kv-{uuid.uuid4().hex}",
                    "auto.offset.reset": "earliest",
                    "enable.partition.eof": False,
                }
            )
        partitions = tuple(sorted(marks))
        if partitions != table.partitions:
            # First sync, or partitions were added: (re)assign from where we are.
            table.consumer.assign(
                [
                    TopicPartition(
                        table.topic,
                        p,
                        max(table.positions.get(p, marks[p][0]), marks[p][0]),
                    )
                    for p in partitions
                ]
            )
            table.partitions = partitions

        for partition, (low, _) in marks.items():
            table.positions[partition] = max(table.positions.get(partition, low), low)

        deadline = time.monotonic() + float(
            self.connector.config.extra_params.get("kv_sync_timeout", 30.0)
        )
        idle = 0
        while any(table.positions[p] < marks[p][1] for p in partitions):
            applied = 0
            for msg in table.consumer.consume(num_messages=500, timeout=0.5):
                if msg.error() is not None:
                    if msg.error().fatal():
                        raise KafkaException(msg.error())
                    continue
                self._apply(table, msg)
                table.positions[msg.partition()] = msg.offset() + 1
                applied += 1
            idle = 0 if applied else idle + 1
            # Compaction can remove the topic's last offsets, so positions may
            # never reach the high watermark: stop after a few empty fetches.
            if idle >= 4:
                break
            if time.monotonic() > deadline:
                raise ConnectionError(
                    f"Timed out catching up Kafka table '{table.topic}'"
                )

    @staticmethod
    def _apply(table: _KVTable, msg: Any) -> None:
        key = msg.key().decode()
        if msg.value() is None:  # tombstone
            table.rows.pop(key, None)
            return
        expires = _header(msg, EXPIRES_HEADER)
        table.rows[key] = (msg.value(), int(expires) if expires else None)

    def _write(
        self,
        schema_name: str,
        table: _KVTable,
        key: str,
        value: Optional[bytes],
        ttl: Optional[int] = None,
    ) -> None:
        self._ensure_topic_sync(table.topic, KIND_KV)
        headers = (
            [(EXPIRES_HEADER, str(int((time.time() + ttl) * 1000)).encode())]
            if (ttl is not None and value is not None)
            else None
        )
        self._produce_sync(table.topic, [(key.encode(), value, headers)])

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _serialize(self, record: "KeyValueStoreIntegrationMixin") -> bytes:
        serialized = self._serialize_record(record)
        return serialized if isinstance(serialized, bytes) else serialized.encode()

    # key-value operations

    def exists(self, schema_name: str, record_key: str) -> bool:
        table = self._table(schema_name)
        try:
            with table.lock:
                self._sync(table)
                return table.get(str(record_key), self._now_ms()) is not None
        except KafkaException as exc:
            raise ConnectionError(f"Failed to check record existence: {exc}") from exc

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        key, table = str(record_key), self._table(schema_name)
        value = self._serialize(record)
        try:
            self._ensure_topic_sync(table.topic, KIND_KV)
            with table.lock:
                self._sync(table)
                if table.get(key, self._now_ms()) is not None:
                    raise ObjectExistError(
                        f"Record '{key}' already exists in schema '{schema_name}'"
                    )
                self._write(schema_name, table, key, value, ttl)
        except KafkaException as exc:
            raise ConnectionError(f"Failed to insert record: {exc}") from exc

    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        key, table = str(record_key), self._table(schema_name)
        value = self._serialize(record)
        try:
            with table.lock:
                self._sync(table)
                if table.get(key, self._now_ms()) is None:
                    raise ObjectDoesNotExist(
                        f"Record '{key}' does not exist in schema '{schema_name}'"
                    )
                self._write(schema_name, table, key, value)
        except KafkaException as exc:
            raise ConnectionError(f"Failed to update record: {exc}") from exc

    def upsert(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        """Unconditional write: one produce, no read (the default does two)."""
        key, table = str(record_key), self._table(schema_name)
        value = self._serialize(record)
        try:
            with table.lock:
                self._write(schema_name, table, key, value)
        except KafkaException as exc:
            raise ConnectionError(f"Failed to upsert record: {exc}") from exc

    def delete(self, schema_name: str, record_key: str) -> None:
        key, table = str(record_key), self._table(schema_name)
        try:
            with table.lock:
                self._sync(table)
                if table.get(key, self._now_ms()) is None:
                    raise ObjectDoesNotExist(
                        f"Record '{key}' does not exist in schema '{schema_name}'"
                    )
                self._write(schema_name, table, key, None)  # tombstone
        except KafkaException as exc:
            raise ConnectionError(f"Failed to delete record: {exc}") from exc

    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> "KeyValueStoreIntegrationMixin":
        key, table = str(record_key), self._table(schema_name)
        try:
            with table.lock:
                self._sync(table)
                raw = table.get(key, self._now_ms())
        except KafkaException as exc:
            raise ConnectionError(f"Failed to get record: {exc}") from exc
        if raw is None:
            raise ObjectDoesNotExist(
                f"Record '{key}' does not exist in schema '{schema_name}'"
            )
        return self._deserialize_record(raw, record_klass)

    def get_many(
        self,
        schema_name: str,
        record_keys: Any,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> Dict[str, "KeyValueStoreIntegrationMixin"]:
        """All keys from a single catch-up, instead of one per key."""
        table = self._table(schema_name)
        try:
            with table.lock:
                self._sync(table)
                now = self._now_ms()
                raws = {str(k): table.get(str(k), now) for k in record_keys}
        except KafkaException as exc:
            raise ConnectionError(f"Failed to get records: {exc}") from exc
        return {
            key: self._deserialize_record(raw, record_klass)
            for key, raw in raws.items()
            if raw is not None
        }

    def _scan(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> List[Tuple[str, "KeyValueStoreIntegrationMixin"]]:
        predicate = create_filter_predicate(**filter_kwargs)
        table = self._table(schema_name)
        try:
            with table.lock:
                self._sync(table)
                snapshot = list(table.live(self._now_ms()))
        except KafkaException as exc:
            raise ConnectionError(
                f"Failed to read schema '{schema_name}': {exc}"
            ) from exc

        matches: List[Tuple[str, "KeyValueStoreIntegrationMixin"]] = []
        for key, raw in snapshot:
            try:
                record = self._deserialize_record(raw, record_klass)
            except SerializationError as exc:
                logger.warning(
                    "Skipping corrupted record '%s' in schema '%s': %s",
                    key,
                    schema_name,
                    exc,
                )
                continue
            if predicate(record):
                matches.append((key, record))
        return matches

    def filter(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        **filter_kwargs: Any,
    ) -> "ResultStream[KeyValueStoreIntegrationMixin]":
        matches = self._scan(schema_name, record_klass, **filter_kwargs)

        if order_by:
            descending = order_by.startswith("-")
            field = order_by.lstrip("-")

            def sort_key(item: Tuple[str, Any]) -> Tuple[int, Any]:
                value, found = resolve_field_value(item[1], field)
                return (1, 0) if (not found or value is None) else (0, value)

            present = [m for m in matches if sort_key(m)[0] == 0]
            missing = [m for m in matches if sort_key(m)[0] == 1]
            present.sort(key=lambda m: sort_key(m)[1], reverse=descending)
            matches = present + missing  # records without the field sort last

        start = offset or 0
        matches = matches[start : None if limit is None else start + limit]
        return self._create_result_stream(
            record_keys=[key for key, _ in matches], record_klass=record_klass
        )

    def count(
        self,
        schema_name: str,
        record_klass: Optional[Type["KeyValueStoreIntegrationMixin"]] = None,
        **filter_kwargs: Any,
    ) -> int:
        if not filter_kwargs:
            table = self._table(schema_name)
            try:
                with table.lock:
                    self._sync(table)
                    return sum(1 for _ in table.live(self._now_ms()))
            except KafkaException as exc:
                raise ConnectionError(f"Failed to count records: {exc}") from exc
        if record_klass is None:
            raise ValueError("record_klass is required to count with filters")
        return len(self._scan(schema_name, record_klass, **filter_kwargs))

    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> "KeyValueStoreIntegrationMixin":
        if not hasattr(record, "id"):
            raise ValueError("Record must have an 'id' attribute for reload")
        try:
            fresh = self.get(schema_name, str(record.id), record.__class__)
        except ObjectDoesNotExist:
            raise ObjectDoesNotExist(
                f"Record '{record.id}' no longer exists in schema '{schema_name}'"
            )
        record.__setstate__(fresh.__getstate__())
        return record

    def bulk_insert(
        self,
        schema_name: str,
        records: Dict[str, "KeyValueStoreIntegrationMixin"],
        ttl: Optional[int] = None,
    ) -> int:
        table = self._table(schema_name)
        encoded = {str(k): self._serialize(r) for k, r in records.items()}
        try:
            self._ensure_topic_sync(table.topic, KIND_KV)
            with table.lock:
                self._sync(table)
                now = self._now_ms()
                clashes = [k for k in encoded if table.get(k, now) is not None]
                if clashes:
                    raise ObjectExistError(
                        f"Records already exist in schema '{schema_name}': {clashes[:5]}"
                    )
                headers = (
                    [(EXPIRES_HEADER, str(int((time.time() + ttl) * 1000)).encode())]
                    if ttl is not None
                    else None
                )
                self._produce_sync(
                    table.topic, [(k.encode(), v, headers) for k, v in encoded.items()]
                )
        except KafkaException as exc:
            raise ConnectionError(f"Failed to bulk insert: {exc}") from exc
        return len(encoded)

    def bulk_delete(self, schema_name: str, record_keys: List[str]) -> int:
        table = self._table(schema_name)
        try:
            with table.lock:
                self._sync(table)
                now = self._now_ms()
                present = [
                    str(k) for k in record_keys if table.get(str(k), now) is not None
                ]
                if present:
                    self._produce_sync(
                        table.topic, [(k.encode(), None, None) for k in present]
                    )
        except KafkaException as exc:
            raise ConnectionError(f"Failed to bulk delete: {exc}") from exc
        return len(present)

    def clear_schema(self, schema_name: str) -> None:
        """Tombstone every record. (Dropping the topic would race with readers.)"""
        table = self._table(schema_name)
        try:
            with table.lock:
                self._sync(table)
                keys = list(table.rows)
                if keys:
                    self._produce_sync(
                        table.topic, [(k.encode(), None, None) for k in keys]
                    )
        except KafkaException as exc:
            raise ConnectionError(f"Failed to clear schema: {exc}") from exc

    def list_schemas(self) -> List[str]:
        """Names of the key-value topics (as sanitized topic bases)."""
        suffix = f".{KIND_KV}"
        metadata = self.connector.admin.list_topics(
            timeout=self.connector.request_timeout
        )
        return sorted(
            name[: -len(suffix)]
            for name, meta in metadata.topics.items()
            if name.endswith(suffix) and meta.error is None
        )
