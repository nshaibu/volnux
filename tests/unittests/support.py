"""Test doubles shared by several test packages."""

from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend


class SerializingMemoryBackend(InMemoryKeyValueStoreBackend):
    """The in-memory backend, storing JSON like a real store does.

    The plain in-memory backend hands back the very objects that were saved, which
    hides every bug in how state is written and rebuilt. Here a record read back is
    a copy rebuilt from its serialised state, as it would be from Redis or a database.
    """

    def insert(self, schema_name, record_key, record, ttl=None):
        super().insert(schema_name, record_key, self._serialize_record(record), ttl)

    def update(self, schema_name, record_key, record):
        super().update(schema_name, record_key, self._serialize_record(record))

    def get(self, schema_name, record_key, record_klass):
        raw = super().get(schema_name, record_key, record_klass)
        return self._deserialize_record(raw, record_klass)

    def get_many(self, schema_name, record_keys, record_klass):
        return {
            key: self._deserialize_record(raw, record_klass)
            for key, raw in super()
            .get_many(schema_name, record_keys, record_klass)
            .items()
        }

    def filter(
        self,
        schema_name,
        record_klass,
        limit=None,
        offset=None,
        order_by=None,
        **filters,
    ):
        predicate = self.create_filter_predicate(**filters)
        bucket = self._get_schema_bucket(schema_name)
        matches = []
        for key, raw in bucket.items():
            record = self._deserialize_record(raw, record_klass)
            if predicate(record):
                matches.append((key, record))
        keys = self._order_and_page(matches, limit, offset, order_by)
        return self._create_result_stream(record_keys=keys, record_klass=record_klass)
