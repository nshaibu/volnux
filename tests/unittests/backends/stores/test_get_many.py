import pytest

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend
from volnux.exceptions import ObjectDoesNotExist


class _Rec:
    def __init__(self, id):
        self.id = id


def test_inmemory_get_many_omits_missing_and_stringifies_keys():
    store = InMemoryKeyValueStoreBackend(namespace_prefix="t")
    store.insert("s", "1", _Rec("1"))
    store.insert("s", "2", _Rec("2"))

    found = store.get_many("s", [1, "2", "3"], _Rec)

    assert set(found) == {"1", "2"}


def test_base_default_get_many_falls_back_to_get_per_key():
    calls = []

    class Fallback(InMemoryKeyValueStoreBackend):
        # Use the base-class implementation, not the in-memory override.
        get_many = KeyValueStoreBackendBase.get_many

        def get(self, schema_name, record_key, record_klass):
            calls.append(record_key)
            if record_key == "gone":
                raise ObjectDoesNotExist(record_key)
            return _Rec(record_key)

    found = Fallback(namespace_prefix="t").get_many("s", ["a", "gone", "b"], _Rec)

    assert calls == ["a", "gone", "b"]
    assert set(found) == {"a", "b"}
