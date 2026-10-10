import asyncio
import collections
import datetime
import os
import re
import sqlite3
from dataclasses import InitVar
from typing import Any, Optional

import orjson as json
import pytest
from formax import Attrib, BaseModel, InitStrategy, MiniAnnotated
from formax.typing import get_type_hints
from yoyo import read_migrations

from volnux.backends.db_utils import _generate_constraint_name, is_optional_field
from volnux.backends.fields import FKConfig, ForeignKeyField
from volnux.backends.store import SQLDialect, TableBlueprint, YoyoMigrationsMixin
from volnux.backends.stores.sqlite import SqliteStoreBackend
from volnux.exceptions import (
    ObjectDoesNotExist,
    ObjectExistError,
    SerializationError,
    SqlOperationError,
)
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

# Points every test model's migration directory at the current test's tmp_path
_MIGRATION_ROOT = {"path": None}


class _TestModel(KeyValueStoreIntegrationMixin, BaseModel):
    class Config:
        init_strategy = InitStrategy.DATACLASS

    @classmethod
    def get_migration_dir(cls) -> str:
        return os.path.join(_MIGRATION_ROOT["path"], cls.__name__)


class SampleRecord(_TestModel):
    name: str
    age: int
    active: bool
    rating: float
    tags: list
    metadata: dict
    nickname: Optional[str] = None


class NullableRecord(_TestModel):
    name: str
    nickname: Optional[str] = None


class BrokenRecord(_TestModel):
    name: str

    def __getstate__(self):
        raise RuntimeError("cannot serialize")


class PatternRecord(_TestModel):
    code: MiniAnnotated[str, Attrib(pattern=re.compile(r"^ab\d+$", re.IGNORECASE))]


class ListLengthRecord(_TestModel):
    items: MiniAnnotated[list, Attrib(max_length=2)]


class KeywordRecord(_TestModel):
    order: int
    group: str
    autosave: InitVar[bool] = False


class Parent(_TestModel):
    name: str


class Child(_TestModel):
    parent: ForeignKeyField[Parent, FKConfig(nullable=True)]


class OneToOneChild(_TestModel):
    parent: ForeignKeyField[Parent, FKConfig(nullable=True)]


class Node(_TestModel):
    name: str
    parent: ForeignKeyField["Node", FKConfig(nullable=True)]


def _force_native_fk(model, field_name, **extra):
    """Native-FK detection depends on global backend config; pin it for tests."""
    hint = get_type_hints(model, include_extras=True)[field_name]
    attrib = hint.__metadata__[0]
    attrib.metadata["has_native_fk"] = True
    attrib.metadata.update(extra)


def _schema(model) -> str:
    """Physical schema name ResultStream resolves for a model."""
    return f"volnux_{model.__name__}"


def _to_list(stream):
    return asyncio.run(stream.to_list())


def _make_record(**overrides) -> SampleRecord:
    values = dict(
        name="Alice",
        age=30,
        active=True,
        rating=4.5,
        tags=["admin", "ops"],
        metadata={"team": "platform"},
        nickname=None,
    )
    values.update(overrides)
    return SampleRecord(**values)


@pytest.fixture(autouse=True)
def migration_root(tmp_path):
    _MIGRATION_ROOT["path"] = str(tmp_path / "migrations")
    yield _MIGRATION_ROOT["path"]
    _MIGRATION_ROOT["path"] = None


@pytest.fixture
def sample_record():
    return _make_record()


@pytest.fixture
def sqlite_store(tmp_path):
    store = SqliteStoreBackend(database=tmp_path / "store.db")
    try:
        yield store
    finally:
        store.close()


def _execute(store, sql, params=()):
    cursor = store.connector.get_cursor()
    try:
        cursor.execute(sql, params)
        store.connector.commit()
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Connection and schema lifecycle
# ---------------------------------------------------------------------------


def test_init_connects_connector(tmp_path):
    store = SqliteStoreBackend(database=tmp_path / "store.db")
    try:
        assert store.connector.is_connected() is True
    finally:
        store.close()


def test_ensure_schema_and_list_schemas(sqlite_store):
    sqlite_store.ensure_schema("users", SampleRecord)

    assert "users" in sqlite_store.list_schemas()
    assert sqlite_store.schema_exists("users") is True


def test_ensure_schema_is_idempotent(sqlite_store):
    sqlite_store.ensure_schema("users", SampleRecord)
    sqlite_store.ensure_schema("users", SampleRecord)

    assert sqlite_store.schema_exists("users") is True


def test_ensure_schema_reuses_migrations_for_a_fresh_database(tmp_path, sample_record):
    first = SqliteStoreBackend(database=tmp_path / "first.db")
    second = SqliteStoreBackend(database=tmp_path / "second.db")
    try:
        first.insert("users", "user-1", sample_record)
        second.insert("users", "user-1", sample_record)

        assert second.exists("users", "user-1") is True
    finally:
        first.close()
        second.close()


def test_drop_schema_removes_existing_schema(sqlite_store):
    sqlite_store.ensure_schema("users", SampleRecord)

    sqlite_store.drop_schema("users")

    assert sqlite_store.schema_exists("users") is False


def test_schema_exists_uses_cache(sqlite_store):
    sqlite_store.ensure_schema("users", SampleRecord)
    assert sqlite_store.schema_exists("users") is True

    sqlite_store.drop_schema("users")
    sqlite_store._schema_cache["users"] = True

    assert sqlite_store.schema_exists("users") is True

    sqlite_store._invalidate_schema_cache("users")

    assert sqlite_store.schema_exists("users") is False


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def test_prepare_record_data_json_encodes_collections_and_skips_private_fields(
    sqlite_store, sample_record
):
    data = sqlite_store._prepare_record_data(sample_record, "user-1")

    assert data == {
        "id": "user-1",
        "name": "Alice",
        "age": 30,
        "active": True,
        "rating": 4.5,
        "tags": '["admin","ops"]',
        "metadata": '{"team":"platform"}',
        "nickname": None,
    }


def test_serialize_record_raises_serialization_error(sqlite_store):
    record = BrokenRecord(name="broken")

    with pytest.raises(SerializationError, match="Serialization failed"):
        sqlite_store._serialize_record(record)


def test_load_record_raises_serialization_error_for_invalid_payload():
    with pytest.raises(SerializationError, match="Failed to load record"):
        SqliteStoreBackend.load_record(b"not-json", SampleRecord)


def test_is_optional_field_detects_optional_and_non_optional_fields():
    assert is_optional_field(Optional[str]) is True
    assert is_optional_field(str | None) is True
    assert is_optional_field(str) is False
    assert is_optional_field(int) is False


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def test_insert_creates_schema_automatically_and_get_returns_record(
    sqlite_store, sample_record
):
    sqlite_store.insert("users", "user-1", sample_record)
    result = sqlite_store.get("users", "user-1", SampleRecord)

    assert isinstance(result, SampleRecord)
    assert result.id == "user-1"
    assert result.name == "Alice"
    assert result.age == 30
    assert result.active
    assert result.tags == ["admin", "ops"]
    assert result.metadata == {"team": "platform"}
    assert result.nickname is None


@pytest.mark.xfail(
    strict=True,
    reason="SQLite returns BOOLEAN columns as int; reads don't coerce to bool yet",
)
def test_bool_fields_round_trip_as_bool(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    assert sqlite_store.get("users", "user-1", SampleRecord).active is True


def test_exists_returns_true_for_inserted_record(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    assert sqlite_store.exists("users", "user-1") is True
    assert sqlite_store.exists("users", "missing") is False


def test_exists_returns_false_for_missing_schema(sqlite_store):
    assert sqlite_store.exists("missing_schema", "user-1") is False


def test_insert_raises_when_record_already_exists(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    with pytest.raises(ObjectExistError, match="already exists"):
        sqlite_store.insert("users", "user-1", sample_record)


def test_get_raises_when_record_does_not_exist(sqlite_store):
    sqlite_store.ensure_schema("users", SampleRecord)

    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.get("users", "missing", SampleRecord)


def test_update_replaces_existing_record(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    updated = _make_record(
        name="Alice Updated",
        age=31,
        active=False,
        rating=4.8,
        tags=["admin", "lead"],
        metadata={"team": "platform", "region": "eu"},
        nickname="ally",
    )
    assert sqlite_store.update("users", "user-1", updated) == 1

    result = sqlite_store.get("users", "user-1", SampleRecord)

    assert result.name == "Alice Updated"
    assert result.age == 31
    assert not result.active
    assert result.tags == ["admin", "lead"]
    assert result.metadata == {"team": "platform", "region": "eu"}
    assert result.nickname == "ally"


def test_update_raises_when_record_does_not_exist(sqlite_store, sample_record):
    sqlite_store.ensure_schema("users", SampleRecord)

    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.update("users", "missing", sample_record)


def test_upsert_inserts_when_record_does_not_exist(sqlite_store, sample_record):
    sqlite_store.upsert("users", "user-1", sample_record)

    assert sqlite_store.get("users", "user-1", SampleRecord).name == "Alice"


def test_upsert_updates_when_record_exists(sqlite_store, sample_record):
    sqlite_store.upsert("users", "user-1", sample_record)

    updated = _make_record(
        name="Alice 2", age=32, rating=5.0, metadata={"team": "core"}, nickname="a2"
    )
    sqlite_store.upsert("users", "user-1", updated)

    result = sqlite_store.get("users", "user-1", SampleRecord)

    assert result.name == "Alice 2"
    assert result.age == 32
    assert result.metadata == {"team": "core"}
    assert result.nickname == "a2"


def test_delete_removes_record(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    assert sqlite_store.delete("users", "user-1") == 1
    assert sqlite_store.exists("users", "user-1") is False


def test_delete_returns_zero_when_record_does_not_exist(sqlite_store):
    sqlite_store.ensure_schema("users", SampleRecord)

    assert sqlite_store.delete("users", "missing") == 0


def test_optional_fields_may_be_null(sqlite_store):
    record = NullableRecord(name="Alice", nickname=None)

    sqlite_store.insert("users", "user-1", record)
    loaded = sqlite_store.get("users", "user-1", NullableRecord)

    assert loaded.name == "Alice"
    assert loaded.nickname is None


def test_sql_keyword_columns_round_trip(sqlite_store):
    record = KeywordRecord(order=3, group="admins")

    sqlite_store.insert("keywords", "k-1", record)
    sqlite_store.update("keywords", "k-1", KeywordRecord(order=4, group="ops"))
    sqlite_store.upsert("keywords", "k-2", KeywordRecord(order=5, group="dev"))

    loaded = sqlite_store.get("keywords", "k-1", KeywordRecord)
    assert (loaded.order, loaded.group) == (4, "ops")
    assert sqlite_store.get("keywords", "k-2", KeywordRecord).order == 5


def test_init_vars_do_not_get_columns(sqlite_store):
    sqlite_store.ensure_schema("keywords", KeywordRecord)

    assert set(sqlite_store.get_cached_table_columns("keywords")) == {
        "id",
        "order",
        "group",
    }


@pytest.mark.parametrize(
    "content",
    [
        {"key": "value"},
        datetime.datetime(2026, 1, 1),
        collections.OrderedDict(a=1),
    ],
    ids=["dict", "datetime", "ordereddict"],
)
def test_event_result_persists_and_restores_content(
    sqlite_store, migration_root, monkeypatch, content
):
    from volnux.result import EventResult

    monkeypatch.setattr(
        EventResult,
        "get_migration_dir",
        classmethod(lambda cls: os.path.join(migration_root, cls.__name__)),
    )
    result = EventResult(error=False, event_name="e", content=content, order=2)

    sqlite_store.upsert("results", result.id, result)
    loaded = sqlite_store.get("results", result.id, EventResult)

    assert loaded.id == result.id
    assert loaded.order == 2
    assert loaded.content == content
    assert type(loaded.content) is type(content)


def test_required_fields_are_not_null(sqlite_store):
    sqlite_store.ensure_schema("users", NullableRecord)

    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        _execute(sqlite_store, 'INSERT INTO "users" ("id") VALUES (?)', ("user-1",))


# ---------------------------------------------------------------------------
# Querying
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_store(sqlite_store):
    schema = _schema(SampleRecord)
    records = {
        "1": _make_record(name="Alice", age=30, tags=["admin"]),
        "2": _make_record(name="Bob", age=25, active=False, nickname="bobby"),
        "3": _make_record(name="Alicia", age=35, nickname="ali"),
    }
    for key, record in records.items():
        sqlite_store.insert(schema, key, record)
    return sqlite_store, schema


def test_filter_returns_matching_records(populated_store):
    store, schema = populated_store

    results = store.filter(
        schema,
        SampleRecord,
        active=True,
        age__gte=30,
        name__icontains="ALI",
        order_by="age",
    )

    assert [record.name for record in _to_list(results)] == ["Alice", "Alicia"]


def test_filter_honors_limit_offset_and_desc_order(populated_store):
    store, schema = populated_store

    results = store.filter(schema, SampleRecord, order_by="-age", limit=2, offset=1)

    assert [record.age for record in _to_list(results)] == [30, 25]


def test_filter_raises_for_missing_schema(sqlite_store):
    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.filter("missing_schema", SampleRecord)


def test_count_returns_total_and_filtered_counts(populated_store):
    store, schema = populated_store

    assert store.count(schema, SampleRecord) == 3
    assert store.count(schema, SampleRecord, active=True) == 2
    assert store.count(schema, SampleRecord, age__gte=30) == 2


def test_count_raises_for_missing_schema(sqlite_store):
    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.count("missing_schema", SampleRecord)


def test_build_sql_filter_supports_multiple_operators(sqlite_store):
    where_clause, params = sqlite_store._build_sql_filter(
        {
            "name__icontains": "a_i%",
            "age__gte": 30,
            "age__lte": 40,
            "nickname__isnull": True,
            "id__in": ["1", "2", "3"],
        }
    )

    assert "\"name\" LIKE ? COLLATE NOCASE ESCAPE '\\'" in where_clause
    assert '"age" >= ?' in where_clause
    assert '"age" <= ?' in where_clause
    assert '"nickname" IS NULL' in where_clause
    assert '"id" IN (?,?,?)' in where_clause
    # LIKE wildcards in the user value are escaped
    assert params == ["%a\\_i\\%%", 30, 40, "1", "2", "3"]


def test_build_sql_filter_qualifies_columns_with_table(sqlite_store):
    where_clause, _ = sqlite_store._build_sql_filter({"id": "1"}, table="users")

    assert where_clause == '"users"."id" = ?'


def test_filter_count_and_order_by_keyword_columns(sqlite_store):
    for key, order in [("a", 3), ("b", 1), ("c", 2)]:
        sqlite_store.insert(
            _schema(KeywordRecord), key, KeywordRecord(order=order, group="g")
        )

    schema = _schema(KeywordRecord)
    ordered = sqlite_store.filter(schema, KeywordRecord, order_by="-order")
    assert [r.order for r in _to_list(ordered)] == [3, 2, 1]

    assert sqlite_store.count(schema, KeywordRecord, order__gte=2) == 2
    assert sqlite_store.count(schema, KeywordRecord, group="g") == 3


def test_filter_offset_without_limit(populated_store):
    store, schema = populated_store

    results = store.filter(schema, SampleRecord, order_by="age", offset=1)

    assert [r.age for r in _to_list(results)] == [30, 35]


@pytest.mark.parametrize(
    "lookup",
    [
        {'name" = "name" OR "1': "x"},
        {"1=1 OR name": "x"},
        {'name"--': "x"},
    ],
)
def test_filter_keys_cannot_inject_sql(populated_store, lookup):
    store, schema = populated_store

    # Unquoted, "1=1 OR name" matched every row. Quoted, the key is one
    # identifier; SQLite treats an unknown quoted identifier as a string
    # literal (legacy DQS behaviour), so it simply matches nothing.
    assert store.count(schema, SampleRecord, **lookup) == 0


def test_order_by_cannot_inject_sql(populated_store):
    store, schema = populated_store
    store.ensure_schema("victim", NullableRecord)

    results = store.filter(schema, SampleRecord, order_by='age"; DROP TABLE victim; --')

    assert len(results) == 3
    assert store.schema_exists("victim") is True


def test_filter_with_joins_resolves_native_fk(sqlite_store):
    _force_native_fk(Child, "parent")
    parent_schema = _schema(Parent)

    sqlite_store.insert(parent_schema, "p-1", Parent(name="root"))
    sqlite_store.ensure_schema("children", Child)
    _execute(sqlite_store, 'INSERT INTO "children" VALUES (?, ?)', ("c-1", "p-1"))
    _execute(sqlite_store, 'INSERT INTO "children" VALUES (?, ?)', ("c-2", None))

    rows = sqlite_store.filter_with_joins("children", Child, ["parent"])
    by_id = {row["id"]: row for row in rows}

    assert by_id["c-1"]["parent__resolved"] == {"id": "p-1", "name": "root"}
    assert by_id["c-2"]["parent__resolved"] is None

    # "id" exists on both tables; the filter must target the main table
    filtered = sqlite_store.filter_with_joins("children", Child, ["parent"], id="c-1")
    assert [row["id"] for row in filtered] == ["c-1"]


def test_build_sql_filter_with_empty_in_matches_nothing(sqlite_store):
    where_clause, params = sqlite_store._build_sql_filter({"id__in": []})

    assert where_clause == "0"
    assert params == []


# ---------------------------------------------------------------------------
# Reload
# ---------------------------------------------------------------------------


def test_reload_returns_fresh_state_from_database(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)
    loaded = sqlite_store.get("users", "user-1", SampleRecord)

    sqlite_store.update(
        "users",
        "user-1",
        _make_record(name="Alice Reloaded", age=33, metadata={"team": "runtime"}),
    )

    reloaded = sqlite_store.reload("users", loaded)

    assert reloaded.id == "user-1"
    assert reloaded.name == "Alice Reloaded"
    assert reloaded.age == 33
    assert reloaded.metadata == {"team": "runtime"}


def test_reload_raises_when_record_has_no_id(sqlite_store):
    class NoIdRecord:
        pass

    with pytest.raises(ValueError, match="must have an 'id' attribute"):
        sqlite_store.reload("users", NoIdRecord())


def test_reload_raises_when_record_no_longer_exists(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)
    loaded = sqlite_store.get("users", "user-1", SampleRecord)
    sqlite_store.delete("users", "user-1")

    with pytest.raises(ObjectDoesNotExist, match="no longer exists"):
        sqlite_store.reload("users", loaded)


def test_ensure_connected_reconnects_when_connection_is_lost(sqlite_store):
    sqlite_store.connector.is_connected = lambda: False

    called = {"count": 0}

    def reconnect():
        called["count"] += 1

    sqlite_store.connector.connect = reconnect

    sqlite_store._ensure_connected()

    assert called["count"] == 1


# ---------------------------------------------------------------------------
# Schema compilation: constraints
# ---------------------------------------------------------------------------


def test_pattern_constraint_is_enforced_with_regexp(sqlite_store):
    sqlite_store.ensure_schema("codes", PatternRecord)

    _execute(sqlite_store, 'INSERT INTO "codes" VALUES (?, ?)', ("1", "AB12"))

    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        _execute(sqlite_store, 'INSERT INTO "codes" VALUES (?, ?)', ("2", "zz"))


def test_regexp_function_is_registered_on_connection(sqlite_store):
    cursor = sqlite_store.connector.get_cursor()
    try:
        cursor.execute("SELECT 'abc' REGEXP 'b', 'abc' REGEXP '^b', NULL REGEXP 'b'")
        assert tuple(cursor.fetchone()) == (1, 0, None)
    finally:
        cursor.close()


def test_list_max_length_is_not_applied_to_serialized_text(sqlite_store):
    graph = {}
    sqlite_store.compile_schema("lists", ListLengthRecord, graph, set())

    blueprint = graph["lists"]
    assert blueprint.chk_constraints == []
    assert '"items" TEXT NOT NULL' in blueprint.columns


# ---------------------------------------------------------------------------
# Schema compilation: foreign keys
# ---------------------------------------------------------------------------


def test_self_referencing_fk_is_declared_inline_and_enforced(sqlite_store):
    _force_native_fk(Node, "parent")
    schema = _schema(Node)

    sqlite_store.ensure_schema(schema, Node)

    _execute(
        sqlite_store, f'INSERT INTO "{schema}" VALUES (?, ?, ?)', ("1", "root", None)
    )
    _execute(
        sqlite_store, f'INSERT INTO "{schema}" VALUES (?, ?, ?)', ("2", "leaf", "1")
    )
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        _execute(
            sqlite_store, f'INSERT INTO "{schema}" VALUES (?, ?, ?)', ("3", "x", "nope")
        )


def test_fk_dependency_baseline_is_written_to_its_own_migration_dir(
    sqlite_store, migration_root
):
    _force_native_fk(Child, "parent")

    sqlite_store.ensure_schema("children", Child)

    assert sqlite_store.schema_exists(_schema(Parent)) is True
    assert sqlite_store.schema_exists("children") is True
    assert sqlite_store.has_fk_constraint("children", "parent_object_id") is True

    parent_files = os.listdir(os.path.join(migration_root, "Parent"))
    child_files = os.listdir(os.path.join(migration_root, "Child"))
    assert any(f.endswith(f"_{_schema(Parent)}_init.py") for f in parent_files)
    assert not any(_schema(Parent) in f for f in child_files)


def test_fk_baseline_creates_index_as_separate_step(sqlite_store, migration_root):
    _force_native_fk(Child, "parent")

    sqlite_store.ensure_schema("children", Child)

    migrations = read_migrations(os.path.join(migration_root, "Child"))
    migration = next(m for m in migrations if m.id.endswith("children_init"))
    migration.load()
    assert len(migration.steps) == 2

    index_name = _generate_constraint_name("children_parent_object_id", "idx", "fk")
    cursor = sqlite_store.connector.get_cursor()
    try:
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (index_name,)
        )
        assert cursor.fetchone() is not None
    finally:
        cursor.close()


def test_unique_fk_column_is_one_to_one(sqlite_store):
    _force_native_fk(OneToOneChild, "parent", unique=True)
    graph = {}

    sqlite_store.compile_schema("o2o", OneToOneChild, graph, set())

    assert '"parent_object_id" VARCHAR(255) UNIQUE' in graph["o2o"].columns


def test_resolve_target_model_for_fk_field():
    assert SqliteStoreBackend.resolve_target_model_for_field(Child, "parent") is Parent
    assert SqliteStoreBackend.resolve_target_model_for_field(Node, "parent") is Node


def test_resolve_target_model_rejects_non_model_fields():
    with pytest.raises(ValueError, match="is not a schema"):
        SqliteStoreBackend.resolve_target_model_for_field(Parent, "name")

    with pytest.raises(ValueError, match="not found"):
        SqliteStoreBackend.resolve_target_model_for_field(Parent, "missing")


class _StubModel:
    """Plain class: the mixin can't yet resolve mutual forward references."""

    @classmethod
    def get_migration_dir(cls):
        return os.path.join(_MIGRATION_ROOT["path"], cls.__name__)

    @classmethod
    def get_storage_route(cls):
        return cls.__name__.lower()


def _stub_fk(target):
    return MiniAnnotated[
        Any,
        Attrib(
            metadata={
                "type": "foreignkey",
                "target_model": target,
                "has_native_fk": True,
                "nullable": True,
            }
        ),
    ]


class _RouteAsSchema(SqliteStoreBackend):
    def resolve_physical_target(self, info, overrides=None):
        return info


@pytest.fixture
def mutual_models():
    a = type("A", (_StubModel,), {})
    b = type("B", (_StubModel,), {})
    a.__annotations__ = {"b": _stub_fk(b)}
    b.__annotations__ = {"a": _stub_fk(a)}
    return a, b


def test_mutual_fk_cycle_is_declared_inline_on_sqlite(tmp_path, mutual_models):
    a, _ = mutual_models
    store = _RouteAsSchema(database=tmp_path / "cycle.db")
    try:
        graph = {}
        store.compile_schema("a", a, graph, set())

        # Dependency is compiled (and so created) first
        assert list(graph) == ["b", "a"]
        assert all(len(bp.fk_constraints) == 1 for bp in graph.values())
        assert all(not bp.deferred_alter_statements for bp in graph.values())

        for blueprint in graph.values():
            _execute(store, blueprint.compile_base_create_sql())
        assert store.has_fk_constraint("a", "b_object_id") is True
        assert store.has_fk_constraint("b", "a_object_id") is True
    finally:
        store.close()


def test_mutual_fk_cycle_is_deferred_on_postgres(mutual_models):
    a, _ = mutual_models

    class PostgresCompiler(YoyoMigrationsMixin):
        dialect = SQLDialect.POSTGRES
        _id_field_length = 36

        def resolve_physical_target(self, info):
            return info

        def map_python_type_to_sql(self, field_type):
            return "TEXT"

    graph = {}
    PostgresCompiler().compile_schema("a", a, graph, set())

    assert graph["b"].fk_constraints == []
    assert graph["b"].deferred_alter_statements == [("a_object_id", "a", "NO ACTION")]
    assert len(graph["a"].fk_constraints) == 1


# ---------------------------------------------------------------------------
# Migration files
# ---------------------------------------------------------------------------


def test_write_yoyo_migration_step_preserves_sql_and_rollbacks(tmp_path):
    apply_sql = """CREATE TABLE "t" ("e" TEXT CHECK ("e" REGEXP '^\\d+\\b'))"""
    path = YoyoMigrationsMixin.write_yoyo_migration_step(
        str(tmp_path),
        "t_init",
        [
            (apply_sql, 'DROP TABLE IF EXISTS "t"'),
            ('CREATE INDEX "i" ON "t" ("e")', None),
        ],
    )

    migration = read_migrations(str(tmp_path))[0]
    migration.load()

    assert os.path.basename(path).endswith("_t_init.py")
    # repr() embedding keeps regex escapes and quotes verbatim
    assert [s.step._apply for s in migration.steps] == [
        apply_sql,
        'CREATE INDEX "i" ON "t" ("e")',
    ]
    assert migration.steps[0].step._rollback == 'DROP TABLE IF EXISTS "t"'


def test_long_constraint_names_are_stable_and_bounded():
    long_name = "a_really_long_schema_name_for_testing_" * 3

    name = _generate_constraint_name(long_name, "idx", "fk")

    assert len(name.encode("utf-8")) <= 63
    assert name.startswith("idx_") and name.endswith("_fk")
    assert name == _generate_constraint_name(long_name, "idx", "fk")
