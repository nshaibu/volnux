"""
Identifier quoting and core CRUD tests for PostgresStoreBackend.

The SQL-generation tests need no database. The integration tests need a
PostgreSQL server and are skipped when none is reachable. Configure with:

    VOLNUX_TEST_PG_HOST     (default: localhost)
    VOLNUX_TEST_PG_PORT     (default: 5432)
    VOLNUX_TEST_PG_DATABASE (default: volnux_test)
    VOLNUX_TEST_PG_USERNAME (default: volnux)
    VOLNUX_TEST_PG_PASSWORD (default: volnux_test_password)
"""

import asyncio
import datetime
import os
from typing import Any, Optional
from unittest.mock import MagicMock

import psycopg
import pytest
from formax import Attrib, BaseModel, InitStrategy, MiniAnnotated
from formax.typing import get_type_hints
from psycopg.types.json import Jsonb

from volnux.backends.db_utils import _safe_name
from volnux.backends.fields import FKConfig, ForeignKeyField
from volnux.backends.stores.postgres import PostgresStoreBackend, _column_list
from volnux.exceptions import ObjectDoesNotExist, SqlOperationError
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

PG_CONFIG = dict(
    host=os.environ.get("VOLNUX_TEST_PG_HOST", "localhost"),
    port=int(os.environ.get("VOLNUX_TEST_PG_PORT", "5432")),
    database=os.environ.get("VOLNUX_TEST_PG_DATABASE", "volnux_test"),
    username=os.environ.get("VOLNUX_TEST_PG_USERNAME", "volnux"),
    password=os.environ.get("VOLNUX_TEST_PG_PASSWORD", "volnux_test_password"),
)

_MIGRATION_ROOT = {"path": None}


class _PgModel(KeyValueStoreIntegrationMixin, BaseModel):
    class Config:
        init_strategy = InitStrategy.DATACLASS

    @classmethod
    def get_migration_dir(cls) -> str:
        return os.path.join(_MIGRATION_ROOT["path"], cls.__name__)


class KeywordModel(_PgModel):
    order: int
    group: str
    tags: list
    meta: dict
    nickname: Optional[str] = None


class PgParent(_PgModel):
    name: str


class PgChild(_PgModel):
    parent: ForeignKeyField[PgParent, FKConfig(nullable=True)]


def _schema(model) -> str:
    return f"volnux_{model.__name__}"


def _to_list(stream):
    return asyncio.run(stream.to_list())


def _force_native_fk(model, field_name):
    hint = get_type_hints(model, include_extras=True)[field_name]
    hint.__metadata__[0].metadata["has_native_fk"] = True


# ---------------------------------------------------------------------------
# SQL generation (no database required)
# ---------------------------------------------------------------------------


@pytest.fixture
def offline_store():
    store = PostgresStoreBackend.__new__(PostgresStoreBackend)
    store.connector = MagicMock()
    return store


def test_safe_name_quotes_and_escapes():
    assert _safe_name("order") == '"order"'
    assert _safe_name("volnux_Workflow") == '"volnux_Workflow"'
    assert _safe_name('a"b') == '"a""b"'


def test_column_list_quotes_every_column():
    assert _column_list(["id", "order", "group"]) == '"id", "order", "group"'


def test_build_sql_filter_quotes_columns(offline_store):
    where_clause, params = offline_store._build_sql_filter(
        {"order__gte": 1, "group": "g", "name__icontains": "a_b", "tags__in": [1]}
    )

    assert where_clause == (
        '"order" >= %s AND "group" = %s AND "name" ILIKE %s ESCAPE \'\\\' '
        'AND "tags" IN (%s)'
    )
    assert params == [1, "g", "%a\\_b%", 1]


def test_build_sql_filter_qualifies_columns_with_table(offline_store):
    where_clause, _ = offline_store._build_sql_filter({"id": "1"}, table="volnux_Child")

    assert where_clause == '"volnux_Child"."id" = %s'


def test_build_sql_filter_keeps_injection_inside_identifier(offline_store):
    where_clause, _ = offline_store._build_sql_filter({'x" = 1 OR "1': "v"})

    assert where_clause == '"x"" = 1 OR ""1" = %s'


@pytest.mark.parametrize(
    "order_by, expected",
    [
        (None, ""),
        ("order", ' ORDER BY "order" ASC'),
        ("-order", ' ORDER BY "order" DESC'),
        ("age; DROP TABLE x", ' ORDER BY "age; DROP TABLE x" ASC'),
    ],
)
def test_order_by_clause(offline_store, order_by, expected):
    assert offline_store._order_by_clause(order_by) == expected


@pytest.mark.parametrize(
    "uri", ["postgres://u:p@h:1/db", "postgresql://u:p@h:1/db", "psql://u:p@h:1/db"]
)
def test_migration_backend_uses_psycopg3(offline_store, monkeypatch, uri):
    captured = {}
    monkeypatch.setattr(
        "volnux.backends.stores.postgres.get_backend",
        lambda u: captured.setdefault("uri", u),
    )
    offline_store.connector.get_uri.return_value = uri

    offline_store._get_migration_backend()

    assert captured["uri"] == "postgresql+psycopg://u:p@h:1/db"


def test_prepare_record_data_wraps_json_values(offline_store):
    record = KeywordModel(order=1, group="g", tags=["a"], meta={"k": 1})

    data = offline_store._prepare_record_data(record, "r-1")

    assert isinstance(data["tags"], Jsonb) and data["tags"].obj == ["a"]
    assert isinstance(data["meta"], Jsonb) and data["meta"].obj == {"k": 1}
    assert data["order"] == 1


# ---------------------------------------------------------------------------
# Integration (requires PostgreSQL)
# ---------------------------------------------------------------------------


def _pg_available() -> bool:
    try:
        psycopg.connect(
            host=PG_CONFIG["host"],
            port=PG_CONFIG["port"],
            dbname=PG_CONFIG["database"],
            user=PG_CONFIG["username"],
            password=PG_CONFIG["password"],
            connect_timeout=2,
        ).close()
        return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(
    not _pg_available(), reason="PostgreSQL test server not reachable"
)

_ALL_TABLES = [_schema(m) for m in (KeywordModel, PgChild, PgParent)] + [
    "children",
    "results",
]


@pytest.fixture
def pg_store(tmp_path):
    _MIGRATION_ROOT["path"] = str(tmp_path / "migrations")
    store = PostgresStoreBackend(**PG_CONFIG, use_pooling=False)

    def drop_all():
        for table in _ALL_TABLES:
            store.drop_schema(table)
        # Fresh migration dirs per test, so forget yoyo's applied history
        store.execute_query(
            "DROP TABLE IF EXISTS _yoyo_migration, _yoyo_log, _yoyo_version, "
            "yoyo_lock CASCADE",
            fetch_method=None,
        )

    drop_all()
    try:
        yield store
    finally:
        drop_all()
        store.close()
        _MIGRATION_ROOT["path"] = None


@pytest.fixture
def keyword_store(pg_store):
    schema = _schema(KeywordModel)
    pg_store.insert(
        schema, "a", KeywordModel(order=3, group="g", tags=["x"], meta={"k": 1})
    )
    pg_store.insert(schema, "b", KeywordModel(order=1, group="g", tags=[], meta={}))
    pg_store.insert(schema, "c", KeywordModel(order=2, group="h", tags=[], meta={}))
    return pg_store, schema


@requires_pg
def test_mixed_case_table_is_created_and_found(pg_store):
    schema = _schema(KeywordModel)

    pg_store.ensure_schema(schema, KeywordModel)

    # Unquoted DML would look for "volnux_keywordmodel" and miss the table
    assert pg_store.schema_exists(schema) is True
    assert set(pg_store.get_cached_table_columns(schema)) == {
        "id",
        "order",
        "group",
        "tags",
        "meta",
        "nickname",
    }


@requires_pg
def test_crud_with_keyword_columns_and_json(keyword_store):
    store, schema = keyword_store

    store.update(
        schema, "b", KeywordModel(order=5, group="z", tags=["t"], meta={"m": 2})
    )
    store.upsert(schema, "d", KeywordModel(order=9, group="u", tags=[], meta={}))

    loaded = store.get(schema, "b", KeywordModel)
    assert (loaded.order, loaded.group, loaded.tags, loaded.meta) == (
        5,
        "z",
        ["t"],
        {"m": 2},
    )
    assert store.get(schema, "d", KeywordModel).order == 9
    assert store.exists(schema, "a") is True

    store.delete(schema, "a")
    assert store.exists(schema, "a") is False
    with pytest.raises(ObjectDoesNotExist):
        store.get(schema, "a", KeywordModel)


@requires_pg
def test_filter_order_by_and_count_keyword_columns(keyword_store):
    store, schema = keyword_store

    ordered = store.filter(schema, KeywordModel, order_by="-order")
    assert [r.order for r in _to_list(ordered)] == [3, 2, 1]

    paged = store.filter(schema, KeywordModel, order_by="order", limit=1, offset=1)
    assert [r.order for r in _to_list(paged)] == [2]

    assert store.count(schema, KeywordModel) == 3
    assert store.count(schema, KeywordModel, order__gte=2) == 2
    assert store.count(schema, KeywordModel, group="g") == 2


@requires_pg
def test_filter_keys_and_order_by_cannot_inject_sql(keyword_store):
    store, schema = keyword_store

    # Unquoted, this key matched every row; quoted it is an unknown column
    with pytest.raises(SqlOperationError, match="does not exist"):
        store.count(schema, KeywordModel, **{"1=1 OR group": "x"})

    with pytest.raises(SqlOperationError, match="does not exist"):
        store.filter(
            schema, KeywordModel, order_by=f'order"; DROP TABLE "{schema}"; --'
        )

    assert store.count(schema, KeywordModel) == 3


@requires_pg
def test_bulk_operations_and_table_info(pg_store):
    schema = _schema(KeywordModel)
    records = {
        f"k{i}": KeywordModel(order=i, group="g", tags=[], meta={}) for i in range(3)
    }

    assert pg_store.bulk_insert(schema, records) == 3
    info = pg_store.get_table_info(schema)
    assert info["exists"] is True
    assert info["row_count"] == 3
    assert info["total_size_bytes"] > 0

    assert pg_store.bulk_delete(schema, ["k0", "k1"]) == 2
    assert pg_store.clear_schema(schema) == 1


@requires_pg
def test_event_result_round_trip(pg_store, monkeypatch):
    from volnux.result import EventResult

    monkeypatch.setattr(
        EventResult,
        "get_migration_dir",
        classmethod(lambda cls: os.path.join(_MIGRATION_ROOT["path"], cls.__name__)),
    )
    content = datetime.datetime(2026, 1, 1)
    result = EventResult(error=False, event_name="e", content=content, order=2)

    pg_store.upsert("results", result.id, result)
    loaded = pg_store.get("results", result.id, EventResult)

    assert loaded.id == result.id
    assert loaded.order == 2
    assert loaded.content == content


@requires_pg
def test_filter_with_joins_resolves_native_fk(pg_store):
    _force_native_fk(PgChild, "parent")
    parent_schema = _schema(PgParent)

    pg_store.insert(parent_schema, "p-1", PgParent(name="root"))
    pg_store.ensure_schema("children", PgChild)
    pg_store.execute_query(
        'INSERT INTO "children" VALUES (%s, %s), (%s, %s)',
        ("c-1", "p-1", "c-2", None),
        fetch_method=None,
    )

    rows = pg_store.filter_with_joins("children", PgChild, ["parent"])
    by_id = {row["id"]: row for row in rows}

    assert by_id["c-1"]["parent__resolved"] == {"id": "p-1", "name": "root"}
    assert by_id["c-2"]["parent__resolved"] is None

    filtered = pg_store.filter_with_joins("children", PgChild, ["parent"], id="c-1")
    assert [row["id"] for row in filtered] == ["c-1"]
