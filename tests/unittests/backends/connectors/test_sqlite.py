from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from volnux.backends.connectors.sqlite import SqliteConnector


@pytest.fixture
def sqlite_connector():
    return SqliteConnector(database=":memory:")


def test_get_connection_params_includes_only_supported_sqlite_connect_args():
    connector = SqliteConnector(
        database=":memory:",
        timeout=12.5,
        check_same_thread=True,
        isolation_level="DEFERRED",
        detect_types=3,
        cached_statements=256,
        uri=True,
        enable_foreign_keys=False,
        enable_wal=False,
        unsupported_option="ignored-value",
    )

    params = connector._get_connection_params()

    assert params == {
        "database": ":memory:",
        "timeout": 12.5,
        "check_same_thread": True,
        "isolation_level": "DEFERRED",
        "detect_types": 3,
        "cached_statements": 256,
        "uri": True,
    }


def test_get_connection_params_allows_supported_extra_factory_param():
    factory = MagicMock()

    connector = SqliteConnector(
        database=":memory:",
        factory=factory,
        unsupported_option="ignored-value",
    )

    params = connector._get_connection_params()

    assert params["database"] == ":memory:"
    assert params["factory"] is factory
    assert "unsupported_option" not in params


@patch("volnux.backends.connectors.sqlite.sqlite3.connect")
def test_connect_success(mock_connect, sqlite_connector):
    connection = MagicMock()
    version_cursor = MagicMock()
    version_cursor.fetchone.return_value = ["3.45.0"]

    main_cursor = MagicMock()

    connection.cursor.side_effect = [main_cursor, version_cursor]
    mock_connect.return_value = connection

    with patch.object(sqlite_connector, "_configure_connection") as mock_configure:
        sqlite_connector.connect()

    mock_connect.assert_called_once_with(**sqlite_connector._get_connection_params())
    mock_configure.assert_called_once_with(connection)
    assert sqlite_connector._connection is connection
    assert sqlite_connector._cursor is main_cursor
    assert sqlite_connector._is_connected is True
    assert connection.row_factory is not None


@patch("volnux.backends.connectors.sqlite.sqlite3.connect")
def test_connect_returns_early_when_already_connected(mock_connect, sqlite_connector):
    sqlite_connector._is_connected = True

    sqlite_connector.connect()

    mock_connect.assert_not_called()


@patch("volnux.backends.connectors.sqlite.sqlite3.connect")
def test_connect_raises_sqlite_error_as_connection_error(
    mock_connect, sqlite_connector
):
    import sqlite3

    mock_connect.side_effect = sqlite3.Error("db unavailable")

    with pytest.raises(ConnectionError, match="SQLite connection failed"):
        sqlite_connector.connect()


@patch("volnux.backends.connectors.sqlite.sqlite3.connect")
def test_connect_raises_unexpected_error_as_connection_error(
    mock_connect, sqlite_connector
):
    mock_connect.side_effect = RuntimeError("boom")

    with pytest.raises(ConnectionError, match="Unexpected connection error"):
        sqlite_connector.connect()


def test_disconnect_closes_resources_and_resets_state(sqlite_connector):
    cursor = MagicMock()
    connection = MagicMock()

    sqlite_connector._cursor = cursor
    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector._isolation_level = "DEFERRED"

    sqlite_connector.disconnect()

    cursor.close.assert_called_once()
    connection.commit.assert_called_once()
    connection.close.assert_called_once()
    assert sqlite_connector._cursor is None
    assert sqlite_connector._connection is None
    assert sqlite_connector._is_connected is False


def test_disconnect_is_noop_when_not_connected_and_no_connection(sqlite_connector):
    sqlite_connector._cursor = None
    sqlite_connector._connection = None
    sqlite_connector._is_connected = False

    sqlite_connector.disconnect()

    assert sqlite_connector._cursor is None
    assert sqlite_connector._connection is None
    assert sqlite_connector._is_connected is False


def test_is_connected_returns_false_when_not_connected(sqlite_connector):
    sqlite_connector._is_connected = False
    sqlite_connector._connection = None

    assert sqlite_connector.is_connected() is False


def test_is_connected_returns_true_when_health_check_succeeds(sqlite_connector):
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor

    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True

    assert sqlite_connector.is_connected() is True
    cursor.execute.assert_called_once_with("SELECT 1")
    cursor.fetchone.assert_called_once()
    cursor.close.assert_called_once()


def test_is_connected_returns_false_and_resets_state_on_error(sqlite_connector):
    import sqlite3

    connection = MagicMock()
    connection.cursor.side_effect = sqlite3.Error("broken")

    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True

    assert sqlite_connector.is_connected() is False
    assert sqlite_connector._is_connected is False


def test_ping_delegates_to_is_connected(sqlite_connector):
    sqlite_connector.is_connected = MagicMock(return_value=True)

    assert sqlite_connector.ping() is True
    sqlite_connector.is_connected.assert_called_once()


def test_get_cursor_returns_new_cursor_when_connected(sqlite_connector):
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor

    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector.is_connected = MagicMock(return_value=True)

    result = sqlite_connector.get_cursor()

    assert result is cursor
    connection.cursor.assert_called_once()


def test_get_cursor_reconnects_via_ensure_connected_when_needed(sqlite_connector):
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor

    sqlite_connector._connection = connection
    sqlite_connector.is_connected = MagicMock(return_value=False)
    sqlite_connector.connect = MagicMock(
        side_effect=lambda: setattr(sqlite_connector, "_is_connected", True)
    )

    result = sqlite_connector.get_cursor()

    assert result is cursor
    sqlite_connector.connect.assert_called_once()


def test_begin_transaction_executes_begin_when_autocommit_mode(sqlite_connector):
    connection = MagicMock()
    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector._isolation_level = None
    sqlite_connector.is_connected = MagicMock(return_value=True)

    sqlite_connector.begin_transaction()

    connection.execute.assert_called_once_with("BEGIN")


def test_begin_transaction_does_not_execute_begin_when_isolation_level_is_set(
    sqlite_connector,
):
    connection = MagicMock()
    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector._isolation_level = "DEFERRED"
    sqlite_connector.is_connected = MagicMock(return_value=True)

    sqlite_connector.begin_transaction()

    connection.execute.assert_not_called()


def test_commit_calls_connection_commit(sqlite_connector):
    connection = MagicMock()
    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector.is_connected = MagicMock(return_value=True)

    sqlite_connector.commit()

    connection.commit.assert_called_once()


def test_rollback_calls_connection_rollback(sqlite_connector):
    connection = MagicMock()
    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector.is_connected = MagicMock(return_value=True)

    sqlite_connector.rollback()

    connection.rollback.assert_called_once()


def test_execute_script_runs_script_and_commits(sqlite_connector):
    connection = MagicMock()
    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector.is_connected = MagicMock(return_value=True)

    script = """
    CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO sample (name) VALUES ('a');
    """

    sqlite_connector.execute_script(script)

    connection.executescript.assert_called_once_with(script)
    connection.commit.assert_called_once()


def test_get_table_info_returns_rows(sqlite_connector):
    cursor = MagicMock()
    cursor.fetchall.return_value = [(0, "id", "INTEGER", 0, None, 1)]

    connection = MagicMock()
    connection.cursor.return_value = cursor

    sqlite_connector._connection = connection
    sqlite_connector._is_connected = True
    sqlite_connector.is_connected = MagicMock(return_value=True)

    result = sqlite_connector.get_table_info("sample")

    cursor.execute.assert_called_once_with("PRAGMA table_info(sample)")
    cursor.close.assert_called_once()
    assert result == [(0, "id", "INTEGER", 0, None, 1)]


def test_get_database_size_returns_zero_for_in_memory_database(sqlite_connector):
    assert sqlite_connector.get_database_size() == 0


def test_get_database_size_returns_file_size_for_file_database(tmp_path):
    db_path = tmp_path / "test.db"
    db_path.write_bytes(b"sqlite-data")

    connector = SqliteConnector(database=db_path)

    assert connector.get_database_size() == len(b"sqlite-data")


def test_get_connection_info_includes_sqlite_details_when_connected(tmp_path):
    db_path = tmp_path / "test.db"
    db_path.write_bytes(b"sqlite-data")

    connector = SqliteConnector(database=db_path)
    connector._is_connected = True

    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        ["3.45.0"],
        [10],
        [4096],
    ]

    connection = MagicMock()
    connection.cursor.return_value = cursor
    connector._connection = connection

    with patch(
        "volnux.backends.connectors.sqlite.BackendConnectorBase.get_connection_info",
        return_value={"connected": True, "database": str(db_path)},
    ):
        info = connector.get_connection_info()

    assert info["connected"] is True
    assert info["database_path"] == str(db_path)
    assert info["wal_enabled"] is True
    assert info["foreign_keys_enabled"] is True
    assert info["sqlite_version"] == "3.45.0"
    assert info["database_size_bytes"] == len(b"sqlite-data")
    assert info["page_count"] == 10
    assert info["page_size"] == 4096


def test_get_connection_info_returns_base_info_when_not_connected(sqlite_connector):
    sqlite_connector._is_connected = False

    with patch(
        "volnux.backends.connectors.sqlite.BackendConnectorBase.get_connection_info",
        return_value={"connected": False},
    ):
        info = sqlite_connector.get_connection_info()

    assert info["connected"] is False
    assert info["database_path"] == ":memory:"
    assert info["wal_enabled"] is True
    assert info["foreign_keys_enabled"] is True
    assert "sqlite_version" not in info


def test_repr_shows_connection_status_and_database_path(sqlite_connector):
    sqlite_connector._is_connected = False
    assert repr(sqlite_connector) == "<SqliteConnector :memory: [disconnected]>"

    sqlite_connector._is_connected = True
    assert repr(sqlite_connector) == "<SqliteConnector :memory: [connected]>"


def test_backup_creates_backup_file(tmp_path):
    source_path = tmp_path / "source.db"
    backup_path = tmp_path / "backup.db"

    connector = SqliteConnector(database=source_path)
    connector.connect()

    try:
        cursor = connector.get_cursor()
        cursor.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("INSERT INTO sample (name) VALUES (?)", ("alice",))
        connector.commit()
        cursor.close()

        connector.backup(backup_path)

        assert backup_path.exists()

        import sqlite3

        with sqlite3.connect(str(backup_path)) as backup_connection:
            backup_cursor = backup_connection.cursor()
            backup_cursor.execute("SELECT name FROM sample")
            rows = backup_cursor.fetchall()
            backup_cursor.close()

        assert rows == [("alice",)]
    finally:
        connector.disconnect()
