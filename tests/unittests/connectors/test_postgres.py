import pytest
from unittest.mock import MagicMock

from volnux.backends.connectors.postgres import PostgresConnector


@pytest.fixture
def connector():
    return PostgresConnector(
        host="localhost",
        port=5432,
        database="test_db",
        username="test_user",
        password="test_password",
        use_pooling=False,
        row_factory="tuple",
        autocommit=False,
    )


def test_execute_query_fetches_results(connector):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("row1",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    connector._is_connected = True
    connector._connection = mock_conn
    connector._use_pooling = False

    result = connector.execute_query("SELECT 1", fetch=True)

    assert result == [("row1",)]
    mock_cursor.execute.assert_called_with("SELECT 1", None)


def test_execute_query_returns_none_when_fetch_false(connector):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    connector._is_connected = True
    connector._connection = mock_conn
    connector._use_pooling = False

    result = connector.execute_query("UPDATE table SET a = 1", fetch=False)

    assert result is None
    mock_cursor.execute.assert_called_with("UPDATE table SET a = 1", None)


def test_get_connection_info_includes_postgres_fields(connector, monkeypatch):
    mock_conn = MagicMock()
    mock_size_cursor = MagicMock()
    mock_count_cursor = MagicMock()

    mock_size_cursor.fetchone.return_value = (1024,)
    mock_count_cursor.fetchone.return_value = (8,)

    mock_conn.cursor.side_effect = [
        MagicMock(__enter__=MagicMock(return_value=mock_size_cursor)),
        MagicMock(__enter__=MagicMock(return_value=mock_count_cursor)),
    ]

    connector._is_connected = True
    connector._connection = mock_conn
    connector._use_pooling = False

    monkeypatch.setattr(connector, "get_server_version", lambda: (15, 2))

    info = connector.get_connection_info()

    assert info["pooling_enabled"] is False
    assert info["row_factory"] == "tuple"
    assert info["ssl_mode"] == "prefer"
    assert info["autocommit"] is False
    assert info["server_version"] == "15.2"
    # assert info["database_size_bytes"] == 1024
    assert info["active_connections"] == 8


def test_get_cursor_raises_when_not_connected(connector):
    connector._is_connected = False
    connector._connection = None
    connector._connection_pool = None

    with pytest.raises(ConnectionError):
        connector.get_cursor()
