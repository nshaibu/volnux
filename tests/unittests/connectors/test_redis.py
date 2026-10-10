from unittest.mock import MagicMock, patch

import pytest
from redis import RedisError
from redis.exceptions import (
    AuthenticationError,
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from volnux.backends.connectors.redis import RedisConnector


@pytest.fixture
def mock_connection_pool():
    with patch("volnux.backends.connectors.redis.ConnectionPool") as mock_pool:
        pool_instance = MagicMock()
        mock_pool.return_value = pool_instance
        yield mock_pool, pool_instance


@pytest.fixture
def redis_connector(mock_connection_pool):
    _, pool_instance = mock_connection_pool
    connector = RedisConnector(
        host="localhost",
        port=6379,
        database=0,
        pool_size=10,
        decode_responses=True,
    )
    connector._connection_pool = pool_instance
    return connector


def test_initialize_pool_uses_expected_kwargs(mock_connection_pool):
    mock_pool, _ = mock_connection_pool

    RedisConnector(
        host="localhost",
        port=6379,
        database=2,
        username="user",
        password="secret",
        pool_size=20,
        decode_responses=False,
        socket_timeout=9,
        socket_connect_timeout=11,
        socket_keepalive=False,
        ssl_enabled=True,
        ssl_cert_reqs="none",
        retry_on_timeout=True,
    )

    mock_pool.assert_called_once()
    kwargs = mock_pool.call_args.kwargs

    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 6379
    assert kwargs["db"] == 2
    assert kwargs["username"] == "user"
    assert kwargs["password"] == "secret"
    assert kwargs["max_connections"] == 20
    assert kwargs["decode_responses"] is False
    assert kwargs["socket_timeout"] == 9
    assert kwargs["socket_connect_timeout"] == 11
    assert kwargs["socket_keepalive"] is False
    if "ssl" in kwargs:
        assert kwargs["ssl"] is True
        assert kwargs["ssl_cert_reqs"] == "none"
    assert kwargs["ssl_cert_reqs"] == "none"
    assert kwargs["retry_on_timeout"] is True
    assert "pool_size" not in kwargs
    assert "database" not in kwargs
    assert "timeout" not in kwargs


def test_initialize_pool_raises_connection_error_on_failure():
    with patch(
        "volnux.backends.connectors.redis.ConnectionPool",
        side_effect=Exception("pool init failed"),
    ):
        with pytest.raises(
            ConnectionError, match="Failed to initialize connection pool"
        ):
            RedisConnector(host="localhost", port=6379, database=0)


@patch("volnux.backends.connectors.redis.Redis")
def test_connect_success(mock_redis, redis_connector, mock_connection_pool):
    _, pool_instance = mock_connection_pool
    client = MagicMock()
    client.ping.return_value = True
    mock_redis.return_value = client

    redis_connector.connect()

    mock_redis.assert_called_once_with(connection_pool=pool_instance)
    client.ping.assert_called_once()
    assert redis_connector._client is client
    assert redis_connector._cursor is client
    assert redis_connector._connection is client
    assert redis_connector._is_connected is True


@patch("volnux.backends.connectors.redis.Redis")
def test_connect_returns_early_when_already_connected(mock_redis, redis_connector):
    redis_connector._is_connected = True

    redis_connector.connect()

    mock_redis.assert_not_called()


@patch("volnux.backends.connectors.redis.Redis")
def test_connect_raises_authentication_error_as_connection_error(
    mock_redis, redis_connector
):
    client = MagicMock()
    client.ping.side_effect = AuthenticationError("bad auth")
    mock_redis.return_value = client

    with pytest.raises(ConnectionError, match="Authentication failed"):
        redis_connector.connect()


@patch("volnux.backends.connectors.redis.Redis")
def test_connect_raises_redis_connection_error_as_connection_error(
    mock_redis, redis_connector
):
    client = MagicMock()
    client.ping.side_effect = RedisConnectionError("connection down")
    mock_redis.return_value = client

    with pytest.raises(ConnectionError, match="Connection failed"):
        redis_connector.connect()


@patch("volnux.backends.connectors.redis.Redis")
def test_connect_raises_timeout_error_as_connection_error(mock_redis, redis_connector):
    client = MagicMock()
    client.ping.side_effect = RedisTimeoutError("timed out")
    mock_redis.return_value = client

    with pytest.raises(ConnectionError, match="Connection timeout"):
        redis_connector.connect()


@patch("volnux.backends.connectors.redis.Redis")
def test_connect_raises_unexpected_error_as_connection_error(
    mock_redis, redis_connector
):
    client = MagicMock()
    client.ping.side_effect = RuntimeError("boom")
    mock_redis.return_value = client

    with pytest.raises(ConnectionError, match="Unexpected connection error"):
        redis_connector.connect()


def test_disconnect_closes_client_and_resets_state(redis_connector):
    client = MagicMock()
    redis_connector._client = client
    redis_connector._cursor = client
    redis_connector._connection = client
    redis_connector._is_connected = True

    redis_connector.disconnect()

    client.close.assert_called_once()
    assert redis_connector._client is None
    assert redis_connector._cursor is None
    assert redis_connector._connection is None
    assert redis_connector._is_connected is False


def test_disconnect_is_noop_when_not_connected_and_no_client(redis_connector):
    redis_connector._client = None
    redis_connector._is_connected = False

    redis_connector.disconnect()

    assert redis_connector._client is None
    assert redis_connector._is_connected is False


def test_disconnect_resets_state_even_if_close_fails(redis_connector):
    client = MagicMock()
    client.close.side_effect = RuntimeError("close failed")
    redis_connector._client = client
    redis_connector._cursor = client
    redis_connector._connection = client
    redis_connector._is_connected = True

    redis_connector.disconnect()

    assert redis_connector._client is None
    assert redis_connector._cursor is None
    assert redis_connector._connection is None
    assert redis_connector._is_connected is False


def test_is_connected_returns_false_when_not_marked_connected(redis_connector):
    redis_connector._is_connected = False
    redis_connector._client = None

    assert redis_connector.is_connected() is False


def test_is_connected_returns_true_when_ping_succeeds(redis_connector):
    client = MagicMock()
    client.ping.return_value = True
    redis_connector._client = client
    redis_connector._is_connected = True

    assert redis_connector.is_connected() is True
    client.ping.assert_called_once()


def test_is_connected_returns_false_on_redis_error(redis_connector):
    client = MagicMock()
    client.ping.side_effect = RedisError("redis problem")
    redis_connector._client = client
    redis_connector._is_connected = True

    assert redis_connector.is_connected() is False
    assert redis_connector._is_connected is False


def test_is_connected_returns_false_on_unexpected_error(redis_connector):
    client = MagicMock()
    client.ping.side_effect = RuntimeError("unexpected")
    redis_connector._client = client
    redis_connector._is_connected = True

    assert redis_connector.is_connected() is False
    assert redis_connector._is_connected is False


def test_ping_delegates_to_is_connected(redis_connector):
    redis_connector.is_connected = MagicMock(return_value=True)

    assert redis_connector.ping() is True
    redis_connector.is_connected.assert_called_once()


def test_get_cursor_returns_client_when_connected(redis_connector):
    client = MagicMock()
    redis_connector._client = client
    redis_connector._is_connected = True

    assert redis_connector.get_cursor() is client


def test_get_cursor_raises_when_not_connected(redis_connector):
    redis_connector._client = None
    redis_connector._is_connected = False

    with pytest.raises(ConnectionError, match="Not connected to Redis"):
        redis_connector.get_cursor()


def test_get_pipeline_returns_client_pipeline(redis_connector):
    client = MagicMock()
    pipeline = MagicMock()
    client.pipeline.return_value = pipeline
    redis_connector._client = client
    redis_connector._is_connected = True

    result = redis_connector.get_pipeline(transaction=False)

    client.pipeline.assert_called_once_with(transaction=False)
    assert result is pipeline


def test_get_info_returns_requested_section(redis_connector):
    client = MagicMock()
    client.info.return_value = {"redis_version": "7.0.0"}
    redis_connector._client = client
    redis_connector._is_connected = True

    result = redis_connector.get_info("server")

    client.info.assert_called_once_with(section="server")
    assert result == {"redis_version": "7.0.0"}


def test_flush_db_calls_client(redis_connector):
    client = MagicMock()
    client.flushdb.return_value = True
    redis_connector._client = client
    redis_connector._is_connected = True

    result = redis_connector.flush_db(asynchronous=True)

    client.flushdb.assert_called_once_with(asynchronous=True)
    assert result is True


def test_flush_all_calls_client(redis_connector):
    client = MagicMock()
    client.flushall.return_value = True
    redis_connector._client = client
    redis_connector._is_connected = True

    result = redis_connector.flush_all(asynchronous=False)

    client.flushall.assert_called_once_with(asynchronous=False)
    assert result is True


def test_get_connection_info_includes_redis_details_when_connected(redis_connector):
    base_info = {"connected": True, "host": "localhost"}
    redis_connector._is_connected = True
    redis_connector.get_info = MagicMock(
        side_effect=[
            {
                "redis_version": "7.0.0",
                "redis_mode": "standalone",
                "uptime_in_seconds": 1234,
            },
            {
                "connected_clients": 5,
            },
        ]
    )

    with patch(
        "volnux.backends.connectors.redis.BackendConnectorBase.get_connection_info",
        return_value=base_info.copy(),
    ):
        info = redis_connector.get_connection_info()

    assert info["connected"] is True
    assert info["host"] == "localhost"
    assert info["redis_version"] == "7.0.0"
    assert info["redis_mode"] == "standalone"
    assert info["uptime_in_seconds"] == 1234
    assert info["connected_clients"] == 5


def test_get_connection_info_returns_base_info_when_not_connected(redis_connector):
    redis_connector._is_connected = False

    with patch(
        "volnux.backends.connectors.redis.BackendConnectorBase.get_connection_info",
        return_value={"connected": False},
    ):
        info = redis_connector.get_connection_info()

    assert info == {"connected": False}


def test_get_connection_info_tolerates_info_lookup_failure(redis_connector):
    redis_connector._is_connected = True
    redis_connector.get_info = MagicMock(side_effect=RuntimeError("info failure"))

    with patch(
        "volnux.backends.connectors.redis.BackendConnectorBase.get_connection_info",
        return_value={"connected": True},
    ):
        info = redis_connector.get_connection_info()

    assert info == {"connected": True}


def test_repr_shows_connection_status(redis_connector):
    redis_connector._is_connected = False
    assert "disconnected" in repr(redis_connector)

    redis_connector._is_connected = True
    assert "connected" in repr(redis_connector)
    assert "localhost:6379" in repr(redis_connector)
