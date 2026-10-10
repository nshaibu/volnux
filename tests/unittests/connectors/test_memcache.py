# from unittest.mock import MagicMock, patch
#
# import pytest
#
# from volnux.backends.connection import ConnectionError
# from volnux.backends.connectors.memcache import MemcacheConnector
#
#
# @pytest.fixture
# def memcache_connector():
#     return MemcacheConnector(
#         host="localhost",
#         port=11211,
#         timeout=3.0,
#         connect_timeout=2.0,
#         pool_size=10,
#         use_pooling=True,
#         key_prefix="app",
#         no_delay=True,
#     )
#
#
# def test_get_client_params_includes_expected_base_fields(memcache_connector):
#     params = memcache_connector._get_client_params()
#
#     assert params["server"] == ("localhost", 11211)
#     assert params["timeout"] == 3.0
#     assert params["connect_timeout"] == 2.0
#     assert params["no_delay"] is True
#     assert params["key_prefix"] == b"app"
#
#
# def test_get_client_params_includes_serializer_and_deserializer():
#     serializer = MagicMock()
#     deserializer = MagicMock()
#
#     connector = MemcacheConnector(
#         host="localhost",
#         serializer=serializer,
#         deserializer=deserializer,
#     )
#
#     params = connector._get_client_params()
#
#     assert params["serializer"] is serializer
#     assert params["deserializer"] is deserializer
#
#
# def test_get_client_params_excludes_connector_only_extra_params():
#     connector = MemcacheConnector(
#         host="localhost",
#         pool_size=20,
#         use_pooling=False,
#         enable_compression=True,
#         compression_threshold=2048,
#         key_prefix="prefix",
#         poolsize=99,
#         retry_attempts=3,
#         custom_option="value",
#     )
#
#     params = connector._get_client_params()
#
#     assert "pool_size" not in params
#     assert "use_pooling" not in params
#     assert "enable_compression" not in params
#     assert "compression_threshold" not in params
#     assert "poolsize" in params
#     assert params["poolsize"] == 99
#     assert params["retry_attempts"] == 3
#     assert params["custom_option"] == "value"
#
#
# def test_get_client_params_adds_compression_callbacks_when_enabled():
#     connector = MemcacheConnector(
#         host="localhost",
#         enable_compression=True,
#         compression_threshold=4,
#     )
#
#     params = connector._get_client_params()
#
#     assert "compressor" in params
#     assert "decompressor" in params
#     assert callable(params["compressor"])
#     assert callable(params["decompressor"])
#
#
# @patch("volnux.backends.connectors.memcache.PooledClient")
# def test_connect_success_with_pooling_uses_pooled_client(
#     mock_pooled_client, memcache_connector
# ):
#     client = MagicMock()
#     client.version.return_value = b"1.6.0"
#     mock_pooled_client.return_value = client
#
#     memcache_connector.connect()
#
#     mock_pooled_client.assert_called_once()
#     kwargs = mock_pooled_client.call_args.kwargs
#     assert kwargs["server"] == ("localhost", 11211)
#     assert kwargs["timeout"] == 3.0
#     assert kwargs["connect_timeout"] == 2.0
#     assert kwargs["no_delay"] is True
#     assert kwargs["key_prefix"] == b"app"
#     assert kwargs["max_pool_size"] == 10
#
#     client.version.assert_called_once()
#     assert memcache_connector._client is client
#     assert memcache_connector._cursor is client
#     assert memcache_connector._connection is client
#     assert memcache_connector._is_connected is True
#
#
# @patch("volnux.backends.connectors.memcache.PooledClient")
# def test_connect_uses_poolsize_override_from_extra_params(mock_pooled_client):
#     client = MagicMock()
#     client.version.return_value = b"1.6.0"
#     mock_pooled_client.return_value = client
#
#     connector = MemcacheConnector(
#         host="localhost",
#         pool_size=25,
#         use_pooling=True,
#     )
#
#     connector.connect()
#
#     kwargs = mock_pooled_client.call_args.kwargs
#     assert kwargs["max_pool_size"] == 25
#
#
# @patch("volnux.backends.connectors.memcache.Client")
# def test_connect_success_without_pooling_uses_single_client(mock_client):
#     client = MagicMock()
#     client.version.return_value = b"1.6.0"
#     mock_client.return_value = client
#
#     connector = MemcacheConnector(
#         host="localhost",
#         use_pooling=False,
#     )
#
#     connector.connect()
#
#     mock_client.assert_called_once()
#     kwargs = mock_client.call_args.kwargs
#     assert kwargs["server"] == ("localhost", 11211)
#     assert "max_pool_size" not in kwargs
#
#     client.version.assert_called_once()
#     assert connector._client is client
#     assert connector._cursor is client
#     assert connector._connection is client
#     assert connector._is_connected is True
#
#
# @patch("volnux.backends.connectors.memcache.PooledClient")
# def test_connect_returns_early_when_already_connected(
#     mock_pooled_client, memcache_connector
# ):
#     memcache_connector._is_connected = True
#
#     memcache_connector.connect()
#
#     mock_pooled_client.assert_not_called()
#
#
# @patch("volnux.backends.connectors.memcache.PooledClient")
# def test_connect_raises_connection_error_on_client_error(mock_pooled_client):
#     from pymemcache.exceptions import MemcacheClientError
#
#     client = MagicMock()
#     client.version.side_effect = MemcacheClientError("bad client")
#     mock_pooled_client.return_value = client
#
#     connector = MemcacheConnector(host="localhost")
#
#     with pytest.raises(ConnectionError, match="Client error"):
#         connector.connect()
#
#
# @patch("volnux.backends.connectors.memcache.PooledClient")
# def test_connect_raises_connection_error_on_server_error(mock_pooled_client):
#     from pymemcache.exceptions import MemcacheServerError
#
#     client = MagicMock()
#     client.version.side_effect = MemcacheServerError("server down")
#     mock_pooled_client.return_value = client
#
#     connector = MemcacheConnector(host="localhost")
#
#     with pytest.raises(ConnectionError, match="Server error"):
#         connector.connect()
#
#
# @patch("volnux.backends.connectors.memcache.PooledClient")
# def test_connect_raises_connection_error_on_unexpected_error(mock_pooled_client):
#     client = MagicMock()
#     client.version.side_effect = RuntimeError("boom")
#     mock_pooled_client.return_value = client
#
#     connector = MemcacheConnector(host="localhost")
#
#     with pytest.raises(ConnectionError, match="Connection failed"):
#         connector.connect()
#
#
# def test_disconnect_closes_client_and_resets_state(memcache_connector):
#     client = MagicMock()
#     memcache_connector._client = client
#     memcache_connector._cursor = client
#     memcache_connector._connection = client
#     memcache_connector._is_connected = True
#
#     memcache_connector.disconnect()
#
#     client.close.assert_called_once()
#     assert memcache_connector._client is None
#     assert memcache_connector._cursor is None
#     assert memcache_connector._connection is None
#     assert memcache_connector._is_connected is False
#
#
# def test_disconnect_is_noop_when_not_connected_and_no_client(memcache_connector):
#     memcache_connector._client = None
#     memcache_connector._is_connected = False
#
#     memcache_connector.disconnect()
#
#     assert memcache_connector._client is None
#     assert memcache_connector._cursor is None
#     assert memcache_connector._connection is None
#     assert memcache_connector._is_connected is False
#
#
# def test_disconnect_resets_state_even_if_close_fails(memcache_connector):
#     client = MagicMock()
#     client.close.side_effect = RuntimeError("close failed")
#     memcache_connector._client = client
#     memcache_connector._cursor = client
#     memcache_connector._connection = client
#     memcache_connector._is_connected = True
#
#     memcache_connector.disconnect()
#
#     assert memcache_connector._client is None
#     assert memcache_connector._cursor is None
#     assert memcache_connector._connection is None
#     assert memcache_connector._is_connected is False
#
#
# def test_is_connected_returns_false_when_not_connected(memcache_connector):
#     memcache_connector._is_connected = False
#     memcache_connector._client = None
#
#     assert memcache_connector.is_connected() is False
#
#
# def test_is_connected_returns_true_when_version_succeeds(memcache_connector):
#     client = MagicMock()
#     client.version.return_value = b"1.6.0"
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     assert memcache_connector.is_connected() is True
#     client.version.assert_called_once()
#
#
# def test_is_connected_returns_false_on_memcache_error(memcache_connector):
#     from pymemcache.exceptions import MemcacheError
#
#     client = MagicMock()
#     client.version.side_effect = MemcacheError("broken")
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     assert memcache_connector.is_connected() is False
#     assert memcache_connector._is_connected is False
#
#
# def test_is_connected_returns_false_on_oserror(memcache_connector):
#     client = MagicMock()
#     client.version.side_effect = OSError("socket closed")
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     assert memcache_connector.is_connected() is False
#     assert memcache_connector._is_connected is False
#
#
# def test_is_connected_returns_false_on_unexpected_error(memcache_connector):
#     client = MagicMock()
#     client.version.side_effect = RuntimeError("unexpected")
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     assert memcache_connector.is_connected() is False
#     assert memcache_connector._is_connected is False
#
#
# def test_ping_delegates_to_is_connected(memcache_connector):
#     memcache_connector.is_connected = MagicMock(return_value=True)
#
#     assert memcache_connector.ping() is True
#     memcache_connector.is_connected.assert_called_once()
#
#
# def test_get_cursor_returns_client_when_connected(memcache_connector):
#     client = MagicMock()
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     assert memcache_connector.get_cursor() is client
#
#
# def test_get_cursor_raises_when_not_connected(memcache_connector):
#     memcache_connector._client = None
#     memcache_connector._is_connected = False
#
#     with pytest.raises(ConnectionError, match="Not connected to Memcache"):
#         memcache_connector.get_cursor()
#
#
# def test_flush_all_calls_client(memcache_connector):
#     client = MagicMock()
#     client.flush_all.return_value = True
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.flush_all(delay=5)
#
#     client.flush_all.assert_called_once_with(delay=5)
#     assert result is True
#
#
# def test_get_stats_calls_client(memcache_connector):
#     client = MagicMock()
#     client.stats.return_value = {b"server": {b"curr_items": b"10"}}
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.get_stats()
#
#     client.stats.assert_called_once()
#     assert result == {b"server": {b"curr_items": b"10"}}
#
#
# def test_get_version_calls_client(memcache_connector):
#     client = MagicMock()
#     client.version.return_value = b"1.6.0"
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.get_version()
#
#     assert client.version.call_count >= 1
#     assert result == b"1.6.0"
#
#
# def test_touch_calls_client(memcache_connector):
#     client = MagicMock()
#     client.touch.return_value = True
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.touch("key", expire=60)
#
#     client.touch.assert_called_once_with("key", expire=60)
#     assert result is True
#
#
# def test_increment_calls_client(memcache_connector):
#     client = MagicMock()
#     client.incr.return_value = 3
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.increment("counter", value=2, noreply=False)
#
#     client.incr.assert_called_once_with("counter", 2, noreply=False)
#     assert result == 3
#
#
# def test_decrement_calls_client(memcache_connector):
#     client = MagicMock()
#     client.decr.return_value = 1
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.decrement("counter", value=2, noreply=False)
#
#     client.decr.assert_called_once_with("counter", 2, noreply=False)
#     assert result == 1
#
#
# def test_get_multi_calls_client(memcache_connector):
#     client = MagicMock()
#     client.get_many.return_value = {b"a": b"1", b"b": b"2"}
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.get_multi(["a", "b"])
#
#     client.get_many.assert_called_once_with(["a", "b"])
#     assert result == {b"a": b"1", b"b": b"2"}
#
#
# def test_set_multi_calls_client(memcache_connector):
#     client = MagicMock()
#     client.set_many.return_value = []
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     mapping = {"a": "1", "b": "2"}
#     result = memcache_connector.set_multi(mapping, expire=30, noreply=False)
#
#     client.set_many.assert_called_once_with(mapping, expire=30, noreply=False)
#     assert result == []
#
#
# def test_delete_multi_calls_client(memcache_connector):
#     client = MagicMock()
#     client.delete_many.return_value = True
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     result = memcache_connector.delete_multi(["a", "b"], noreply=False)
#
#     client.delete_many.assert_called_once_with(["a", "b"], noreply=False)
#     assert result is True
#
#
# def test_memcache_operations_raise_connection_error_on_memcache_error(
#     memcache_connector,
# ):
#     from pymemcache.exceptions import MemcacheError
#
#     client = MagicMock()
#     client.flush_all.side_effect = MemcacheError("flush failed")
#     memcache_connector._client = client
#     memcache_connector._is_connected = True
#
#     with pytest.raises(ConnectionError, match="Failed to flush"):
#         memcache_connector.flush_all()
#
#
# def test_get_connection_info_includes_memcache_details_when_connected(
#     memcache_connector,
# ):
#     memcache_connector._is_connected = True
#     memcache_connector.get_version = MagicMock(return_value=b"1.6.0")
#     memcache_connector.get_stats = MagicMock(
#         return_value={
#             b"server": {
#                 b"curr_items": b"12",
#                 b"curr_connections": b"5",
#                 b"uptime": b"3600",
#             }
#         }
#     )
#
#     with patch(
#         "volnux.backends.connectors.memcache.BackendConnectorBase.get_connection_info",
#         return_value={"connected": True, "host": "localhost"},
#     ):
#         info = memcache_connector.get_connection_info()
#
#     assert info["connected"] is True
#     assert info["host"] == "localhost"
#     assert info["pooling_enabled"] is True
#     assert info["compression_enabled"] is False
#     assert info["key_prefix"] == "app"
#     assert info["memcache_version"] == "1.6.0"
#     assert info["total_items"] == "12"
#     assert info["total_connections"] == "5"
#     assert info["uptime_seconds"] == "3600"
#
#
# def test_get_connection_info_returns_base_info_when_not_connected(memcache_connector):
#     memcache_connector._is_connected = False
#
#     with patch(
#         "volnux.backends.connectors.memcache.BackendConnectorBase.get_connection_info",
#         return_value={"connected": False},
#     ):
#         info = memcache_connector.get_connection_info()
#
#     assert info["connected"] is False
#     assert info["pooling_enabled"] is True
#     assert info["compression_enabled"] is False
#     assert info["key_prefix"] == "app"
#     assert "memcache_version" not in info
#
#
# def test_get_connection_info_tolerates_metadata_lookup_failure(memcache_connector):
#     memcache_connector._is_connected = True
#     memcache_connector.get_version = MagicMock(
#         side_effect=RuntimeError("version failed")
#     )
#
#     with patch(
#         "volnux.backends.connectors.memcache.BackendConnectorBase.get_connection_info",
#         return_value={"connected": True},
#     ):
#         info = memcache_connector.get_connection_info()
#
#     assert info["connected"] is True
#     assert info["pooling_enabled"] is True
#     assert info["compression_enabled"] is False
#     assert info["key_prefix"] == "app"
#
#
# def test_repr_shows_connection_status_and_pool_type(memcache_connector):
#     memcache_connector._is_connected = False
#     assert (
#         repr(memcache_connector)
#         == "<MemcacheConnector localhost:11211 [pooled, disconnected]>"
#     )
#
#     memcache_connector._is_connected = True
#     assert (
#         repr(memcache_connector)
#         == "<MemcacheConnector localhost:11211 [pooled, connected]>"
#     )
#
#
# def test_repr_shows_single_when_pooling_disabled():
#     connector = MemcacheConnector(
#         host="localhost",
#         use_pooling=False,
#     )
#
#     assert (
#         repr(connector) == "<MemcacheConnector localhost:11211 [single, disconnected]>"
#     )
