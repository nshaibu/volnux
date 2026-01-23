# import pytest
# from unittest.mock import patch, MagicMock
# from volnux.backends.connectors.redis import RedisConnector
#
#
# @pytest.fixture
# def redis_connector():
#     return RedisConnector(host="localhost", port=6379, db=0)
#
#
# @patch("volnux.backends.connectors.redis.Redis")
# def test_connect(mock_redis, redis_connector):
#     mock_instance = MagicMock()
#     mock_redis.return_value = mock_instance
#
#     redis_connector._cursor = None
#
#     connection = redis_connector.connect()
#
#     mock_redis.assert_called_once_with(host="localhost", port=6379, db=0)
#     assert connection == mock_instance
#
#
# @patch("volnux.backends.connectors.redis.Redis")
# def test_disconnect(mock_redis, redis_connector):
#     mock_instance = MagicMock()
#     mock_instance.close = MagicMock()
#     redis_connector._cursor = mock_instance
#
#     redis_connector.disconnect()
#
#     mock_instance.close.assert_called_once()
#     assert redis_connector._cursor is None
#
#
# @patch("volnux.backends.connectors.redis.Redis")
# def test_is_connected(mock_redis, redis_connector):
#     mock_instance = MagicMock()
#     redis_connector._cursor = mock_instance
#
#     mock_instance.ping.return_value = True
#     assert redis_connector.is_connected() is True
#
#     redis_connector._cursor = None
#     assert redis_connector.is_connected() is False
