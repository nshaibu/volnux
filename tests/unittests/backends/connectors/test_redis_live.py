"""RedisConnector against a live server (skipped when none is reachable).

Configure with VOLNUX_TEST_REDIS_HOST / VOLNUX_TEST_REDIS_PORT (default localhost:6379).
"""

import os

import pytest
import redis

from volnux.backends.connectors.redis import RedisConnector

HOST = os.environ.get("VOLNUX_TEST_REDIS_HOST", "localhost")
PORT = int(os.environ.get("VOLNUX_TEST_REDIS_PORT", "6379"))


@pytest.fixture
def connector():
    c = RedisConnector(host=HOST, port=PORT, socket_connect_timeout=1)
    try:
        c.connect()
    except ConnectionError as exc:
        pytest.skip(f"Redis not reachable at {HOST}:{PORT}: {exc}")
    yield c
    c.disconnect()


def ping_count():
    admin = redis.Redis(host=HOST, port=PORT)
    stats = admin.info("commandstats").get("cmdstat_ping", {})
    return int(stats.get("calls", 0))


def test_is_connected_sends_no_commands(connector):
    before = ping_count()
    for _ in range(100):
        assert connector.is_connected() is True
    assert ping_count() == before


def test_ping_sends_exactly_one_command(connector):
    before = ping_count()
    assert connector.ping() is True
    assert ping_count() == before + 1


def test_redis_py_reconnects_a_socket_the_server_dropped(connector):
    """The reason a per-operation PING is unnecessary."""
    client = connector.get_cursor()
    client.set("volnux-live-test", "v")
    redis.Redis(host=HOST, port=PORT).execute_command(
        "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"
    )
    assert client.get("volnux-live-test") in ("v", b"v")  # no error, no probe
    client.delete("volnux-live-test")
