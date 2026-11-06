"""PostgresConnector pooling against a live server (skipped when none is reachable).

Configure with the same variables as the store tests:
VOLNUX_TEST_PG_HOST / _PORT / _DATABASE / _USERNAME / _PASSWORD.
"""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from psycopg_pool import PoolTimeout

from volnux.backends.connectors.postgres import PostgresConnector

PG = dict(
    host=os.environ.get("VOLNUX_TEST_PG_HOST", "localhost"),
    port=int(os.environ.get("VOLNUX_TEST_PG_PORT", "5432")),
    database=os.environ.get("VOLNUX_TEST_PG_DATABASE", "volnux_test"),
    username=os.environ.get("VOLNUX_TEST_PG_USERNAME", "volnux"),
    password=os.environ.get("VOLNUX_TEST_PG_PASSWORD", "volnux_test_password"),
)


def raw_connect(**extra):
    return psycopg.connect(
        host=PG["host"],
        port=PG["port"],
        dbname=PG["database"],
        user=PG["username"],
        password=PG["password"],
        connect_timeout=3,
        **extra,
    )


@pytest.fixture(scope="module")
def table():
    """A scratch table, dropped afterwards. Skips the module without a server."""
    name = f"pool_test_{uuid.uuid4().hex[:8]}"
    try:
        with raw_connect(autocommit=True) as conn:
            conn.execute(f'CREATE TABLE "{name}" (id TEXT PRIMARY KEY)')
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    yield name
    with raw_connect(autocommit=True) as conn:
        conn.execute(f'DROP TABLE IF EXISTS "{name}"')


@pytest.fixture(autouse=True)
def clean(table):
    with raw_connect(autocommit=True) as conn:
        conn.execute(f'DELETE FROM "{table}"')


def make(**kw):
    options = dict(PG, use_pooling=True, min_pool_size=1, max_pool_size=4, timeout=3)
    options.update(kw)
    connector = PostgresConnector(**options)
    connector.connect()
    return connector


@pytest.fixture
def connector():
    c = make()
    yield c
    c.disconnect()


def visible_rows(table):
    """Rows another connection can see (i.e. committed ones)."""
    with raw_connect() as conn:
        return sorted(r[0] for r in conn.execute(f'SELECT id FROM "{table}"'))


def insert(connector, table, key):
    cursor = connector.get_cursor()
    try:
        cursor.execute(f'INSERT INTO "{table}" VALUES (%s)', (key,))
    finally:
        cursor.close()


class TestConnecting:
    def test_pool_connects_with_default_keepalives(self):
        """Defaults used to reach libpq as the string 'True' and every connect failed."""
        connector = make()
        try:
            assert connector.is_connected()
        finally:
            connector.disconnect()

    def test_connection_options_reach_pooled_connections(self):
        connector = make(application_name="volnux-pool-test", row_factory="dict")
        try:
            cursor = connector.get_cursor()
            try:
                cursor.execute("SHOW application_name")
                assert cursor.fetchone() == {"application_name": "volnux-pool-test"}
            finally:
                cursor.close()
        finally:
            connector.disconnect()

    def test_autocommit_applies_to_pooled_connections(self, table):
        connector = make(autocommit=True)
        try:
            insert(connector, table, "a")
            assert visible_rows(table) == ["a"]  # no commit() was needed
        finally:
            connector.disconnect()


class TestCursorLifecycle:
    def test_get_cursor_returns_a_working_cursor(self, connector):
        """Used to raise: 'Cursor' object has no attribute '_pool_connection'."""
        cursor = connector.get_cursor()
        try:
            cursor.execute("SELECT 41 + 1")
            assert cursor.fetchone() == (42,)
        finally:
            cursor.close()

    def test_closing_a_cursor_returns_its_connection(self):
        """Connections used to be checked out forever; the pool ran dry."""
        connector = make(max_pool_size=2, timeout=2)
        try:
            for _ in range(20):
                with connector.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
            assert connector._connection_pool.get_stats()["pool_available"] >= 1
        finally:
            connector.disconnect()

    def test_unclosed_cursors_exhaust_a_small_pool_but_closing_one_frees_it(self):
        connector = make(min_pool_size=2, max_pool_size=2, timeout=1)
        try:
            held = [connector.get_cursor(), connector.get_cursor()]
            with pytest.raises((PoolTimeout, ConnectionError)):
                connector.get_cursor()
            held[0].close()
            connector.get_cursor().close()
            held[1].close()
        finally:
            connector.disconnect()

    def test_closing_twice_is_harmless(self, connector):
        cursor = connector.get_cursor()
        cursor.close()
        cursor.close()
        assert connector.is_connected()

    def test_reads_leave_no_open_transaction_on_returned_connections(self, connector):
        for _ in range(3):
            with connector.get_cursor() as cursor:
                cursor.execute("SELECT 1")  # opens an implicit transaction
        with connector.get_cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction' AND datname = current_database()"
            )
            assert cursor.fetchone()[0] <= 1  # only this cursor's own, if any


class TestTransactions:
    def test_commit_makes_writes_visible_and_rollback_discards_them(
        self, connector, table
    ):
        with connector.transaction():
            insert(connector, table, "kept")
            assert visible_rows(table) == []  # not committed yet
        assert visible_rows(table) == ["kept"]  # used to stay invisible forever

        with pytest.raises(RuntimeError):
            with connector.transaction():
                insert(connector, table, "dropped")
                raise RuntimeError("boom")
        assert visible_rows(table) == ["kept"]

    def test_a_transaction_stays_on_one_connection(self, connector):
        pids = []
        with connector.transaction():
            for _ in range(5):
                with connector.get_cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    pids.append(cursor.fetchone()[0])
            with connector._connection_context() as conn:
                pids.append(conn.execute("SELECT pg_backend_pid()").fetchone()[0])
        assert len(set(pids)) == 1

    def test_the_connection_goes_back_to_the_pool_afterwards(self):
        connector = make(min_pool_size=1, max_pool_size=1, timeout=2)
        try:
            for _ in range(5):  # a single-connection pool survives repeated use
                with connector.transaction():
                    with connector.get_cursor() as cursor:
                        cursor.execute("SELECT 1")
            assert connector._transaction_state() is None
        finally:
            connector.disconnect()

    def test_nested_blocks_do_not_commit_the_outer_work_early(self, connector, table):
        with pytest.raises(RuntimeError):
            with connector.transaction():
                insert(connector, table, "outer")
                with connector.transaction():
                    insert(connector, table, "inner")
                assert visible_rows(table) == []  # inner exit must not commit
                raise RuntimeError("outer fails")
        assert visible_rows(table) == []

    def test_a_failed_nested_block_forces_the_outer_to_roll_back(
        self, connector, table
    ):
        with connector.transaction():
            insert(connector, table, "outer")
            try:
                with connector.transaction():
                    raise ValueError("inner fails")
            except ValueError:
                pass  # the caller swallows it and carries on
        assert visible_rows(table) == []

    def test_threads_each_get_their_own_transaction(self, connector, table):
        barrier = threading.Barrier(2)
        pids, seen = {}, {}

        def worker(name):
            with connector.transaction():
                insert(connector, table, name)
                with connector.get_cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    pids[name] = cursor.fetchone()[0]
                barrier.wait(5)  # both transactions are open at the same time
                with connector.get_cursor() as cursor:
                    cursor.execute(f'SELECT id FROM "{table}"')
                    seen[name] = sorted(r[0] for r in cursor.fetchall())

        with ThreadPoolExecutor(2) as pool:
            list(pool.map(worker, ["t1", "t2"]))

        assert pids["t1"] != pids["t2"]
        assert seen == {"t1": ["t1"], "t2": ["t2"]}  # no dirty reads
        assert visible_rows(table) == ["t1", "t2"]

    def test_execute_batch_inside_a_transaction_defers_the_commit(
        self, connector, table
    ):
        with pytest.raises(RuntimeError):
            with connector.transaction():
                connector.execute_batch(
                    f'INSERT INTO "{table}" VALUES (%s)', [("a",), ("b",)]
                )
                raise RuntimeError("boom")
        assert visible_rows(table) == []

    def test_execute_query_commits_on_its_own(self, connector, table):
        connector.execute_query(
            f'INSERT INTO "{table}" VALUES (%s)', ("q",), fetch=False
        )
        assert visible_rows(table) == ["q"]


class TestConcurrency:
    def test_many_threads_share_a_small_pool_without_exhausting_it(self, table):
        connector = make(min_pool_size=1, max_pool_size=4, timeout=10)
        try:

            def work(i):
                for j in range(10):
                    with connector.transaction():
                        insert(connector, table, f"{i}-{j}")

            with ThreadPoolExecutor(16) as pool:
                list(pool.map(work, range(16)))

            assert len(visible_rows(table)) == 160
            assert connector._connection_pool.get_stats()["pool_available"] >= 1
        finally:
            connector.disconnect()


def kill_server_side_connections(exclude_own=True):
    """Terminate every other session, as a server restart or idle timeout would."""
    with raw_connect(autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )


class TestIsConnectedIsCheap:
    """is_connected() runs before every store operation: it must not hit the network."""

    def test_pooled_is_connected_makes_no_round_trip(self, connector):
        calls = []
        pool = connector._connection_pool
        real_getconn = pool.getconn
        pool.getconn = lambda *a, **k: (calls.append(1), real_getconn(*a, **k))[1]
        original_probe = connector._probe
        connector._probe = lambda: (calls.append("probe"), original_probe())[1]
        try:
            for _ in range(200):
                assert connector.is_connected() is True
        finally:
            pool.getconn = real_getconn
        assert calls == []

    def test_single_connection_probes_at_most_once_per_interval(self):
        connector = make(use_pooling=False, health_check_interval=60)
        probes = []
        original = connector._probe
        connector._probe = lambda: (probes.append(1), original())[1]
        try:
            for _ in range(200):
                assert connector.is_connected() is True
            assert probes == []  # connect() just verified; the result is trusted
        finally:
            connector.disconnect()

    def test_zero_interval_keeps_the_old_probe_per_call_behaviour(self):
        connector = make(use_pooling=False, health_check_interval=0)
        probes = []
        original = connector._probe
        connector._probe = lambda: (probes.append(1), original())[1]
        try:
            for _ in range(5):
                connector.is_connected()
            assert len(probes) == 5
        finally:
            connector.disconnect()

    def test_disconnect_makes_it_false_without_a_query(self, connector):
        connector.disconnect()
        assert connector.is_connected() is False
        assert connector.ping() is False


class TestRecoveryFromServerSideDisconnects:
    def test_pool_replaces_connections_the_server_killed(self, connector, table):
        with connector.transaction():
            insert(connector, table, "before")
        kill_server_side_connections()

        # checkout validation discards the dead connections: no failed operation
        for i in range(4):
            with connector.transaction():
                insert(connector, table, f"after-{i}")

        assert visible_rows(table) == [
            "after-0",
            "after-1",
            "after-2",
            "after-3",
            "before",
        ]

    def test_single_connection_reconnects_within_the_health_check_interval(self, table):
        import time

        connector = make(use_pooling=False, health_check_interval=0.3)
        try:
            insert(connector, table, "before")
            connector._connection.commit()
            kill_server_side_connections()
            time.sleep(0.5)  # longer than the interval, so the next check really probes

            connector.ensure_connected()  # notices the dead connection and reconnects
            insert(connector, table, "after")
            connector._connection.commit()

            assert visible_rows(table) == ["after", "before"]
        finally:
            connector.disconnect()

    def test_ping_reports_a_dead_connection_and_flips_the_state(self):
        connector = make(use_pooling=False, health_check_interval=60)
        try:
            assert connector.ping() is True
            kill_server_side_connections()
            assert connector.ping() is False
            assert connector.is_connected() is False  # so ensure_connected() reconnects
            connector.ensure_connected()
            assert connector.ping() is True
        finally:
            connector.disconnect()

    def test_connect_rebuilds_a_closed_pool(self, connector):
        connector._connection_pool.close()
        assert connector.is_connected() is False  # no query needed to know
        connector.ensure_connected()
        assert connector.ping() is True


class TestFailedStatementsDoNotPoisonTheConnection:
    """After an error PostgreSQL refuses every command until a rollback."""

    @pytest.mark.parametrize("pooling", [False, True])
    def test_a_failed_cursor_statement_does_not_break_the_next_one(self, pooling):
        connector = make(use_pooling=pooling, health_check_interval=60)
        try:
            with pytest.raises(psycopg.errors.UndefinedTable):
                with connector.get_cursor() as cursor:
                    cursor.execute("SELECT * FROM table_that_does_not_exist")

            with connector.get_cursor() as cursor:
                cursor.execute("SELECT 41 + 1")
                assert cursor.fetchone() == (42,)
        finally:
            connector.disconnect()

    @pytest.mark.parametrize("pooling", [False, True])
    def test_a_failed_execute_query_does_not_break_the_next_one(self, pooling):
        connector = make(use_pooling=pooling, health_check_interval=60)
        try:
            with pytest.raises(ConnectionError):
                connector.execute_query("SELECT * FROM table_that_does_not_exist")
            assert connector.execute_query("SELECT 7") == [(7,)]
        finally:
            connector.disconnect()

    def test_a_healthy_open_transaction_is_left_alone(self, table):
        connector = make(use_pooling=False, health_check_interval=60)
        try:
            with connector.transaction():
                insert(connector, table, "kept")
                with connector.get_cursor() as cursor:  # closing must not roll back
                    cursor.execute("SELECT 1")
            assert visible_rows(table) == ["kept"]
        finally:
            connector.disconnect()
