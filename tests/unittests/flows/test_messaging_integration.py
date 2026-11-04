"""Process-pool tasks controlled over a real Redis backend.

A spawned worker is a different process, so the only thing it shares with the
engine is Redis. Skipped when no Redis is reachable:

    VOLNUX_TEST_REDIS_HOST (default 127.0.0.1)   VOLNUX_TEST_REDIS_PORT (default 6379)
"""

import asyncio
import os
import time
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

from volnux import Pipeline
from volnux.execution.context import ExecutionStatus
from volnux.flows.bridge.communications.tasks import TaskCommunicationBridge, TaskState
from volnux.flows.bridge.communications.tasks.channels.messaging import ChannelEnvelope

from tests.unittests.flows import process_events  # noqa: F401 - registers the events

HOST = os.environ.get("VOLNUX_TEST_REDIS_HOST", "127.0.0.1")
PORT = int(os.environ.get("VOLNUX_TEST_REDIS_PORT", "6379"))


def make_pipeline(pointy):
    return type(
        "Pipe" + uuid.uuid4().hex[:8],
        (Pipeline,),
        {"Meta": type("Meta", (), {"pointy": pointy})},
    )


@pytest.fixture(scope="module")
def redis_backend():
    from volnux.backends.stores.redis import RedisStoreBackend

    try:
        backend = RedisStoreBackend(host=HOST, port=PORT, socket_connect_timeout=1)
        backend.connector.get_cursor().ping()
    except Exception as exc:
        pytest.skip(f"Redis not reachable at {HOST}:{PORT}: {exc}")
    yield backend
    backend.close()


@pytest_asyncio.fixture
async def redis_env(redis_backend, tmp_path, monkeypatch):
    """Engine and spawned workers both use Redis; workers report to ``marker_dir``."""
    settings = tmp_path / "settings.py"
    settings.write_text(
        "KEY_VALUE_STORE_CONFIG = {\n"
        '    "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",\n'
        f'    "CONNECTOR_CONFIG": {{"host": "{HOST}", "port": {PORT}}},\n'
        "}\n"
    )
    markers = tmp_path / "markers"
    markers.mkdir()
    monkeypatch.setenv("VOLNUX_CONFIG", str(settings))
    monkeypatch.setenv("VOLNUX_TEST_MARKER_DIR", str(markers))

    async with ChannelEnvelope.change_backend(redis_backend):
        yield markers


def leftover_queues(backend):
    cursor = backend.connector.get_cursor()
    return [k for k in cursor.scan_iter("*task-channel*")]


async def wait_for_marker(markers: Path, name: str, timeout=20.0):
    deadline = time.monotonic() + timeout
    while not (markers / name).exists():
        assert time.monotonic() < deadline, f"worker never wrote {name}"
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_a_process_task_gets_a_messaging_channel_and_reports_status(
    redis_env, redis_backend, monkeypatch
):
    states = []
    original = TaskCommunicationBridge._process_message

    async def spy(self, message, full_id=None):
        await original(self, message, full_id)
        if (message.payload or {}).get("state"):
            states.append(message.payload["state"])

    monkeypatch.setattr(TaskCommunicationBridge, "_process_message", spy)

    pipeline = make_pipeline("ProcessTalker")()
    await pipeline.start()

    (result,) = pipeline.execution_context.get_latest_context().get_results()
    assert result.content == "channel=MessagingCommandChannel"
    assert TaskState.RUNNING.value in states
    assert TaskState.COMPLETED.value in states
    assert leftover_queues(redis_backend) == [], "channel queues were not cleaned up"


@pytest.mark.asyncio
async def test_stop_cancels_a_task_running_in_another_process(redis_env):
    pipeline = make_pipeline("ProcessSlow")()
    run = asyncio.ensure_future(pipeline.start())
    await wait_for_marker(redis_env, "slow-started")

    t0 = time.perf_counter()
    await pipeline.stop()
    await asyncio.wait_for(run, 20)

    assert time.perf_counter() - t0 < 10, "stop() should not wait out the 60 s task"
    assert not (redis_env / "slow-finished").exists()
    assert (
        pipeline.execution_context.get_latest_context().status
        == ExecutionStatus.CANCELLED
    )


@pytest.mark.asyncio
async def test_pause_and_resume_reach_a_task_in_another_process(redis_env):
    pipeline = make_pipeline("ProcessPausable")()
    run = asyncio.ensure_future(pipeline.start())
    await wait_for_marker(redis_env, "pausable-communicating")

    assert await pipeline.pause() == 1
    await asyncio.sleep(2.0)  # long after communicate() ended
    assert not (redis_env / "pausable-processed").exists(), "ran although paused"
    assert not run.done()

    await pipeline.resume()
    await asyncio.wait_for(run, 20)
    assert (redis_env / "pausable-processed").exists()


@pytest.mark.asyncio
async def test_parallel_process_tasks_share_one_inbox(redis_env, monkeypatch):
    pumps = []
    original = TaskCommunicationBridge.start_monitoring

    async def spy(self):
        await original(self)
        pumps.append(len(self._pumps))

    monkeypatch.setattr(TaskCommunicationBridge, "start_monitoring", spy)

    pipeline = make_pipeline("ProcessTalker||ProcessReportsChannel")()
    await pipeline.start()

    results = pipeline.execution_context.get_latest_context().get_results()
    assert sorted(r.content for r in results) == ["channel=MessagingCommandChannel"] * 2
    assert max(pumps) == 1  # one pump served both tasks


# --- Celery: the same messaging bridge, a task running in a worker -------------


@pytest.fixture(scope="module")
def celery_executor(redis_backend):
    """A Celery app with an in-process worker; its tasks resolve the Redis backend
    through the class (a worker thread does not see the test's context-local one)."""
    from celery import Celery
    from celery.contrib.testing.worker import start_worker

    from volnux.executors.celery import CeleryExecutor
    from volnux.executors.utils.registry import get_global_executor_registry

    app = Celery("volnux-channel-test", broker="memory://", backend="cache+memory://")
    app.conf.task_serializer = app.conf.result_serializer = "pickle"
    app.conf.accept_content = ["pickle"]
    executor = CeleryExecutor(app)

    previous = (ChannelEnvelope._backend_store, ChannelEnvelope._backend_initialized)
    ChannelEnvelope._backend_store, ChannelEnvelope._backend_initialized = (
        redis_backend,
        True,
    )
    get_global_executor_registry().register_instance(
        "celery-test",
        executor,
        shared=True,
        health_check_enabled=False,
        override=True,
    )
    with start_worker(app, pool="solo", loglevel="WARNING", perform_ping_check=False):
        yield executor
    ChannelEnvelope._backend_store, ChannelEnvelope._backend_initialized = previous
    executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_a_celery_task_gets_a_messaging_channel(
    celery_executor, redis_env, redis_backend
):
    pipeline = make_pipeline("CeleryTalker")()
    await asyncio.wait_for(pipeline.start(), 30)

    (result,) = pipeline.execution_context.get_latest_context().get_results()
    assert result.error is False
    assert result.content == "channel=MessagingCommandChannel"
    assert leftover_queues(redis_backend) == []


@pytest.mark.asyncio
async def test_stop_cancels_a_task_running_in_a_celery_worker(
    celery_executor, redis_env
):
    pipeline = make_pipeline("CelerySlow")()
    run = asyncio.ensure_future(pipeline.start())
    await wait_for_marker(redis_env, "celery-slow-started", timeout=30)

    t0 = time.perf_counter()
    await pipeline.stop()
    await asyncio.wait_for(run, 30)

    assert time.perf_counter() - t0 < 15, "stop() should not wait out the 60 s task"
    assert not (redis_env / "celery-slow-finished").exists()
    assert (
        pipeline.execution_context.get_latest_context().status
        == ExecutionStatus.CANCELLED
    )


# --- Kafka: the same channel over a different backend -----------------------------

KAFKA_HOST = os.environ.get("VOLNUX_TEST_KAFKA_HOST", "127.0.0.1")
KAFKA_PORT = int(os.environ.get("VOLNUX_TEST_KAFKA_PORT", "19092"))


@pytest_asyncio.fixture
async def kafka_env(tmp_path, monkeypatch):
    pytest.importorskip("confluent_kafka", reason="confluent-kafka is not installed")
    from volnux.backends.stores.kafka import KafkaStoreBackend

    try:
        backend = KafkaStoreBackend(
            host=KAFKA_HOST, port=KAFKA_PORT, request_timeout=3.0
        )
    except Exception as exc:
        pytest.skip(f"Kafka not reachable at {KAFKA_HOST}:{KAFKA_PORT}: {exc}")

    settings = tmp_path / "settings.py"
    settings.write_text(
        "KEY_VALUE_STORE_CONFIG = {\n"
        '    "ENGINE": "volnux.backends.stores.kafka.KafkaStoreBackend",\n'
        f'    "CONNECTOR_CONFIG": {{"host": "{KAFKA_HOST}", "port": {KAFKA_PORT}, '
        '"request_timeout": 5.0},\n'
        "}\n"
    )
    markers = tmp_path / "markers"
    markers.mkdir()
    monkeypatch.setenv("VOLNUX_CONFIG", str(settings))
    monkeypatch.setenv("VOLNUX_TEST_MARKER_DIR", str(markers))

    async with ChannelEnvelope.change_backend(backend):
        yield markers, backend
    backend.close()


@pytest.mark.asyncio
async def test_stop_cancels_a_process_task_over_kafka(kafka_env):
    markers, backend = kafka_env
    pipeline = make_pipeline("ProcessSlow")()
    run = asyncio.ensure_future(pipeline.start())
    await wait_for_marker(markers, "slow-started", timeout=40)

    await pipeline.stop()
    await asyncio.wait_for(run, 40)

    assert not (markers / "slow-finished").exists()
    assert (
        pipeline.execution_context.get_latest_context().status
        == ExecutionStatus.CANCELLED
    )
    # per-task queues are topics on Kafka: they must not pile up
    topics = backend.connector.admin.list_topics(timeout=5).topics
    assert [t for t in topics if "task-channel" in t] == []
