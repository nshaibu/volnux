import asyncio
import threading

import pytest

from .conftest import Sender


@pytest.mark.asyncio
async def test_emit_async_awaits_coroutine_listener(signal):
    async def handler(**kw):
        await asyncio.sleep(0)
        return kw["value"]

    signal.connect(Sender, handler)
    assert (await signal.emit_async(Sender, value=4))[0][1] == 4


@pytest.mark.asyncio
async def test_emit_async_runs_sync_listener_off_the_loop_thread(signal):
    def handler(**kw):
        return threading.current_thread()

    signal.connect(Sender, handler)
    thread = (await signal.emit_async(Sender, value=1))[0][1]
    assert thread is not threading.current_thread()


@pytest.mark.asyncio
async def test_emit_async_mixed_listeners_keep_order(signal):
    async def a(**kw):
        return "a"

    def b(**kw):
        return "b"

    signal.connect(Sender, a)
    signal.connect(None, b)
    assert [r for _, r in await signal.emit_async(Sender, value=1)] == ["a", "b"]


@pytest.mark.asyncio
async def test_emit_async_collects_exceptions(signal):
    async def bad(**kw):
        raise ValueError("x")

    async def good(**kw):
        return 1

    signal.connect(Sender, bad)
    signal.connect(Sender, good)
    results = dict(await signal.emit_async(Sender, value=1))
    assert isinstance(results[bad], ValueError) and results[good] == 1


@pytest.mark.asyncio
async def test_emit_async_bad_arguments_do_not_raise(signal):
    async def handler(**kw):
        return 1

    signal.connect(Sender, handler)
    [(_, response)] = await signal.emit_async(Sender, nope=1)
    assert isinstance(response, TypeError)


def test_sync_emit_runs_coroutine_listener_without_running_loop(signal):
    async def handler(**kw):
        return kw["value"]

    signal.connect(Sender, handler)
    assert signal.emit(Sender, value=9)[0][1] == 9


@pytest.mark.asyncio
async def test_sync_emit_inside_running_loop_schedules_task(signal):
    done = asyncio.Event()

    async def handler(**kw):
        done.set()
        return "late"

    signal.connect(Sender, handler)
    [(_, task)] = signal.emit(Sender, value=1)  # must not raise
    assert isinstance(task, asyncio.Future)
    assert await asyncio.wait_for(task, 1) == "late"
    assert done.is_set()


def test_sync_listener_returning_awaitable_is_resolved(signal):
    async def inner():
        return "inner"

    def handler(**kw):
        return inner()

    signal.connect(Sender, handler)
    assert signal.emit(Sender, value=1)[0][1] == "inner"
