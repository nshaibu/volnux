"""Running a template over a stream of items: bounded, ordered, reusable.

Two pieces, both independent of any event:

* :func:`run_concurrently` applies an async ``call`` to an async iterator of items with
  at most ``workers`` calls running and at most ``workers + buffer`` items in flight
  (taken from the input but not yet handed on). The input is read only as fast as
  results are consumed, so memory is bounded however long the input is. Results come
  back in input order (``ordered``) or as they finish.
* :class:`TemplateCaller` turns a template (an event class, or a plain/async callable)
  into such a ``call``, on the executor the meta event asks for.
"""

import asyncio
import inspect
import itertools
import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Optional, Type

from volnux.exceptions import CONTROL_FLOW_EXCEPTIONS
from volnux.event.base import EventBase

logger = logging.getLogger(__name__)

__all__ = ["Outcome", "TemplateCaller", "TemplateFailure", "run_concurrently"]


class TemplateFailure(Exception):
    """A template event finished with an error result."""


@dataclass
class Outcome:
    """What applying the template to one input produced."""

    index: int
    item: Any
    ok: bool
    value: Any  # the result when ``ok``, else the exception

    @property
    def error_text(self) -> str:
        return f"{type(self.value).__name__}: {self.value}"


_END = object()
_FAILED = object()


async def run_concurrently(
    items: AsyncIterator[Any],
    call: Callable[[Any], Awaitable[Any]],
    *,
    workers: int,
    ordered: bool = True,
    buffer: Optional[int] = None,
) -> AsyncIterator[Outcome]:
    """Apply ``call`` to every item; yield an :class:`Outcome` for each.

    An exception of ``call`` is an unsuccessful outcome, except a control-flow
    exception (``SwitchTask``, ``SuspendTask`` ...), which is raised to the caller.
    """
    workers = max(1, workers)
    extra = workers if buffer is None else max(0, buffer)
    slots = asyncio.Semaphore(workers + extra)  # items in flight, until handed on
    gate = asyncio.Semaphore(workers)  # calls running
    done: "asyncio.Queue[Any]" = asyncio.Queue()
    tasks: set = set()

    async def work(index: int, item: Any) -> None:
        async with gate:
            try:
                outcome = Outcome(index, item, True, await call(item))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                outcome = Outcome(index, item, False, exc)
            except BaseException as exc:  # e.g. a template that exits
                outcome = Outcome(index, item, False, exc)
        await done.put(outcome)

    async def feed() -> None:
        count = 0
        try:
            async for item in items:
                await slots.acquire()
                task = asyncio.ensure_future(work(count, item))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
                count += 1
            await done.put((_END, count))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await done.put((_FAILED, exc))

    feeder = asyncio.ensure_future(feed())
    total: Optional[int] = None
    emitted = 0
    held: Dict[int, Outcome] = {}
    try:
        while total is None or emitted < total:
            got = await done.get()
            if isinstance(got, tuple):
                if got[0] is _FAILED:
                    raise got[1]
                total = got[1]
                continue
            if ordered:
                held[got.index] = got
                while emitted in held:
                    outcome = held.pop(emitted)
                    emitted += 1
                    slots.release()
                    _reraise_control_flow(outcome)
                    yield outcome
            else:
                emitted += 1
                slots.release()
                _reraise_control_flow(got)
                yield got
    finally:
        feeder.cancel()
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(feeder, *list(tasks), return_exceptions=True)


def _reraise_control_flow(outcome: Outcome) -> None:
    if not outcome.ok and isinstance(outcome.value, CONTROL_FLOW_EXCEPTIONS):
        raise outcome.value


class TemplateCaller:
    """Calls a template with one input, however the template is written.

    * An **event class** gets a fresh instance per call (an event runs once: a second
      call would return the first result), built with the meta event's execution
      context and its input as ``previous_result``. Its result's content is the value;
      an error result raises :class:`TemplateFailure`.
    * A **callable** is called with the input (``fn(item)``, or ``fn(*args)`` through
      :meth:`call_with`). Coroutine functions are awaited; plain functions run on a
      worker.

    ``mode`` is where the work runs: ``"async"`` on the event loop, ``"thread"`` /
    ``"process"`` on the shared pools (``volnux.executors.shared``), as in a flow. A
    process pool needs picklable templates and contexts.
    """

    def __init__(
        self,
        template: Any,
        *,
        owner: EventBase,
        mode: str = "thread",
        timeout: Optional[float] = None,
    ) -> None:
        self.template = template
        self.owner = owner
        self.mode = mode
        self.timeout = timeout
        self._ids = itertools.count()
        self.is_event = inspect.isclass(template) and issubclass(template, EventBase)

    async def call(self, item: Any) -> Any:
        """Apply the template to one input."""
        return await self._bounded(self._apply(item))

    async def call_with(self, *args: Any) -> Any:
        """Apply the template to several inputs at once (REDUCE: accumulator, item)."""
        return await self._bounded(self._apply(*args))

    async def _bounded(self, awaitable: Awaitable[Any]) -> Any:
        if not self.timeout:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout=self.timeout)

    async def _apply(self, *inputs: Any) -> Any:
        if self.is_event:
            return await self._run_event(inputs)
        return await self._run_callable(inputs)

    def _pool(self) -> Any:
        from volnux.executors import ProcessPoolExecutor, ThreadPoolExecutor
        from volnux.executors.shared import get_shared_pool

        if self.mode == "process":
            return get_shared_pool(ProcessPoolExecutor)
        if self.mode == "thread":
            return get_shared_pool(ThreadPoolExecutor)
        return None

    async def _run_callable(self, inputs: Any) -> Any:
        fn = self.template
        if inspect.iscoroutinefunction(fn):
            return await fn(*inputs)
        pool = self._pool()
        loop = asyncio.get_running_loop()
        if pool is not None:
            return await loop.run_in_executor(pool, fn, *inputs)
        if self.mode == "async":
            return fn(*inputs)
        return await asyncio.to_thread(fn, *inputs)

    async def _run_event(self, inputs: Any) -> Any:
        from volnux.result import EventResult

        owner = self.owner
        number = next(self._ids)
        task_id = f"{owner._task_id}:item:{number}"
        previous = [
            EventResult(
                error=False,
                event_name=owner.__class__.__name__,
                content=value,
                workflow_id=None,
                task_id=task_id,
            )
            for value in inputs
        ]
        instance = self.template(
            execution_context=owner._execution_context,
            task_id=task_id,
            previous_result=previous,
        )
        pool = self._pool()
        if pool is not None:
            from volnux.flows.base import BaseFlow

            result = await BaseFlow._dispatch_to_executor(pool, instance, {})
        else:
            result = await instance()
        if result is None:
            raise TemplateFailure(f"{self.template.__name__} returned no result")
        if result.error:
            raise TemplateFailure(result.content)
        return result.content
