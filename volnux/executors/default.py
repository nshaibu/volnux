import asyncio
import inspect
import logging
from concurrent.futures import Executor, Future

logger = logging.getLogger(__name__)


class DefaultExecutor(Executor):
    """The default ("async") executor.

    Plain callables run inline, as before. It additionally advertises
    ``supports_coroutines``: ``submit_coroutine`` runs an ``async`` callable
    (an event) on the *running* event loop instead of calling it and losing the
    un-awaited coroutine, so async events never need a thread hop.
    """

    #: Flows check this to hand coroutine functions to ``submit_coroutine``.
    supports_coroutines = True

    def submit(self, fn, /, *args, **kwargs):
        f = Future()
        try:
            f.set_result(fn(*args, **kwargs))
        except Exception as e:
            logger.error(str(e))
            f.set_exception(e)
        return f

    def submit_coroutine(self, fn, /, *args, **kwargs) -> "asyncio.Future":
        """Run ``fn(*args, **kwargs)`` (awaiting the result if it is awaitable)
        on the running loop and return the ``asyncio`` task.

        Must be called from inside a running event loop.
        """
        loop = asyncio.get_running_loop()

        async def run():
            result = fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            return result

        return loop.create_task(run())
