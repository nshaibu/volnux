import asyncio
import contextvars
import functools
import sys
import typing
import inspect


async def to_thread(
    func: typing.Callable[..., typing.Any],
    /,
    *args: typing.Any,
    **kwargs: typing.Any,
) -> typing.Any:
    """
    Run a synchronous function in a thread pool executor.

    Provides compatibility for Python 3.8 and earlier versions
    by emulating asyncio.to_thread() behavior.

    Args:
        func: The synchronous function to execute
        *args: Positional arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function

    Returns:
        The result of the function call

    Raises:
        RuntimeError: If called outside an async context
    """
    if sys.version_info >= (3, 9):
        return await asyncio.to_thread(func, *args, **kwargs)

    # Fallback for Python 3.8 and earlier
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as e:
        raise RuntimeError(
            "to_thread() must be called from a running event loop"
        ) from e

    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


async def as_coroutine(
    fn: typing.Callable[[...], typing.Any], /, *args, **kwargs
) -> typing.Any:
    """
    Normalise a synchronous or asynchronous callable into a coroutine.

    :param fn: The callable to be executed. It can be a synchronous or
        asynchronous function.
    :param args: Positional arguments to be passed to the callable.
    :param kwargs: Keyword arguments to be passed to the callable.
    :return: The result of the executed callable, delivered as a coroutine.
    :rtype: typing.Any
    """
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    return to_thread(fn, *args, **kwargs)
