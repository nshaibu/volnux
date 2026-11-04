"""Shared thread pool for blocking key-value backend calls.

Every sync backend call made from async code (the model mixin *and*
``ResultStream``) goes through ``run_in_kv_executor``. Sharing one pool means
there is a single concurrency limit in front of the backend's connection pool,
instead of two pools that can together exceed it. It is deliberately separate
from the messaging pool, which is occupied by long-running blocking calls
(BLPOP, SUBSCRIBE listeners) that would otherwise starve short reads.

While a ``KeyValueStoreIntegrationMixin.transaction()`` block is active, calls
made by the current task are routed to that transaction's single dedicated
thread so they run on the connection the transaction owns.
"""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from typing import Any, Callable, Optional

__all__ = [
    "KV_EXECUTOR",
    "TRANSACTION_EXECUTOR",
    "run_in_kv_executor",
    "shutdown_kv_executor",
]

DEFAULT_KV_EXECUTOR_WORKERS = 128


def _configured_workers() -> int:
    raw = os.environ.get("VOLNUX_KV_EXECUTOR_WORKERS")
    try:
        workers = int(raw) if raw else DEFAULT_KV_EXECUTOR_WORKERS
    except ValueError:
        workers = DEFAULT_KV_EXECUTOR_WORKERS
    return max(1, workers)


# Dedicated pool for blocking KV backend calls.
# NEVER use the default asyncio.to_thread pool for persistence operations.
KV_EXECUTOR = ThreadPoolExecutor(
    max_workers=_configured_workers(), thread_name_prefix="volnux-kv-backend"
)

# Set by ``transaction()`` for the duration of the block.
TRANSACTION_EXECUTOR: ContextVar[Optional[ThreadPoolExecutor]] = ContextVar(
    "volnux_kv_transaction_executor", default=None
)


async def run_in_kv_executor(
    func: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """Run a sync backend call off the event loop and await its result."""
    loop = asyncio.get_running_loop()
    executor = TRANSACTION_EXECUTOR.get() or KV_EXECUTOR
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


def shutdown_kv_executor(wait: bool = True) -> None:
    """Shut the shared pool down (application teardown / tests)."""
    KV_EXECUTOR.shutdown(wait=wait, cancel_futures=True)
