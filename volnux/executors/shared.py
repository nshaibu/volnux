"""Shared, bounded worker pools.

Every flow used to build its own ``ProcessPoolExecutor`` / ``ThreadPoolExecutor``
for the tasks it ran. With cooperative ``{}`` groups, where every chain can start
flows of its own and groups nest, the number of pools (and so of processes and
threads) multiplied with the shape of the workflow: a fork bomb in the making, with
no limit and no fairness between flows.

The pools are now process-wide singletons with a fixed size, created on first use:

* ``SHARED_PROCESS_POOL_SIZE`` (default: the CPU count)
* ``SHARED_THREAD_POOL_SIZE`` (default: ``min(32, CPU count + 4)``)
* ``SHARED_EXECUTOR_POOLS`` (default ``True``): set it to ``False`` to give every
  flow a pool of its own again.

Work queues in the pool in submission order, so a busy pool delays tasks but never
starves one for ever, and the number of live workers stays fixed whatever the workflow
looks like. Only the standard-library pools are shared; Celery, remote and custom
executors are used as the workflow asks.

A flow that runs tasks which wait on each other (a stream segment) must not run them
on a pool with fewer workers than tasks, or a consumer could hold the only worker
while its producer waits in the queue: ``StreamFlow`` falls back to the async
executor in that case (see :func:`pool_size`).
"""

import atexit
import logging
import os
import threading
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Dict, Optional, Type

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_pools: Dict[str, Executor] = {}


def _config(name: str, default: Any) -> Any:
    try:
        from volnux.config import VolnuxConfig

        return VolnuxConfig.get_instance().resolve(name, default)
    except Exception:
        return default


def enabled() -> bool:
    return bool(_config("SHARED_EXECUTOR_POOLS", True))


def _kind(executor_class: Any) -> Optional[str]:
    if executor_class is ProcessPoolExecutor:
        return "process"
    if executor_class is ThreadPoolExecutor:
        return "thread"
    return None


def _size(name: str, default: int) -> int:
    value = _config(name, None)
    return max(1, int(value if value is not None else default))


def pool_size(executor_class: Type[Executor]) -> Optional[int]:
    """Workers of the shared pool that would serve ``executor_class`` (None if
    that class is not served by a shared pool)."""
    kind = _kind(executor_class)
    cpus = os.cpu_count() or 1
    if kind == "process":
        return _size("SHARED_PROCESS_POOL_SIZE", cpus)
    if kind == "thread":
        return _size("SHARED_THREAD_POOL_SIZE", min(32, cpus + 4))
    return None


def get_shared_pool(executor_class: Type[Executor]) -> Optional[Executor]:
    """The shared pool for ``executor_class``, created on first use; None when the
    class is not a standard pool or sharing is switched off."""
    kind = _kind(executor_class)
    if kind is None or not enabled():
        return None
    with _lock:
        pool = _pools.get(kind)
        if pool is None:
            size = pool_size(executor_class)
            pool = (
                ProcessPoolExecutor(max_workers=size)
                if kind == "process"
                else ThreadPoolExecutor(
                    max_workers=size, thread_name_prefix="volnux-shared"
                )
            )
            _pools[kind] = pool
            logger.info("Shared %s pool created with %d worker(s)", kind, size)
        return pool


def shutdown_shared_pools(wait: bool = True) -> None:
    """Shut the shared pools down (at exit, and between tests)."""
    with _lock:
        pools = list(_pools.values())
        _pools.clear()
    for pool in pools:
        try:
            pool.shutdown(wait=wait)
        except Exception:
            logger.debug("Error shutting down a shared pool", exc_info=True)


atexit.register(shutdown_shared_pools, False)
