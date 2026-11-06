import logging
import typing
import asyncio
from celery import Celery, Task

from .stream import (
    RemoteBatch,
    batch_key_for,
    batch_route,
    batch_ttl_seconds,
    done_entry,
)
from .task import RemoteTaskPayload, RemoteTaskResult, execute_remote_task

logger = logging.getLogger(__name__)

# Batches are pushed as RemoteBatch messages onto a per-task queue of the messaging
# backend (see .stream); the requester reads until the "done" entry or the queue
# expires.


def make_volnux_celery_task(
    celery_app: Celery,
    task_name: str = "volnux.execute_remote_event",
) -> typing.Callable:
    """
    Create a Celery task for Volnux event execution.

    Wires up a flush callback that pushes batches to the messaging backend so
    that batch and aggregate mode contexts stream their results incrementally
    rather than returning one large payload. The requester reads the queue of
    the task (``stream.BatchStreamReader``) to reconstruct the full result stream.
    The backend is the ``messaging`` entry of ``DATABASES``; it must be shared by
    the workers and the requester.

    :param celery_app: The Celery application to register the task on.
    :param task_name: Celery task name. Override when registering multiple
        instances to avoid duplicate-name errors.
    """

    @celery_app.task(
        bind=True,
        name=task_name,
        acks_late=True,
        reject_on_worker_lost=True,
        # Prevent re-queuing non-idempotent tasks on a worker crash.
        # Set to a small number and make events idempotent if retries
        # are needed for transient infrastructure failures.
        max_retries=0,
    )
    def execute_remote_event(self: Task, payload_dict: dict) -> dict:
        """
        Execute a Volnux event on a Celery worker.

        Streams result/error batches to the messaging backend as they are
        produced, then pushes a ``done`` entry so the requester knows the
        stream is complete.

        :param payload_dict: Serialised ``RemoteTaskPayload``.
        :returns: Serialised ``RemoteTaskResult`` (summary only — bulk data
            is in the stream).
        """
        payload = RemoteTaskPayload.from_dict(payload_dict)
        state_id = payload.mini_context["state_id"]
        batch_key = batch_key_for(state_id, payload.task_id)
        route = batch_route(state_id, payload.task_id)

        async def flush_callback(batch: dict) -> None:
            """Push one batch onto the task's stream."""
            try:
                await RemoteBatch.push(
                    RemoteBatch.from_batch(batch),
                    ttl=batch_ttl_seconds(),
                    route_override=route,
                )
            except Exception:
                logger.exception(
                    "Failed to push batch to the stream for state_id=%s type=%s count=%d",
                    payload.mini_context["state_id"],
                    batch.get("type"),
                    batch.get("count", 0),
                )
                raise  # propagate so MiniExecutionContext records the failure

        async def push_done(status: typing.Optional[str]) -> None:
            # Always pushed, so the requester is never left waiting on a queue that
            # will never receive a "done" entry. A failure here is only logged.
            try:
                await RemoteBatch.push(
                    done_entry(status), ttl=batch_ttl_seconds(), route_override=route
                )
            except Exception:
                logger.exception("Failed to push done entry for state_id=%s", state_id)

        async def run():
            # (batches and the done entry go out on one loop: a backend's
            # connections belong to the loop they were made on)
            status: typing.Optional[str] = "FAILED"  # until the task has finished
            try:
                result = await execute_remote_task(
                    payload, flush_callback=flush_callback, batch_key=batch_key
                )
                status = result.context_delta.get("final_status")
                return result
            except Exception:
                logger.exception(
                    "execute_remote_task failed for task_id=%s state_id=%s",
                    payload.task_id,
                    state_id,
                )
                raise
            finally:
                await push_done(status)

        result = asyncio.run(run())

        return result.to_dict()

    return execute_remote_event
