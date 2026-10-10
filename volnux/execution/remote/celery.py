import logging
import typing
import asyncio
import redis
import orjson as json
from celery import Celery, Task

from .task import RemoteTaskPayload, RemoteTaskResult, execute_remote_task

logger = logging.getLogger(__name__)

# Redis key template for streamed batches.
# Batches are appended as JSON to a list; the requester LPOPs until it sees
# the sentinel {"type": "done"} entry or the key expires.
_BATCH_KEY_TEMPLATE = "volnux:batches:{state_id}"
_BATCH_TTL_SECONDS = 3600


def make_volnux_celery_task(
    celery_app: Celery,
    redis_client: redis.Redis,
    task_name: str = "volnux.execute_remote_event",
) -> typing.Callable:
    """
    Create a Celery task for Volnux event execution.

    Wires up a Redis-backed flush callback so that batch and aggregate mode
    contexts stream their results incrementally rather than returning one
    large payload. The requester polls the Redis list at
    ``volnux:batches:{state_id}`` to reconstruct the full result stream.

    :param celery_app: The Celery application to register the task on.
    :param redis_client: Redis client used for batch streaming. Must be
        accessible from all workers that will run this task.
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

        Streams result/error batches to Redis as they are produced, then
        pushes a ``{"type": "done"}`` sentinel so the requester knows the
        stream is complete.

        :param payload_dict: Serialised ``RemoteTaskPayload``.
        :returns: Serialised ``RemoteTaskResult`` (summary only — bulk data
            is in Redis).
        """
        payload = RemoteTaskPayload.from_dict(payload_dict)
        batch_key = _BATCH_KEY_TEMPLATE.format(
            state_id=payload.mini_context["state_id"]
        )

        def flush_callback(batch: dict) -> None:
            """Write one batch to the Redis stream list."""
            try:
                redis_client.rpush(batch_key, json.dumps(batch))
                redis_client.expire(batch_key, _BATCH_TTL_SECONDS)
            except redis.RedisError:
                logger.exception(
                    "Failed to write batch to Redis for state_id=%s type=%s count=%d",
                    payload.mini_context["state_id"],
                    batch.get("type"),
                    batch.get("count", 0),
                )
                raise  # propagate so MiniExecutionContext records the failure

        try:
            result = asyncio.run(
                execute_remote_task(payload, flush_callback=flush_callback)
            )
        except Exception:
            logger.exception(
                "execute_remote_task failed for task_id=%s state_id=%s",
                payload.task_id,
                payload.mini_context.get("state_id"),
            )
            raise

        finally:
            # Always push the sentinel so the requester is never left
            # blocking on a list that will never receive a "done" entry.
            try:
                redis_client.rpush(batch_key, json.dumps({"type": "done"}))
                redis_client.expire(batch_key, _BATCH_TTL_SECONDS)
            except redis.RedisError:
                logger.exception(
                    "Failed to push done sentinel for state_id=%s",
                    payload.mini_context.get("state_id"),
                )

        return result.to_dict()

    return execute_remote_event
