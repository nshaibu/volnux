import logging
import threading
import typing
import warnings


logger = logging.getLogger(__name__)


class ClientTaskRegistry:
    """
    Registry for managing client tasks.
    Keeps track of all client tasks submitted to the server, making it possible to route responses
    back to the correct client.
    """
    def __init__(self):
        self.tasks: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        self._lock = threading.RLock()
        self.ready = False

    def register(self, client_task: typing.Dict[str, typing.Any]):
        """Register a new client task."""
        with self._lock:
            if task := self.tasks.get(client_task["correlation_id"], None):
                # task already exists, updating task
                self.tasks[client_task["correlation_id"]] = task | client_task
                warnings.warn(f"Task with correlation_id {client_task['correlation_id']} already exists")
            else:
                self.tasks[client_task["correlation_id"]] = client_task

            if not self.ready:
                self.set_ready()

            logger.debug(f"Registered client task: {client_task['correlation_id']}")

    def get_task(self, client_task_id: str) -> typing.Union[typing.Dict[str, typing.Any], None]:
        """Get a registered client task by its ID."""
        return self.tasks.get(client_task_id)

    def remove(self, client_task_id: str) -> None:
        """Remove a task from the registry."""
        with self._lock:
            if client_task_id in self.tasks:
                del self.tasks[client_task_id]
                logger.debug(f"Removed client task: {client_task_id}")

    def cleanup_expired(self, ttl_seconds: int = 600) -> None:
        """Remove tasks that have been in the registry longer than TTL."""
        import datetime
        now = datetime.datetime.now()
        expired = []

        with self._lock:
             for task_id, task in self.tasks.items():
                created_at_str = task.get("created_at")
                if not created_at_str:
                    continue
                try:
                    # str(datetime.now()) format: YYYY-MM-DD HH:MM:SS.micros
                    created_at = datetime.datetime.fromisoformat(created_at_str)
                except ValueError:
                    # Fallback for space separator if fromisoformat fails (older python) or different format
                    try:
                        created_at = datetime.datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S.%f")
                    except ValueError:
                        continue

                if (now - created_at).total_seconds() > ttl_seconds:
                    expired.append(task_id)

             for task_id in expired:
                del self.tasks[task_id]

        if expired:
            logger.info(f"Cleaned up {len(expired)} expired tasks from registry")

    def set_ready(self) -> None:
        """Mark the registry as populated and ready."""
        self.ready = True
