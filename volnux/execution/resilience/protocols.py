import typing
from typing import Protocol, runtime_checkable, Dict, List, Callable, Awaitable

if typing.TYPE_CHECKING:
    import asyncio

__all__ = ["HealthCheckable", "Restartable", "TaskInfo"]


@runtime_checkable
class HealthCheckable(Protocol):
    """
    Protocol for objects that support health checking.

    Any manager implementing this protocol can be monitored
    by the HealthMonitor for automatic recovery.
    """

    def is_healthy(self) -> bool:
        """
        Check if the manager is healthy.

        Returns:
            True if healthy, False otherwise
        """
        ...

    async def health_check(self) -> typing.Dict[str, typing.Any]:
        """
        Perform a comprehensive health check.

        Returns:
            Dictionary with health status and details.
            Must include at least:
            - "healthy": bool
            - "running": bool
            - "tasks": Dict[str, Dict] with task statuses
        """
        ...


class TaskInfo(typing.TypedDict, total=False):
    """Information about a managed task."""

    alive: bool
    task: typing.Optional["asyncio.Task"]
    restart_function: str  # Name of the restart method


@runtime_checkable
class Restartable(Protocol):
    """
    Protocol for objects that support task restart.

    Managers implementing this protocol can have their
    internal tasks restarted by the HealthMonitor.

    The protocol uses a registry-based approach where managers
    register their tasks and provide restart functions.
    """

    _running: bool

    def get_managed_tasks(self) -> Dict[str, TaskInfo]:
        """
        Get information about all managed tasks.

        Returns:
            Dictionary mapping task names to TaskInfo.
            Each TaskInfo should include:
            - alive: bool indicating if task is running
            - task: Optional reference to the asyncio.Task
            - restart_function: Name of the method to call for restart

        Example:
            {
                "worker": {
                    "alive": True,
                    "task": <Task>,
                    "restart_function": "_start_worker"
                },
                "monitor": {
                    "alive": False,
                    "task": None,
                    "restart_function": "_start_monitor"
                }
            }
        """
        ...

    async def restart_task(self, task_name: str) -> None:
        """
        Restart a specific task by name.

        Args:
            task_name: Name of the task to restart

        Raises:
            ValueError: If task_name is not recognized,
            RuntimeError: If restart fails
        """
        ...
