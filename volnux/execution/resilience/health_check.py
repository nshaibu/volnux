"""
Generic Health Monitor for Self-Healing Managers

This module provides a generic health monitoring system that can monitor
and automatically restart tasks for any manager implementing the
HealthCheckable and Restartable protocols.
"""

import asyncio
import logging
import time
import typing
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, TypeVar, Generic

if typing.TYPE_CHECKING:
    from .protocols import HealthCheckable, Restartable

logger = logging.getLogger(__name__)

__all__ = ["HealthMonitor", "HealthStatus", "ManagedComponent", "HealthMonitorConfig"]


class HealthStatus(Enum):
    """Health status enumeration."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    STOPPED = "stopped"


@dataclass
class ManagedComponent:
    """
    Represents a component (task) being monitored.

    Tracks restart attempts and timing for a specific task
    within a managed object.
    """

    manager_name: str
    task_name: str
    restart_count: int = 0
    last_restart: Optional[float] = None
    consecutive_failures: int = 0

    def record_restart(self):
        """Record a restart attempt."""
        self.restart_count += 1
        self.consecutive_failures += 1
        self.last_restart = time.time()

    def record_success(self):
        """Record successful health check (reset consecutive failures)."""
        self.consecutive_failures = 0

    def should_backoff(self, backoff_threshold: float = 300.0) -> bool:
        """
        Check if enough time has passed since last restart.

        Args:
            backoff_threshold: Time in seconds to wait between restarts

        Returns:
            True if should wait before restart
        """
        if not self.last_restart:
            return False
        return (time.time() - self.last_restart) < backoff_threshold


@dataclass
class HealthMonitorConfig:
    """Configuration for the health monitor."""

    health_check_interval: float = 10.0
    max_restart_attempts: int = 3
    restart_backoff: float = 2.0
    max_backoff: float = 60.0
    stability_window: float = 300.0  # 5 minutes
    enable_auto_restart: bool = True


TManager = TypeVar("TManager", bound="HealthCheckable")


class HealthMonitor(Generic[TManager]):
    """
    Generic health monitor with automatic task restart.

    This monitor can manage multiple managers simultaneously,
    checking their health and restarting failed tasks.

    The monitor is agnostic to the number and names of tasks -
    it uses the Restartable protocol to discover and restart tasks.

    Example:
        ```python
        monitor = HealthMonitor[VolnuxCheckPointManager](
            config=HealthMonitorConfig(
                health_check_interval=10.0,
                max_restart_attempts=3
            )
        )

        # Register managers
        monitor.register(checkpoint_manager, "checkpoint")
        monitor.register(another_manager, "other")

        # Start monitoring
        await monitor.start()
        ```
    """

    def __init__(self, config: Optional[HealthMonitorConfig] = None):
        """
        Initialize the health monitor.

        Args:
            config: Configuration for the monitor
        """
        self.config = config or HealthMonitorConfig()

        # Registered managers
        self._managers: Dict[str, TManager] = {}

        # Component tracking (manager_name -> task_name -> ManagedComponent)
        self._components: Dict[str, Dict[str, ManagedComponent]] = defaultdict(dict)

        # Monitor state
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

        # Statistics
        self._total_health_checks = 0
        self._total_restarts = 0

    def register(self, manager: TManager, name: str) -> None:
        """
        Register a manager for monitoring.

        Args:
            manager: Manager to monitor (must implement HealthCheckable)
            name: Unique name for this manager

        Raises:
            ValueError: If name already registered
            TypeError: If manager doesn't implement HealthCheckable protocol
        """
        if name in self._managers:
            raise ValueError(f"Manager '{name}' already registered")

        # Validate manager implements HealthCheckable
        if not hasattr(manager, "is_healthy") or not hasattr(manager, "health_check"):
            raise TypeError(
                f"Manager must implement HealthCheckable protocol "
                f"(is_healthy, health_check)"
            )

        self._managers[name] = manager
        logger.info(f"Registered manager '{name}' for health monitoring")

    def unregister(self, name: str) -> None:
        """
        Unregister a manager from monitoring.

        Args:
            name: Name of manager to unregister
        """
        if name in self._managers:
            del self._managers[name]
            self._components.pop(name, None)
            logger.info(f"Unregistered manager '{name}'")

    async def start(self) -> None:
        """Start the health monitoring loop."""
        if self._running:
            logger.warning("HealthMonitor already running")
            return

        self._running = True
        self._monitor_task = asyncio.create_task(
            self._health_check_loop(), name="health-monitor"
        )
        logger.info(
            f"HealthMonitor started (interval={self.config.health_check_interval}s, "
            f"auto_restart={self.config.enable_auto_restart})"
        )

    async def stop(self) -> None:
        """Stop the health monitoring loop."""
        if not self._running:
            return

        self._running = False

        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        logger.info("HealthMonitor stopped")

    async def _health_check_loop(self) -> None:
        """Main health checking loop."""
        logger.info("Health check loop started")

        while self._running:
            try:
                await asyncio.sleep(self.config.health_check_interval)

                # Check all registered managers
                for manager_name, manager in self._managers.items():
                    await self._check_manager(manager_name, manager)

                self._total_health_checks += 1

            except asyncio.CancelledError:
                logger.info("Health check loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}", exc_info=True)
                # Continue checking despite errors

    async def _check_manager(self, name: str, manager: TManager) -> None:
        """
        Check the health of a specific manager and restart if needed.

        Args:
            name: Manager name
            manager: Manager instance
        """
        try:
            # Perform health check
            health = await manager.health_check()
            is_healthy = health.get("healthy", False)

            if is_healthy:
                # Manager is healthy - reset failure counters
                for component in self._components[name].values():
                    component.record_success()
                return

            # Manager is unhealthy - check if we can restart
            if not self.config.enable_auto_restart:
                logger.warning(
                    f"Manager '{name}' is unhealthy but auto-restart is disabled"
                )
                return

            # Check if manager supports restart (implements Restartable)
            if not self._is_restartable(manager):
                logger.error(
                    f"Manager '{name}' is unhealthy but doesn't support restart "
                    "(missing Restartable protocol)"
                )
                return

            # Attempt to restart failed tasks
            await self._restart_manager_tasks(name, manager)

        except Exception as e:
            logger.error(f"Error checking manager '{name}': {e}", exc_info=True)

    def _is_restartable(self, manager: typing.Any) -> bool:
        """Check if manager implements Restartable protocol."""
        required_attrs = ["_running", "get_managed_tasks", "restart_task"]
        return all(hasattr(manager, attr) for attr in required_attrs)

    async def _restart_manager_tasks(self, name: str, manager: "Restartable") -> None:
        """
        Restart failed tasks for a manager.

        Args:
            name: Manager name
            manager: Manager instance (must implement Restartable)
        """
        try:
            # Get all managed tasks from the manager
            managed_tasks = manager.get_managed_tasks()

            # Check each task and restart if needed
            for task_name, task_info in managed_tasks.items():
                if not task_info.get("alive", True):
                    await self._restart_task(name, manager, task_name)

        except Exception as e:
            logger.error(
                f"Error restarting tasks for manager '{name}': {e}", exc_info=True
            )

    async def _restart_task(
        self, manager_name: str, manager: "Restartable", task_name: str
    ) -> None:
        """
        Restart a specific task.

        Args:
            manager_name: Name of the manager
            manager: Manager instance
            task_name: Name of the task to restart
        """
        # Get or create component tracker
        component_key = f"{manager_name}:{task_name}"
        if task_name not in self._components[manager_name]:
            self._components[manager_name][task_name] = ManagedComponent(
                manager_name=manager_name, task_name=task_name
            )

        component = self._components[manager_name][task_name]

        if component.restart_count >= self.config.max_restart_attempts:
            logger.error(
                f"Task '{task_name}' of manager '{manager_name}' has exceeded "
                f"max restart attempts ({self.config.max_restart_attempts})"
            )
            return

        backoff = self._calculate_backoff(component)

        if backoff > 0:
            logger.warning(
                f"Restarting '{task_name}' of manager '{manager_name}' "
                f"in {backoff:.1f}s (attempt {component.restart_count + 1}/"
                f"{self.config.max_restart_attempts})"
            )
            await asyncio.sleep(backoff)

        if not manager._running:
            logger.info(
                f"Manager '{manager_name}' stopped during backoff, " "skipping restart"
            )
            return

        # Attempt restart via the manager's restart_task method
        try:
            logger.info(
                f"Restarting '{task_name}' of manager '{manager_name}' "
                f"(attempt {component.restart_count + 1})"
            )

            await manager.restart_task(task_name)

            component.record_restart()
            self._total_restarts += 1

            logger.info(
                f"Successfully restarted '{task_name}' of manager '{manager_name}'"
            )

        except ValueError as e:
            logger.error(
                f"Invalid task name '{task_name}' for manager '{manager_name}': {e}"
            )
        except Exception as e:
            logger.error(
                f"Failed to restart '{task_name}' of manager '{manager_name}': {e}",
                exc_info=True,
            )
            component.consecutive_failures += 1

    def _calculate_backoff(self, component: ManagedComponent) -> float:
        """
        Calculate backoff delay for restart.

        Args:
            component: Component to calculate backoff for

        Returns:
            Delay in seconds
        """

        if component.restart_count == 0:
            return 0

        if component.last_restart:
            time_since_last = time.time() - component.last_restart
            if time_since_last > self.config.stability_window:
                # Reset counter after stability period
                component.restart_count = 0
                component.consecutive_failures = 0
                return 0

        delay = self.config.restart_backoff * (2**component.restart_count)
        return min(delay, self.config.max_backoff)

    def reset_restart_counters(self, manager_name: Optional[str] = None) -> None:
        """
        Reset restart counters.

        Args:
            manager_name: Name of specific manager, or None to reset all
        """
        if manager_name:
            if manager_name in self._components:
                for component in self._components[manager_name].values():
                    component.restart_count = 0
                    component.consecutive_failures = 0
                    component.last_restart = None
                logger.info(f"Reset restart counters for manager '{manager_name}'")
        else:
            for components in self._components.values():
                for component in components.values():
                    component.restart_count = 0
                    component.consecutive_failures = 0
                    component.last_restart = None
            logger.info("Reset restart counters for all managers")

    def get_stats(self) -> Dict[str, typing.Any]:
        """Get health monitor statistics."""
        return {
            "running": self._running,
            "registered_managers": len(self._managers),
            "total_health_checks": self._total_health_checks,
            "total_restarts": self._total_restarts,
            "managers": {
                name: {
                    "components": {
                        task_name: {
                            "restart_count": comp.restart_count,
                            "consecutive_failures": comp.consecutive_failures,
                            "last_restart": comp.last_restart,
                        }
                        for task_name, comp in components.items()
                    }
                }
                for name, components in self._components.items()
            },
        }

    async def get_health_status(self) -> Dict[str, typing.Any]:
        """Get comprehensive health status of all managers."""
        statuses = {}

        for name, manager in self._managers.items():
            try:
                health = await manager.health_check()
                statuses[name] = health
            except Exception as e:
                statuses[name] = {"error": str(e), "healthy": False}

        return {"monitor_stats": self.get_stats(), "managers": statuses}
