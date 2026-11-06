import asyncio
import pytest
import time
from typing import Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from volnux.execution.resilience.health_check import (
    HealthMonitor,
    HealthMonitorConfig,
    ManagedComponent,
    HealthStatus,
)
from volnux.execution.resilience.protocols import TaskInfo


# Mock Manager Classes for Testing
class MockHealthyManager:
    """Mock manager that is always healthy."""

    def __init__(self):
        self._running = True
        self._worker_task = AsyncMock()
        self._worker_task.done.return_value = False
        self._restart_count = 0

    def is_healthy(self) -> bool:
        return True

    async def health_check(self) -> Dict:
        return {
            "healthy": True,
            "running": self._running,
            "tasks": {"worker": {"alive": True}},
        }

    def get_managed_tasks(self) -> Dict[str, TaskInfo]:
        return {
            "worker": {
                "alive": True,
                "task": self._worker_task,
                "restart_function": "_start_worker",
            }
        }

    async def restart_task(self, task_name: str) -> None:
        if task_name == "worker":
            self._restart_count += 1
        else:
            raise ValueError(f"Unknown task: {task_name}")


class MockUnhealthyManager:
    """Mock manager with a dead task."""

    def __init__(self):
        self._running = True
        self._worker_task = None
        self._restart_count = 0
        self._restart_should_fail = False

    def is_healthy(self) -> bool:
        return self._worker_task is not None

    async def health_check(self) -> Dict:
        return {
            "healthy": self.is_healthy(),
            "running": self._running,
            "tasks": {"worker": {"alive": self._worker_task is not None}},
        }

    def get_managed_tasks(self) -> Dict[str, TaskInfo]:
        return {
            "worker": {
                "alive": self._worker_task is not None,
                "task": self._worker_task,
                "restart_function": "_start_worker",
            }
        }

    async def restart_task(self, task_name: str) -> None:
        if self._restart_should_fail:
            raise RuntimeError("Restart failed")

        if task_name == "worker":
            self._restart_count += 1
            # Simulate successful restart
            self._worker_task = AsyncMock()
            self._worker_task.done.return_value = False
        else:
            raise ValueError(f"Unknown task: {task_name}")


class MockMultiTaskManager:
    """Mock manager with multiple tasks."""

    def __init__(self):
        self._running = True
        self._worker_task = AsyncMock()
        self._worker_task.done.return_value = False
        self._monitor_task = None  # Dead task
        self._processor_task = AsyncMock()
        self._processor_task.done.return_value = False
        self._restart_counts = {"worker": 0, "monitor": 0, "processor": 0}

    def is_healthy(self) -> bool:
        tasks = self.get_managed_tasks()
        return all(task["alive"] for task in tasks.values())

    async def health_check(self) -> Dict:
        tasks = self.get_managed_tasks()
        return {
            "healthy": self.is_healthy(),
            "running": self._running,
            "tasks": {name: {"alive": info["alive"]} for name, info in tasks.items()},
        }

    def get_managed_tasks(self) -> Dict[str, TaskInfo]:
        return {
            "worker": {
                "alive": self._worker_task is not None and not self._worker_task.done(),
                "task": self._worker_task,
                "restart_function": "_start_worker",
            },
            "monitor": {
                "alive": self._monitor_task is not None,
                "task": self._monitor_task,
                "restart_function": "_start_monitor",
            },
            "processor": {
                "alive": self._processor_task is not None
                and not self._processor_task.done(),
                "task": self._processor_task,
                "restart_function": "_start_processor",
            },
        }

    async def restart_task(self, task_name: str) -> None:
        if task_name in self._restart_counts:
            self._restart_counts[task_name] += 1

            if task_name == "monitor":
                self._monitor_task = AsyncMock()
                self._monitor_task.done.return_value = False
        else:
            raise ValueError(f"Unknown task: {task_name}")


class MockNonRestartableManager:
    """Mock manager that implements HealthCheckable but not Restartable."""

    def is_healthy(self) -> bool:
        return False

    async def health_check(self) -> Dict:
        return {"healthy": False, "running": True, "tasks": {}}


@pytest.fixture
def health_monitor_config():
    """Default health monitor configuration for tests."""
    return HealthMonitorConfig(
        health_check_interval=0.1,  # Fast for testing
        max_restart_attempts=3,
        restart_backoff=0.05,
        max_backoff=1.0,
        stability_window=5.0,
        enable_auto_restart=True,
    )


@pytest.fixture
def health_monitor(health_monitor_config):
    """Health monitor instance."""
    return HealthMonitor(config=health_monitor_config)


# Registration Tests


class TestRegistration:
    """Test manager registration and unregistration."""

    def test_register_manager(self, health_monitor):
        """Test registering a manager."""
        manager = MockHealthyManager()
        health_monitor.register(manager, "test_manager")

        assert "test_manager" in health_monitor._managers
        assert health_monitor._managers["test_manager"] is manager

    def test_register_duplicate_name_raises_error(self, health_monitor):
        """Test that registering duplicate name raises ValueError."""
        manager1 = MockHealthyManager()
        manager2 = MockHealthyManager()

        health_monitor.register(manager1, "test")

        with pytest.raises(ValueError, match="already registered"):
            health_monitor.register(manager2, "test")

    def test_register_non_healthcheckable_raises_error(self, health_monitor):
        """Test that registering non-HealthCheckable object raises TypeError."""

        class BadManager:
            pass

        with pytest.raises(TypeError, match="HealthCheckable protocol"):
            health_monitor.register(BadManager(), "bad")

    def test_unregister_manager(self, health_monitor):
        """Test unregistering a manager."""
        manager = MockHealthyManager()
        health_monitor.register(manager, "test")

        health_monitor.unregister("test")

        assert "test" not in health_monitor._managers
        assert "test" not in health_monitor._components

    def test_unregister_nonexistent_manager(self, health_monitor):
        """Test unregistering a non-existent manager doesn't raise error."""
        health_monitor.unregister("nonexistent")  # Should not raise


# Start/Stop Tests


class TestStartStop:
    """Test starting and stopping the health monitor."""

    @pytest.mark.asyncio
    async def test_start_monitor(self, health_monitor):
        """Test starting the health monitor."""
        await health_monitor.start()

        assert health_monitor._running is True
        assert health_monitor._monitor_task is not None
        assert not health_monitor._monitor_task.done()

        await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_stop_monitor(self, health_monitor):
        """Test stopping the health monitor."""
        await health_monitor.start()
        await health_monitor.stop()

        assert health_monitor._running is False
        assert health_monitor._monitor_task.done()

    @pytest.mark.asyncio
    async def test_start_already_running(self, health_monitor):
        """Test starting an already running monitor."""
        await health_monitor.start()

        # Start again
        await health_monitor.start()

        # Should still be running with same task
        assert health_monitor._running is True

        await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_stop_not_running(self, health_monitor):
        """Test stopping a monitor that's not running."""
        await health_monitor.stop()  # Should not raise


# Health Check Tests


class TestHealthChecking:
    """Test health checking functionality."""

    @pytest.mark.asyncio
    async def test_health_check_healthy_manager(self, health_monitor):
        """Test health check with healthy manager."""
        manager = MockHealthyManager()
        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.2)  # Wait for check
        await health_monitor.stop()

        assert health_monitor._total_health_checks > 0
        assert health_monitor._total_restarts == 0

    @pytest.mark.asyncio
    async def test_health_check_unhealthy_manager_restarts(self, health_monitor):
        """Test that unhealthy manager triggers restart."""
        manager = MockUnhealthyManager()
        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.3)  # Wait for check and restart
        await health_monitor.stop()

        assert manager._restart_count > 0
        assert health_monitor._total_restarts > 0

    @pytest.mark.asyncio
    async def test_multiple_managers_checked(self, health_monitor):
        """Test that multiple managers are checked."""
        manager1 = MockHealthyManager()
        manager2 = MockHealthyManager()

        health_monitor.register(manager1, "manager1")
        health_monitor.register(manager2, "manager2")

        await health_monitor.start()
        await asyncio.sleep(0.2)
        await health_monitor.stop()

        stats = health_monitor.get_stats()
        assert stats["registered_managers"] == 2

    @pytest.mark.asyncio
    async def test_health_check_exception_handled(self, health_monitor):
        """Test that exceptions in health check are handled."""
        manager = MockHealthyManager()
        manager.health_check = AsyncMock(
            side_effect=RuntimeError("Health check failed")
        )

        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.2)
        await health_monitor.stop()

        # Should continue running despite exception
        assert health_monitor._total_health_checks > 0


class TestRestart:
    """Test task restart functionality."""

    @pytest.mark.asyncio
    async def test_restart_single_task(self, health_monitor):
        """Test restarting a single dead task."""
        manager = MockUnhealthyManager()
        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.3)
        await health_monitor.stop()

        assert manager._restart_count >= 1
        assert manager._worker_task is not None  # Task was restored

    @pytest.mark.asyncio
    async def test_restart_multiple_tasks(self, health_monitor):
        """Test restarting multiple tasks in same manager."""
        manager = MockMultiTaskManager()
        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.3)
        await health_monitor.stop()

        # Monitor task should have been restarted
        assert manager._restart_counts["monitor"] >= 1
        # Other tasks should not have been restarted
        assert manager._restart_counts["worker"] >= 0
        assert manager._restart_counts["processor"] >= 0

    @pytest.mark.asyncio
    async def test_max_restart_attempts(self, health_monitor_config):
        """Test that restart stops after max attempts."""
        health_monitor_config.max_restart_attempts = 2
        monitor = HealthMonitor(config=health_monitor_config)

        manager = MockUnhealthyManager()
        manager._restart_should_fail = True  # Always fail restart
        monitor.register(manager, "test")

        await monitor.start()
        await asyncio.sleep(0.5)
        await monitor.stop()

        # Should have stopped after max attempts
        component = monitor._components["test"]["worker"]
        assert component.restart_count <= 2

    @pytest.mark.asyncio
    async def test_restart_with_backoff(self, health_monitor_config):
        """Test that restarts respect backoff timing."""
        health_monitor_config.restart_backoff = 0.1
        monitor = HealthMonitor(config=health_monitor_config)

        manager = MockUnhealthyManager()
        monitor.register(manager, "test")

        start_time = time.time()
        await monitor.start()
        await asyncio.sleep(0.5)
        await monitor.stop()
        elapsed = time.time() - start_time

        # Should have some delay due to backoff
        component = monitor._components["test"]["worker"]
        if component.restart_count > 1:
            assert elapsed > 0.2  # At least some backoff happened

    @pytest.mark.asyncio
    async def test_no_restart_when_disabled(self, health_monitor_config):
        """Test that restart doesn't happen when auto-restart is disabled."""
        health_monitor_config.enable_auto_restart = False
        monitor = HealthMonitor(config=health_monitor_config)

        manager = MockUnhealthyManager()
        monitor.register(manager, "test")

        await monitor.start()
        await asyncio.sleep(0.3)
        await monitor.stop()

        assert manager._restart_count == 0

    @pytest.mark.asyncio
    async def test_no_restart_non_restartable_manager(self, health_monitor):
        """Test that non-Restartable manager is not restarted."""
        manager = MockNonRestartableManager()
        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.3)
        await health_monitor.stop()

        # No restart should have been attempted
        assert health_monitor._total_restarts == 0

    @pytest.mark.asyncio
    async def test_restart_skipped_if_manager_stopped(self, health_monitor_config):
        """Test that restart is skipped if manager stops during backoff."""
        health_monitor_config.restart_backoff = 0.2
        monitor = HealthMonitor(config=health_monitor_config)

        manager = MockUnhealthyManager()
        monitor.register(manager, "test")

        await monitor.start()
        await asyncio.sleep(0.1)  # Let health check detect issue

        # Stop manager during backoff
        manager._running = False

        await asyncio.sleep(0.3)
        await monitor.stop()

        # Restart count should be low since manager was stopped
        assert manager._restart_count <= 1


# Backoff Calculation Tests


class TestBackoff:
    """Test backoff calculation logic."""

    def test_backoff_first_restart_no_delay(self, health_monitor):
        """Test that first restart has no delay."""
        component = ManagedComponent("test", "worker")
        backoff = health_monitor._calculate_backoff(component)
        assert backoff == 0

    def test_backoff_exponential_growth(self, health_monitor):
        """Test that backoff grows exponentially."""
        component = ManagedComponent("test", "worker")
        component.restart_count = 0
        component.last_restart = time.time()

        backoffs = []
        for i in range(4):
            component.restart_count = i
            backoff = health_monitor._calculate_backoff(component)
            backoffs.append(backoff)

        # Should grow: 0, 2, 4, 8
        assert backoffs[0] == 0
        assert backoffs[1] > backoffs[0]
        assert backoffs[2] > backoffs[1]
        assert backoffs[3] > backoffs[2]

    def test_backoff_capped_at_max(self, health_monitor_config):
        """Test that backoff is capped at max_backoff."""
        health_monitor_config.max_backoff = 5.0
        monitor = HealthMonitor(config=health_monitor_config)

        component = ManagedComponent("test", "worker")
        component.restart_count = 10  # Very high
        component.last_restart = time.time()

        backoff = monitor._calculate_backoff(component)
        assert backoff <= 5.0

    def test_backoff_reset_after_stability(self, health_monitor_config):
        """Test that backoff resets after stability window."""
        health_monitor_config.stability_window = 0.1
        monitor = HealthMonitor(config=health_monitor_config)

        component = ManagedComponent("test", "worker")
        component.restart_count = 3
        component.last_restart = time.time() - 1.0  # Long ago

        backoff = monitor._calculate_backoff(component)
        assert backoff == 0
        assert component.restart_count == 0


# Statistics Tests


class TestStatistics:
    """Test statistics and status reporting."""

    def test_get_stats_initial(self, health_monitor):
        """Test getting initial statistics."""
        stats = health_monitor.get_stats()

        assert stats["running"] is False
        assert stats["registered_managers"] == 0
        assert stats["total_health_checks"] == 0
        assert stats["total_restarts"] == 0

    @pytest.mark.asyncio
    async def test_get_stats_after_checks(self, health_monitor):
        """Test statistics after running checks."""
        manager = MockHealthyManager()
        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.3)
        await health_monitor.stop()

        stats = health_monitor.get_stats()
        assert stats["registered_managers"] == 1
        assert stats["total_health_checks"] > 0

    @pytest.mark.asyncio
    async def test_get_health_status(self, health_monitor):
        """Test getting comprehensive health status."""
        manager = MockHealthyManager()
        health_monitor.register(manager, "test")

        status = await health_monitor.get_health_status()

        assert "monitor_stats" in status
        assert "managers" in status
        assert "test" in status["managers"]
        assert status["managers"]["test"]["healthy"] is True

    @pytest.mark.asyncio
    async def test_get_health_status_with_error(self, health_monitor):
        """Test health status when manager raises exception."""
        manager = MockHealthyManager()
        manager.health_check = AsyncMock(side_effect=RuntimeError("Error"))
        health_monitor.register(manager, "test")

        status = await health_monitor.get_health_status()

        assert status["managers"]["test"]["healthy"] is False
        assert "error" in status["managers"]["test"]

    def test_reset_restart_counters_all(self, health_monitor):
        """Test resetting all restart counters."""
        # Create some components
        health_monitor._components["manager1"]["worker"] = ManagedComponent(
            "manager1", "worker"
        )
        health_monitor._components["manager1"]["worker"].restart_count = 5
        health_monitor._components["manager2"]["monitor"] = ManagedComponent(
            "manager2", "monitor"
        )
        health_monitor._components["manager2"]["monitor"].restart_count = 3

        health_monitor.reset_restart_counters()

        assert health_monitor._components["manager1"]["worker"].restart_count == 0
        assert health_monitor._components["manager2"]["monitor"].restart_count == 0

    def test_reset_restart_counters_specific_manager(self, health_monitor):
        """Test resetting counters for specific manager."""
        # Create components
        health_monitor._components["manager1"]["worker"] = ManagedComponent(
            "manager1", "worker"
        )
        health_monitor._components["manager1"]["worker"].restart_count = 5
        health_monitor._components["manager2"]["monitor"] = ManagedComponent(
            "manager2", "monitor"
        )
        health_monitor._components["manager2"]["monitor"].restart_count = 3

        health_monitor.reset_restart_counters("manager1")

        assert health_monitor._components["manager1"]["worker"].restart_count == 0
        assert health_monitor._components["manager2"]["monitor"].restart_count == 3


# Component Tests


class TestManagedComponent:
    """Test ManagedComponent class."""

    def test_record_restart(self):
        """Test recording a restart."""
        component = ManagedComponent("manager", "worker")

        component.record_restart()

        assert component.restart_count == 1
        assert component.consecutive_failures == 1
        assert component.last_restart is not None

    def test_record_success(self):
        """Test recording a success."""
        component = ManagedComponent("manager", "worker")
        component.consecutive_failures = 5

        component.record_success()

        assert component.consecutive_failures == 0

    def test_should_backoff_no_previous_restart(self):
        """Test backoff check with no previous restart."""
        component = ManagedComponent("manager", "worker")

        assert component.should_backoff() is False

    def test_should_backoff_recent_restart(self):
        """Test backoff check with recent restart."""
        component = ManagedComponent("manager", "worker")
        component.last_restart = time.time()

        assert component.should_backoff(backoff_threshold=1.0) is True

    def test_should_backoff_old_restart(self):
        """Test backoff check with old restart."""
        component = ManagedComponent("manager", "worker")
        component.last_restart = time.time() - 10.0

        assert component.should_backoff(backoff_threshold=1.0) is False


# Edge Cases and Error Handling


class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_restart_with_invalid_task_name(self, health_monitor):
        """Test that invalid task name is handled gracefully."""
        manager = MockHealthyManager()

        # Make manager report non-existent task as dead
        async def bad_health_check():
            return {
                "healthy": False,
                "running": True,
                "tasks": {"nonexistent": {"alive": False}},
            }

        manager.health_check = bad_health_check
        manager.get_managed_tasks = lambda: {
            "nonexistent": {
                "alive": False,
                "task": None,
                "restart_function": "_start_nonexistent",
            }
        }

        health_monitor.register(manager, "test")

        await health_monitor.start()
        await asyncio.sleep(0.3)
        await health_monitor.stop()

        # Should not crash, just log error
        assert health_monitor._running is False

    @pytest.mark.asyncio
    async def test_concurrent_registration_during_checks(self, health_monitor):
        """Test registering manager while checks are running."""
        manager1 = MockHealthyManager()
        health_monitor.register(manager1, "manager1")

        await health_monitor.start()
        await asyncio.sleep(0.1)

        # Register another while running
        manager2 = MockHealthyManager()
        health_monitor.register(manager2, "manager2")

        await asyncio.sleep(0.2)
        await health_monitor.stop()

        stats = health_monitor.get_stats()
        assert stats["registered_managers"] == 2


# Integration Tests


class TestIntegration:
    """Integration tests with realistic scenarios."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_recovery(self, health_monitor):
        """Test full lifecycle: unhealthy -> restart -> healthy."""
        manager = MockUnhealthyManager()
        health_monitor.register(manager, "test")

        # Start monitoring
        await health_monitor.start()

        # Wait for restart
        await asyncio.sleep(0.3)

        # Verify restart happened
        assert manager._restart_count >= 1
        assert manager.is_healthy()

        # Continue monitoring (should not restart again)
        restart_count = manager._restart_count
        await asyncio.sleep(0.3)

        # Should stabilize
        assert manager._restart_count == restart_count

        await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_multiple_managers_different_states(self, health_monitor):
        """Test monitoring multiple managers with different health states."""
        healthy = MockHealthyManager()
        unhealthy = MockUnhealthyManager()
        multi = MockMultiTaskManager()

        health_monitor.register(healthy, "healthy")
        health_monitor.register(unhealthy, "unhealthy")
        health_monitor.register(multi, "multi")

        await health_monitor.start()
        await asyncio.sleep(0.3)
        await health_monitor.stop()

        stats = health_monitor.get_stats()

        # Healthy manager should not have been restarted
        assert (
            "healthy" not in stats["managers"]
            or len(stats["managers"]["healthy"]["components"]) == 0
        )

        # Unhealthy manager should have been restarted
        assert "unhealthy" in stats["managers"]
        assert (
            stats["managers"]["unhealthy"]["components"]["worker"]["restart_count"] > 0
        )

        # Multi-task manager should have restarted only dead task
        assert stats["managers"]["multi"]["components"]["monitor"]["restart_count"] > 0
