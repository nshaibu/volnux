import re
import logging
import threading
import weakref
from concurrent.futures import Executor, ThreadPoolExecutor, ProcessPoolExecutor
from typing import Dict, Type, Optional, Callable, Union, Any, cast

from volnux.executors import BaseExecutor, DefaultExecutor

logger = logging.getLogger(__name__)


class ManagedExecutorInstance:
    """
    Wrapper for managed executor instances with health monitoring and auto-recovery.

    Manages lifecycle of pre-initialized executor instances:
    - Health checking via is_alive property
    - Automatic re-initialization on failure
    - Reference counting for shared instances
    - Graceful shutdown

    :ivar instance: The managed executor instance.
    :type instance: Executor
    :ivar label: is The unique label associated with this managed instance.
    :type label: str
    :ivar reinit_callback: Optional callback for re-initializing the instance after failure.
    :type reinit_callback: Optional[Callable[[], Executor]]
    :ivar shared: Whether this instance is shared across the workflow.
    :type shared: bool
    :ivar auto_shutdown: Whether this instance should shut down automatically when no longer needed.
    :type auto_shutdown: bool
    :ivar health_check_enabled: Indicates if health monitoring is enabled for this instance.
    :type health_check_enabled: bool
    :ivar ref_count: The reference count indicating how many times this instance is acquired.
    :type ref_count: int
    :ivar failed: Indicates whether the executor instance has failed.
    :type failed: bool
    """

    def __init__(
        self,
        instance: Executor,
        label: str,
        *,
        reinit_callback: Optional[Callable[[], Executor]] = None,
        shared: bool = True,
        auto_shutdown: bool = True,
        health_check_enabled: bool = True,
    ):
        """
        Initialize a managed executor instance.

        Args:
            instance: The executor instance
            label: The label this instance is registered under
            reinit_callback: Callback to re-initialize the instance if it fails
            shared: If True, this instance is shared across the workflow
            auto_shutdown: If True, automatically shutdown on cleanup
            health_check_enabled: If True, monitor health via is_alive
        """
        self.instance = instance
        self.label = label
        self.reinit_callback = reinit_callback
        self.shared = shared
        self.auto_shutdown = auto_shutdown
        self.health_check_enabled = health_check_enabled

        self.ref_count = 0
        self.failed = False
        self._lock = threading.RLock()

    def is_alive(self) -> bool:
        """
        Check if the executor instance is alive.

        Returns:
            True if alive, False otherwise
        """
        if self.failed:
            return False

        # Check if an instance has is_alive property/method
        if hasattr(self.instance, "is_alive"):
            try:
                is_alive_attr = self.instance.is_alive
                # Could be property or method
                if callable(is_alive_attr):
                    return is_alive_attr()
                else:
                    return bool(is_alive_attr)
            except Exception as e:
                logger.warning(
                    f"Error checking is_alive for executor '{self.label}': {e}"
                )
                return False

        return True

    def acquire(self) -> Optional[Executor]:
        """
        Acquire a reference to this executor.

        Performs health check and re-initialization if needed.

        Returns:
            Executor instance or None if failed and cannot re-initialize
        """
        with self._lock:
            # Health check if enabled
            if self.health_check_enabled and not self.is_alive():
                logger.warning(
                    f"Executor '{self.label}' is not alive, attempting re-initialization"
                )

                if not self.reinitialize():
                    logger.error(f"Failed to re-initialize executor '{self.label}'")
                    return None

            self.ref_count += 1
            logger.debug(
                f"Acquired executor '{self.label}' "
                f"({self.instance.__class__.__name__}, refs: {self.ref_count})"
            )
            return self.instance

    def release(self) -> None:
        """Release a reference to this executor."""
        with self._lock:
            self.ref_count = max(0, self.ref_count - 1)
            logger.debug(
                f"Released executor '{self.label}' "
                f"({self.instance.__class__.__name__}, refs: {self.ref_count})"
            )

            # Auto-shutdown if no more references and not shared
            if self.ref_count == 0 and self.auto_shutdown and not self.shared:
                self.shutdown()

    def reinitialize(self) -> bool:
        """
        Re-initialize the executor instance using the callback.

        Returns:
            True if re-initialization succeeded, False otherwise
        """
        with self._lock:
            if not self.reinit_callback:
                logger.error(
                    f"No re-initialization callback for executor '{self.label}'"
                )
                self.failed = True
                return False

            try:
                # Shutdown old instance
                logger.info(f"Shutting down failed executor '{self.label}'")
                self.shutdown(wait=False)

                # Re-initialize via callback
                logger.info(f"Re-initializing executor '{self.label}'")
                new_instance = self.reinit_callback()

                if new_instance is None:
                    raise ValueError("Re-initialization callback returned None")

                self.instance = new_instance
                self.failed = False

                logger.info(f"Successfully re-initialized executor '{self.label}'")
                return True

            except Exception as e:
                logger.error(
                    f"Failed to re-initialize executor '{self.label}': {e}",
                    exc_info=True,
                )
                self.failed = True
                return False

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the executor instance.

        Args:
            wait: Whether to wait for pending tasks
        """
        try:
            if hasattr(self.instance, "shutdown"):
                self.instance.shutdown(wait=wait)
                logger.info(
                    f"Shut down executor '{self.label}' "
                    f"({self.instance.__class__.__name__})"
                )
        except Exception as e:
            logger.warning(f"Error shutting down executor '{self.label}': {e}")

    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of this executor."""
        return {
            "label": self.label,
            "class": self.instance.__class__.__name__,
            "alive": self.is_alive(),
            "failed": self.failed,
            "shared": self.shared,
            "ref_count": self.ref_count,
            "health_check_enabled": self.health_check_enabled,
            "has_reinit_callback": self.reinit_callback is not None,
        }

    def __del__(self):
        """Cleanup on garbage collection."""
        if self.auto_shutdown and self.ref_count == 0:
            self.shutdown(wait=False)


class ExecutorRegistry:
    """
    Registry for managing executors, supporting class registrations, instance
    registrations, factories for dynamic creation, and executor aliasing.

    The ExecutorRegistry provides a framework for managing executors used in
    parallel processing or workflow-driven environments. Executors can be
    registered as either classes or initialized instances, with options to
    monitor health, enable re-initialization upon failure, and manage automatic
    shutdown. Factories allow for dynamic executor creation based on patterns,
    while aliases simplify referencing of existing executors.

    :ivar parent: Parent registry to inherit from, typically for workflow-specific
        registries. None indicates a global registry.
    :type parent: Optional[ExecutorRegistry]

    :ivar _executors: Mapping of executor labels to their corresponding classes for
        on-demand instantiation.
    :type _executors: Dict[str, Type[BaseExecutor]]

    :ivar _instances: Pre-initialized and managed executor instances, supporting
        health monitoring and failure recovery.
    :type _instances: Dict[str, ManagedExecutorInstance]

    :ivar _factories: Mapping of patterns to factory functions for creating
        executors dynamically.
    :type _factories: Dict[str, Callable[..., Type[BaseExecutor]]]

    :ivar _aliases: Mapping of alias labels to their corresponding target executor
        labels for simplification purposes.
    :type _aliases: Dict[str, str]

    :ivar _lock: Re-entrant lock for thread-safe operations within the registry.
    :type _lock: threading.RLock

    :ivar _shutdown: Flag indicating whether the registry has been shut down,
        preventing further operations.
    :type _shutdown: bool
    """

    def __init__(self, parent: Optional["ExecutorRegistry"] = None):
        """
        Initialize the executor registry.

        Args:
            parent: Parent registry to inherit from (for workflow-specific registries)
        """
        # Class registrations (for on-demand instantiation)
        self._executors: Dict[str, Type["BaseExecutor"]] = {}

        # Instance registrations (pre-initialized, managed executors)
        self._instances: Dict[str, ManagedExecutorInstance] = {}

        # Factories for dynamic creation
        self._factories: Dict[str, Callable[..., Type["BaseExecutor"]]] = {}

        # Aliases
        self._aliases: Dict[str, str] = {}

        # Parent registry
        self._parent = parent

        # Lock for thread safety
        self._lock = threading.RLock()

        # Shutdown flag
        self._shutdown = False

        # Register built-ins only in a global registry
        if parent is None:
            self._register_builtins()

    def _register_builtins(self) -> None:
        """Register standard Python executors."""

        self.register("default", DefaultExecutor)
        self.register("thread", ThreadPoolExecutor)
        self.register("process", ProcessPoolExecutor)

        # Aliases
        self.alias("threads", "thread")
        self.alias("processes", "process")
        self.alias("multiprocess", "process")

        logger.debug("Registered built-in executors")

    def register(
        self,
        label: str,
        executor: Union[Type["BaseExecutor"], Executor],
        *,
        override: bool = False,
        reinit_callback: Optional[Callable[[], Executor]] = None,
        shared: bool = False,
        auto_shutdown: bool = True,
        health_check_enabled: bool = True,
    ) -> None:
        """
        Register an executor class or instance.

        Args:
            label: The label to use
            executor: Either an executor class or an instance
            override: Allow overriding existing registrations
            reinit_callback: Callback to re-initialize instance if it fails
            shared: If True, instance is shared (for instances only)
            auto_shutdown: Auto-shutdown when no longer referenced
            health_check_enabled: Enable health monitoring via is_alive

        Example - Register class:
            >>> registry.register("gpu", GPUExecutor)

        Example - Register instance with auto-recovery:
            >>> celery_app = Celery("app")
            >>> celery_executor = CeleryExecutor(celery_app)
            >>>
            >>> def reinit_celery():
            ...     return CeleryExecutor(celery_app)
            >>>
            >>> registry.register(
            ...     "celery",
            ...     celery_executor,
            ...     reinit_callback=reinit_celery,
            ...     shared=True,
            ...     health_check_enabled=True
            ... )
        """
        label = label.lower().strip()

        with self._lock:
            if not override and (label in self._executors or label in self._instances):
                raise ValueError(
                    f"Executor label '{label}' already registered. "
                    f"Use override=True to replace."
                )

            is_instance = isinstance(executor, Executor) and not isinstance(
                executor, type
            )

            if is_instance:
                wrapped = ManagedExecutorInstance(
                    executor,
                    label,
                    reinit_callback=reinit_callback,
                    shared=shared,
                    auto_shutdown=auto_shutdown,
                    health_check_enabled=health_check_enabled,
                )
                self._instances[label] = wrapped

                logger.info(
                    f"Registered executor instance: {label} -> "
                    f"{executor.__class__.__name__} "
                    f"(shared={shared}, health_check={health_check_enabled}, "
                    f"auto_reinit={reinit_callback is not None})"
                )
            else:

                executor_class = cast(Type["BaseExecutor"], executor)

                if not (
                    isinstance(executor_class, type)
                    and issubclass(executor_class, BaseExecutor)
                ):
                    raise TypeError(
                        f"Executor must be a class or instance, got {executor}"
                    )

                self._executors[label] = executor_class
                logger.debug(
                    f"Registered executor class: {label} -> {executor_class.__name__}"
                )

    def register_instance(
        self,
        label: str,
        instance: Executor,
        *,
        reinit_callback: Optional[Callable[[], Executor]] = None,
        shared: bool = True,
        auto_shutdown: bool = True,
        health_check_enabled: bool = True,
        override: bool = False,
    ) -> None:
        """
        Register a pre-initialized executor instance with health monitoring.

        Args:
            label: The label to use
            instance: The executor instance
            reinit_callback: Callback to re-initialize if instance fails
            shared: If True, instance is shared (singleton pattern)
            auto_shutdown: Auto-shutdown when workflow ends
            health_check_enabled: Enable health monitoring via is_alive
            override: Allow overriding existing registrations

        Example:
            >>> # Celery executor with auto-recovery
            >>> celery_app = Celery("app", broker="redis://localhost")
            >>> celery_exec = CeleryExecutor(celery_app, queue="default")
            >>>
            >>> def reinit_celery():
            ...     # Re-create executor if it fails
            ...     return CeleryExecutor(celery_app, queue="default")
            >>>
            >>> registry.register_instance(
            ...     "celery",
            ...     celery_exec,
            ...     reinit_callback=reinit_celery,
            ...     shared=True,
            ...     health_check_enabled=True
            ... )
        """
        self.register(
            label,
            instance,
            override=override,
            reinit_callback=reinit_callback,
            shared=shared,
            auto_shutdown=auto_shutdown,
            health_check_enabled=health_check_enabled,
        )

    def register_factory(
        self,
        pattern: str,
        factory: Callable[..., Type["BaseExecutor"]],
        *,
        override: bool = False,
    ) -> None:
        """Register a factory function for dynamic executor creation."""
        pattern = pattern.lower().strip()

        with self._lock:
            if not override and pattern in self._factories:
                raise ValueError(f"Factory for '{pattern}' already registered")

            self._factories[pattern] = factory
            logger.debug(f"Registered executor factory: {pattern}")

    def alias(self, alias: str, target: str) -> None:
        """Create an alias for an existing label."""
        alias = alias.lower().strip()
        target = target.lower().strip()

        with self._lock:
            self._aliases[alias] = target
            logger.debug(f"Created alias: {alias} -> {target}")

    def get(
        self, label: str, **factory_kwargs
    ) -> Optional[Union[Type["BaseExecutor"], Executor]]:
        """
        Resolve executor from the label.

        For instances: Returns the instance (with health check + auto-reinit)
        For classes: Returns the class

        Resolution order:
        1. Check for a registered instance (with health check)
        2. Check for registered class
        3. Check factories
        4. Check parent registry

        Args:
            label: The executor label
            factory_kwargs: Kwargs to pass to factory if label matches a factory

        Returns:
            Executor instance or class, or None if not found
        """
        label = label.lower().strip()

        with self._lock:
            # Resolve aliases
            label = self._aliases.get(label, label)

            if label in self._instances:
                managed = self._instances[label]
                instance = managed.acquire()
                if instance is None:
                    logger.error(
                        f"Failed to acquire executor instance '{label}' "
                        "(not alive and re-initialization failed)"
                    )
                return instance

            if label in self._executors:
                return self._executors[label]

            for pattern, factory in self._factories.items():
                if self._matches_pattern(label, pattern):
                    try:
                        params = self._extract_params(label, pattern)
                        params.update(factory_kwargs)
                        return factory(**params)
                    except Exception as e:
                        logger.warning(
                            f"Factory for pattern '{pattern}' failed: {e}",
                            exc_info=True,
                        )
                        continue

            if self._parent:
                return self._parent.get(label, **factory_kwargs)

            logger.warning(f"No executor found for label: {label}")
            return None

    def release(self, label: str) -> None:
        """
        Release a reference to a managed executor instance.

        Should be called when done using an executor instance
        obtained via get().

        Args:
            label: The executor label
        """
        label = label.lower().strip()
        label = self._aliases.get(label, label)

        with self._lock:
            if label in self._instances:
                self._instances[label].release()

    def health_check(self, label: Optional[str] = None) -> Dict[str, Any]:
        """
        Check health of registered executor instances.

        Args:
            label: Check specific executor, or None for all

        Returns:
            Health status dict
        """
        with self._lock:
            if label:
                label = label.lower().strip()
                label = self._aliases.get(label, label)

                if label in self._instances:
                    return self._instances[label].get_health_status()
                else:
                    return {"error": f"No instance registered for '{label}'"}

            # Check all instances
            return {
                label: managed.get_health_status()
                for label, managed in self._instances.items()
            }

    def _matches_pattern(self, label: str, pattern: str) -> bool:
        """Check if label matches factory pattern."""

        regex_pattern = (
            pattern.replace("*", ".*").replace("{", "(?P<").replace("}", ">[^-]+)")
        )
        return re.fullmatch(regex_pattern, label) is not None

    def _extract_params(self, label: str, pattern: str) -> Dict[str, str]:
        """Extract parameters from label based on pattern."""

        regex_pattern = (
            pattern.replace("*", "(?P<name>.*)")
            .replace("{", "(?P<")
            .replace("}", ">[^-]+)")
        )
        match = re.fullmatch(regex_pattern, label)
        return match.groupdict() if match else {}

    def list_executors(self, include_parent: bool = True) -> Dict[str, str]:
        """List all registered executors."""
        result = {}

        with self._lock:
            # Parent executors first
            if include_parent and self._parent:
                result.update(self._parent.list_executors(include_parent=True))

            # Classes
            for label, executor_cls in self._executors.items():
                result[label] = f"{executor_cls.__module__}.{executor_cls.__name__}"

            # Instances
            for label, managed in self._instances.items():
                alive_status = " [ALIVE]" if managed.is_alive() else " [DEAD]"
                shared_status = " (shared)" if managed.shared else ""
                result[label] = (
                    f"{managed.instance.__class__.__module__}."
                    f"{managed.instance.__class__.__name__} "
                    f"[instance]{alive_status}{shared_status}"
                )

            # Factories
            for pattern in self._factories.keys():
                result[pattern] = f"<factory: {pattern}>"

            # Aliases
            for alias, target in self._aliases.items():
                result[f"{alias} (alias)"] = f"-> {target}"

        return result

    def unregister(self, label: str) -> bool:
        """Unregister an executor."""
        label = label.lower().strip()

        with self._lock:
            removed = False

            if label in self._executors:
                del self._executors[label]
                removed = True

            if label in self._instances:
                # Shutdown instance
                self._instances[label].shutdown()
                del self._instances[label]
                removed = True

            if label in self._factories:
                del self._factories[label]
                removed = True

            if label in self._aliases:
                del self._aliases[label]
                removed = True

            return removed

    def shutdown_all(self, wait: bool = True) -> None:
        """Shutdown all managed executor instances."""
        with self._lock:
            self._shutdown = True

            for label, managed in list(self._instances.items()):
                logger.info(f"Shutting down executor instance: {label}")
                managed.shutdown(wait=wait)

            self._instances.clear()
            logger.info("All executor instances shut down")

    def __del__(self):
        """Cleanup on garbage collection."""
        if not self._shutdown:
            self.shutdown_all(wait=False)


# Global registry
_global_registry = ExecutorRegistry()
_global_registry.register("default", DefaultExecutor, override=True)
_global_registry.register("thread", ThreadPoolExecutor, override=True)
_global_registry.alias("threads", "thread")
logger.info("Registered built-in executors:")


def get_global_executor_registry() -> ExecutorRegistry:
    """Get the global executor registry."""
    return _global_registry
