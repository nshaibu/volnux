"""
Workflow Configuration System

WorkflowConfig is the infrastructure and discovery layer for a workflow.
It owns its directory, discovers its components (events, pipeline, batch_pipeline),
manages executor registration, trigger setup, and workflow execution.

User business logic lives in the discovered modules — events.py, pipeline.py,
batch_pipeline.py — not in this class.

Structure:
workflows/
├── trading/
│   ├── __init__.py
│   ├── workflow.py      # WorkflowConfig subclass — infrastructure
│   ├── events.py         # EventBase subclasses — business logic
│   ├── pipeline.py      # Pipeline subclass — input schema
│   ├── batch_pipeline.py # BatchPipeline subclass — multi-instance
│   └── trading.ptl      # Pointy-Lang source — workflow structure
"""

import logging
import types
import typing
import inspect
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional, Callable, Awaitable, Dict, Type, Literal

from .registry import get_workflow_registry
from volnux.executors.utils.registry import get_global_executor_registry
from volnux.result import ResultSet as TriggerSet
from volnux.event import EventBase
from volnux.event.agent import AgentEventBase
from volnux.event.meta import ControlFlowEvent
from volnux.execution.pipeline import Pipeline, BatchPipeline
from volnux.config import VolnuxConfig
from volnux.import_utils import load_module_from_path, load_multiple_submodules

if typing.TYPE_CHECKING:
    from .registry import WorkflowRegistry
    from volnux.executors import BaseExecutor
    from .trigger.triggers import TriggerBase, TriggerActivation
    from .trigger.triggers.base import TriggerType
    from volnux.executors.utils.registry import ExecutorRegistry

logger = logging.getLogger(__name__)

_system_conf = VolnuxConfig.get_instance()


class WorkflowExecutionError(Exception):
    """Raised when workflow execution fails."""


class WorkflowNotFound(Exception):
    """Raised when a workflow is not found in the registry."""


class TriggerRegistry:
    """Manages triggers registered by a workflow configuration."""

    def __init__(self):
        self._triggers: TriggerSet["TriggerBase"] = TriggerSet[TriggerBase]()

    def register(
        self,
        trigger: "TriggerBase",
        trigger_activation_callback: Callable[["TriggerActivation"], Awaitable[None]],
    ) -> None:
        """
        Register a trigger with its activation callback.

        Args:
            trigger: The trigger instance to register.
            trigger_activation_callback: Async callback invoked when the trigger fires.

        Raises:
            ValueError: If a trigger of the same type is already registered.
        """
        from .trigger.triggers import TriggerLifecycle

        existing = self._triggers.filter(trigger_type=trigger.trigger_type).first()
        if existing:
            raise ValueError(
                f"Trigger type '{trigger.trigger_type.value}' is already registered"
            )

        trigger.set_activation_callback(trigger_activation_callback)
        trigger.state.lifecycle = TriggerLifecycle.INITIALIZED
        self._triggers.add(trigger)

        logger.info(
            "Registered trigger '%s' (type=%s) for workflow '%s'",
            trigger.trigger_id,
            trigger.trigger_type.value,
            trigger.workflow_name,
        )

    def unregister(self, trigger_id: str) -> None:
        """
        Remove a trigger by ID.

        Args:
            trigger_id: The trigger's unique identifier.

        Raises:
            ValueError: If no trigger with the given ID exists.
        """
        trigger = self.get_trigger(trigger_id)
        if trigger is None:
            raise ValueError(f"Trigger '{trigger_id}' not found")

        self._triggers.discard(trigger)
        logger.info("Unregistered trigger '%s'", trigger_id)

    def get_trigger(self, trigger_id: str) -> Optional["TriggerBase"]:
        """Return a trigger by ID, or None if not found."""
        try:
            return typing.cast("TriggerBase", self._triggers.get(id=trigger_id))
        except KeyError:
            return None


class WorkflowConfig(ABC):
    """
    Base class for workflow configuration.

    This class serves as a foundational component for managing and configuring workflows. It
    establishes the structure, default settings, and executor registration mechanisms required
    to implement a comprehensive workflow management system. Subclasses are expected to
    override certain attributes and implement the `ready` method to customize workflow behavior.

    :ivar name: Name of the workflow configuration. Must be overridden in subclass.
    :type name: str
    :ivar verbose_name: Human-readable name of the workflow configuration.
    :type verbose_name: Optional[str]
    :ivar version: Version of the workflow. Defaults to "1.0.0".
    :type version: str
    :ivar mode: Workflow mode defining its structural representation. Defaults to "CFG".
    :type mode: Literal[ "DAG", "CFG"]
    :ivar path: Path to the workflow’s configuration file or directory. Set automatically by registries.
    :type path: Optional[Path]
    :ivar default_timeout: Default timeout setting for tasks (in milliseconds). Defaults to 300000.
    :type default_timeout: int
    :ivar default_retries: Default number of retries for tasks. Defaults to 3.
    :type default_retries: int
    :ivar default_auto_cleanup: Flag to determine if auto-cleanup should be performed. Defaults to False.
    :type default_auto_cleanup: bool

    Example:
        class SimpleConfig(WorkflowConfig):
            name = 'simple'
            verbose_name = 'Simple Configuration'

            def ready(self):
                # Register registries (infrastructure)
                self.register_registry(...)

                # Set defaults
                self.default_timeout = 60000
    """

    # Subclass overrides
    name: str = None
    verbose_name: Optional[str] = None
    version: str = "1.0.0"
    mode: Literal["DAG", "CFG"] = "CFG"

    # Set by registry
    path: Optional[Path] = None

    # Defaults (overridable per workflow)
    default_timeout: int = 300_000  # milliseconds
    default_retries: int = 3
    default_auto_cleanup: bool = False

    # checkpointing
    checkpointing = True
    checkpoint_interval: int = 10000
    checkpoint_timeout: int = 300000

    def __init__(self, workflow_path: Optional[Path] = None):
        """Initialize the workflow configuration.

        Args:
            workflow_path: Filesystem path to the workflow directory.
        """
        if self.name is None:
            raise ValueError("WorkflowConfig.name must be set by the subclass")

        if self.verbose_name is None:
            self.verbose_name = self.name.replace("_", " ").title()

        self.path = workflow_path
        if self.path is None:
            raise ValueError("WorkflowConfig.path must be set by the registry")

        self.module: Optional[types.ModuleType] = None
        self.is_executable: bool = False

        # Cached discovered modules
        self._loaded_modules: Dict[str, types.ModuleType] = {}

        # Cached component classes
        self._pipeline_class: Optional[Type[Pipeline]] = None
        self._batch_pipeline_class: Optional[Type[BatchPipeline]] = None
        self._event_classes: Optional[List[Type["EventBase"]]] = None

        # Registries
        self._registry: Optional["WorkflowRegistry"] = None
        self._settings: VolnuxConfig = _system_conf
        self._executor_registry = get_global_executor_registry()
        self.triggers: TriggerRegistry = TriggerRegistry()

        # Let subclasses register infrastructure
        self.ready()

    @abstractmethod
    def ready(self) -> None:
        """
        Override to register infrastructure resources.

        Called once during initialization. Use this to:
        - Register event sources (EventHub, PyPI, GitHub)
        - Register triggers (schedule, event, webhook)
        - Register custom executors
        - Set workflow-level configuration
        - Initialize connections and load environment variables
        """
        ...

    def _ensure_modules_loaded(self) -> Dict[str, types.ModuleType]:
        """Lazily load and cache workflow submodules."""
        if self._loaded_modules:
            return self._loaded_modules

        try:
            module = self._load_workflow_module()
        except ImportError as e:
            raise RuntimeError(
                f"Failed to load workflow module from '{self.path}': {e}"
            ) from e

        self._loaded_modules = load_multiple_submodules(
            module, self.path, ["events", "pipeline", "batch_pipeline"]
        )
        return self._loaded_modules

    def _load_workflow_module(self) -> types.ModuleType:
        """Load the workflow package from its __init__.py."""
        if not self.path or not self.path.exists():
            raise ImportError(f"Workflow path does not exist: {self.path}")

        if self.module is None:
            init_file = self.path / "__init__.py"
            self.module = load_module_from_path(self.name, init_file)

        return self.module

    def get_event_module(self) -> Optional[types.ModuleType]:
        """Return the events module, or None if not found."""
        self._ensure_modules_loaded()
        return self._loaded_modules.get("events")

    def get_pipeline_module(self) -> Optional[types.ModuleType]:
        """Return the pipeline module, or None if not found."""
        self._ensure_modules_loaded()
        return self._loaded_modules.get("pipeline")

    def get_batch_pipeline_module(self) -> Optional[types.ModuleType]:
        """Return the batch pipeline module, or None if not found."""
        self._ensure_modules_loaded()
        return self._loaded_modules.get("batch_pipeline")

    def get_event_classes(self) -> List[Type["EventBase"]]:
        """
        Discover and return EventBase subclasses from the events module.

        Returns:
            List of EventBase subclasses. Empty list if no events module exists.
        """
        if self._event_classes is not None:
            return self._event_classes

        module = self.get_event_module()
        if module is None:
            self._event_classes = []
            return self._event_classes

        classes: List[Type["EventBase"]] = []
        for name in dir(module):
            obj = getattr(module, name)
            if (
                inspect.isclass(obj)
                and issubclass(obj, EventBase)
                and obj is not EventBase
                and obj is not AgentEventBase
                and obj is not ControlFlowEvent
            ):
                classes.append(obj)

        self._event_classes = classes
        logger.debug(
            "Discovered %d event class(es) in workflow '%s'",
            len(classes),
            self.name,
        )
        return self._event_classes  # type: ignore

    def get_pipeline_class(self) -> Type[Pipeline]:
        """
        Discover and return the Pipeline subclass.

        Returns:
            The Pipeline subclass is defined in pipeline.py.

        Raises:
            RuntimeError: If no pipeline module or Pipeline subclass is found.
        """
        if self._pipeline_class is not None:
            return self._pipeline_class

        module = self.get_pipeline_module()
        if module is None:
            raise RuntimeError(
                f"Workflow '{self.name}' has no pipeline module. "
                f"Create a pipeline.py with a Pipeline subclass."
            )

        for name in dir(module):
            obj = getattr(module, name)
            if (
                inspect.isclass(obj)
                and issubclass(obj, Pipeline)
                and obj is not Pipeline
            ):
                self._pipeline_class = obj
                return self._pipeline_class  # type: ignore

        raise RuntimeError(
            f"Workflow '{self.name}' has no Pipeline subclass in pipeline.py"
        )

    def get_batch_pipeline_class(self) -> Type[BatchPipeline]:
        """
        Discover and return the BatchPipeline subclass.

        Returns:
            The BatchPipeline subclass is defined in batch_pipeline.py.

        Raises:
            RuntimeError: If no batch pipeline module or BatchPipeline subclass
                is found.
        """
        if self._batch_pipeline_class is not None:
            return self._batch_pipeline_class

        module = self.get_batch_pipeline_module()
        if module is None:
            raise RuntimeError(
                f"Workflow '{self.name}' has no batch_pipeline module. "
                f"Create a batch_pipeline.py with a BatchPipeline subclass."
            )

        for name in dir(module):
            obj = getattr(module, name)
            if (
                inspect.isclass(obj)
                and issubclass(obj, BatchPipeline)
                and obj is not BatchPipeline
            ):
                self._batch_pipeline_class = obj
                return self._batch_pipeline_class  # type: ignore

        raise RuntimeError(
            f"Workflow '{self.name}' has no BatchPipeline subclass in batch_pipeline.py"
        )

    def get_pointy_ast(self):
        """Return the pointy AST for this workflow."""
        pipeline = self.get_pipeline_class()
        return pipeline.get_pointy_ast()

    # Executor management
    def get_executor_registry(self) -> "ExecutorRegistry":
        """Return the executor registry for this workflow."""
        return self._executor_registry

    def register_executor(
        self,
        label: str,
        executor_class: Type["BaseExecutor"],
        *,
        override: bool = False,
        reinit_callback: Optional[Callable[[], "BaseExecutor"]] = None,
        shared: bool = False,
        auto_shutdown: bool = True,
        health_check_enabled: bool = True,
    ) -> None:
        """
        Register a custom executor under a label.

        Once registered, events can use ``executor = "label"`` to target
        this executor.

        Args:
            label: Unique label for the executor (e.g., ``"gpu"``, ``"redis-queue"``).
            executor_class: Executor class inheriting from ``BaseExecutor``.
            override: If True, replace an existing registration with the same label.
            reinit_callback: Optional callable to reinitialize the executor.
            shared: If True, the same executor instance may be reused across tasks.
            auto_shutdown: If True, shut down the executor when the workflow completes.
            health_check_enabled: If True, enable periodic health checks.
        """
        self._executor_registry.register(
            label,
            executor_class,
            override=override,
            reinit_callback=reinit_callback,
            shared=shared,
            auto_shutdown=auto_shutdown,
            health_check_enabled=health_check_enabled,
        )
        logger.info(
            "Registered executor '%s' for workflow '%s': %s.%s",
            label,
            self.name,
            executor_class.__module__,
            executor_class.__name__,
        )

    def register_executor_factory(
        self,
        pattern: str,
        factory: Callable[..., Type["BaseExecutor"]],
        *,
        override: bool = False,
    ) -> None:
        """
        Register a factory for dynamic executor creation from label patterns.

        Args:
            pattern: Pattern with placeholders (e.g., ``"redis-{queue}"``).
            factory: Callable that receives extracted parameters and returns
                an executor class.
            override: If True, replace an existing factory for this pattern.

        Example:
            >>> def ready(self):
            ...     self.register_executor_factory(
            ...         "redis-{queue}",
            ...         lambda queue: create_redis_executor(queue),
            ...     )
            # Events can now use executor="redis-orders" or executor="redis-payments"
        """
        self._executor_registry.register_factory(pattern, factory, override=override)
        logger.info(
            "Registered executor factory pattern '%s' for workflow '%s'",
            pattern,
            self.name,
        )

    def register_executor_alias(self, alias: str, target: str) -> None:
        """
        Create an alias pointing to an existing executor label.

        Args:
            alias: The new alias name.
            target: The existing executor label to point to.
        """
        self._executor_registry.alias(alias, target)
        logger.debug(
            "Created executor alias '%s' -> '%s' for workflow '%s'",
            alias,
            target,
            self.name,
        )

    def get_executor(self, label: str, **kwargs) -> Optional[Type["BaseExecutor"]]:
        """Resolve an executor class from a label."""
        return self._executor_registry.get(label, **kwargs)

    def list_executors(self) -> Dict[str, str]:
        """Return a mapping of executor labels to class names."""
        return self._executor_registry.list_executors()

    # Trigger management
    def register_trigger(self, trigger: "TriggerBase") -> None:
        """
        Register a trigger for this workflow.

        The trigger is added to the trigger engine, which manages its
        lifecycle (start, stop, pause, resume).

        Args:
            trigger: The trigger instance to register.
        """
        from volnux.engine.workflows.trigger import get_trigger_engine

        engine = get_trigger_engine()
        engine.register(trigger)

    # Registry and sources
    def get_registry(self) -> "WorkflowRegistry":
        """Return the workflow registry singleton."""
        if self._registry is None:
            self._registry = get_workflow_registry()
        return self._registry  # type: ignore

    def register_registry_source(self, source: "WorkflowSource") -> None:
        """
        Register a workflow source for event resolution.

        Args:
            source: The WorkflowSource pointing to an event package.
        """
        self.get_registry().add_workflow_source(source)

    # Settings
    def set_setting(self, key: str, value: Any) -> None:
        """Set a configuration value for this workflow."""
        self._settings.add(key, value)

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a configuration value, falling back to project defaults."""
        return self._settings.get(key, default)

    # Validation
    def check(self) -> List[str]:
        """
        Validate the workflow configuration.

        Returns:
            List of issue descriptions. Empty list means no issues.

        Note:
            This validates infrastructure readiness, not business logic
            correctness. Missing events.py is a warning, not an error —
            workflows can source events from remote packages.
        """
        issues: List[str] = []

        if not self.is_executable:
            issues.append(f"Workflow '{self.name}' is not executable")

        if not self.get_event_module():
            issues.append(
                f"Workflow '{self.name}' has no events module — "
                f"ensure events are sourced from a registry or add events.py"
            )

        if not self.get_pipeline_module():
            issues.append(
                f"Workflow '{self.name}' has no pipeline module — "
                f"create a pipeline.py with a Pipeline subclass"
            )

        return issues

    # Execution
    async def run_workflow(
        self,
        params: Dict[str, Any],
        run_type: Literal["batch", "single"] = "single",
    ) -> typing.Union[Pipeline, BatchPipeline, None]:
        """
        Execute the workflow.

        Args:
            params: Workflow parameters passed to the pipeline.
            run_type: ``"single"`` for a single execution, ``"batch"`` for
                multi-instance execution via BatchPipeline.

        Returns:
            The Pipeline or BatchPipeline instance that was executed,
            or None if pre-flight checks failed.

        Raises:
            WorkflowExecutionError: If execution fails.
            RuntimeError: If run_type is invalid.
        """
        issues = self.check()
        if issues:
            issue_summary = "; ".join(issues)
            raise WorkflowExecutionError(
                f"Workflow '{self.name}' pre-flight checks failed: {issue_summary}"
            )

        if run_type == "single":
            return await self._run_single(params)

        if run_type == "batch":
            return await self._run_batch(params)

        raise RuntimeError(
            f"Unknown run_type '{run_type}'. Expected 'single' or 'batch'."
        )

    async def _run_single(self, params: Dict[str, Any]) -> Pipeline:
        """Execute a single pipeline run."""
        pipeline_class = self.get_pipeline_class()
        try:
            pipeline = pipeline_class(**params)
            await pipeline.start(force_rerun=True)
            logger.info("Workflow '%s' completed single run successfully", self.name)
            return pipeline
        except Exception as e:
            logger.error("Workflow '%s' failed: %s", self.name, e)
            raise WorkflowExecutionError(
                f"Workflow '{self.name}' execution failed"
            ) from e

    async def _run_batch(
        self, params: Dict[str, Any], on_result: typing.Optional[typing.Callable] = None
    ) -> BatchPipeline:
        """
        Execute a batch pipeline run.

        Args:
            params: Parameters passed to the BatchPipeline constructor.
            on_result: Callback invoked for each completed result. When
                       ``None``, a no-op discard callback is used — results
                       are not accumulated. Pass a callback to collect or
                       stream results.
        """
        batch_pipeline_class = self.get_batch_pipeline_class()
        _on_result = on_result if on_result is not None else (lambda r: None)
        try:
            batch_pipeline = batch_pipeline_class(**params)
            await batch_pipeline.execute(on_result=_on_result)
            logger.info("Workflow '%s' completed batch run successfully", self.name)
            return batch_pipeline
        except Exception as e:
            logger.error("Workflow '%s' batch failed: %s", self.name, e)
            raise WorkflowExecutionError(
                f"Workflow '{self.name}' batch execution failed"
            ) from e
