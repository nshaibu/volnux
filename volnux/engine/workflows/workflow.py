"""
Workflow Configuration System - Pure Registry Pattern

WorkflowConfig is ONLY for infrastructure/registry configuration (like Django's AppConfig).
User business logic (steps, pipelines, events) lives in workflow files.

Structure:
workflows/
├── docker_registry/
│   ├── __init__.py
│   ├── workflow.py      # WorkflowConfig - ONLY registries/infrastructure
│   ├── events.py         # USER CODE - event definitions
│   ├── pipeline.py      # USER CODE - pipeline logic
│   └── pointy.pty       # USER CODE - workflow structure
"""

import logging
import types
import typing
import inspect
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional, Callable, Awaitable

from .registry import (
    WorkflowSource,
    get_workflow_registry,
)
from volnux.result import ResultSet as TriggerSet
from volnux.pipeline import Pipeline, BatchPipeline
from volnux.conf import ConfigLoader
from volnux.import_utils import load_module_from_path, load_multiple_submodules

if typing.TYPE_CHECKING:
    from .trigger.triggers import TriggerBase, TriggerActivation
    from .trigger.triggers.base import TriggerType

logger = logging.getLogger(__name__)

system_conf = ConfigLoader.get_lazily_loaded_config()


class WorkflowExecutionError(Exception):
    """Exception raised when pipeline execution fails."""


class WorkflowNotfound(Exception):
    """Workflow was not found"""


class TriggerRegistry:
    """Central registry for managing all triggers."""

    def __init__(self):
        self._triggers: TriggerSet["TriggerBase"] = TriggerSet()

    def register(
        self,
        trigger: "TriggerBase",
        trigger_activation_callback: Callable[["TriggerActivation"], Awaitable[None]],
    ) -> None:
        """
        Register a trigger.
        Args:
            trigger: Instance of a trigger
            trigger_activation_callback: callback to handle trigger when activated
        Raises:
            ValueError: If the trigger already exists
        """
        from .trigger.triggers import TriggerLifecycle

        trigger_qs = self._triggers.filter(trigger_type=trigger.trigger_type)
        if trigger_qs.first():
            raise ValueError(f"Trigger type {trigger.trigger_type.value} already added")

        trigger.set_activation_callback(trigger_activation_callback)
        trigger.lifecycle = TriggerLifecycle.INITIALIZED
        self._triggers.add(trigger)
        logger.info(
            f"Registered trigger {trigger.trigger_id} for workflow {trigger.workflow_name}"
        )

    def unregister(self, trigger_id: str):
        """
        Unregister a trigger.
        Args:
            trigger_id: trigger id
        Raises:
            ValueError: if the trigger does not exists
        """
        trigger = self.get_trigger(trigger_id)
        if trigger is None:
            raise ValueError(f"Trigger {trigger_id} not found")

        self._triggers.discard(trigger)
        logger.info(f"Unregistered trigger {trigger_id}")

    def get_trigger(self, trigger_id: str) -> Optional["TriggerBase"]:
        """
        Get a trigger by ID.
        Args:
            trigger_id: trigger's id
        Returns:
            trigger instance if it exist
        """
        try:
            return typing.cast("TriggerBase", self._triggers.get(id=trigger_id))
        except KeyError:
            return None


class WorkflowConfig(ABC):
    """
    Base class for workflow configuration.

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

    # Attributes to override in subclass
    name: str = None
    verbose_name: Optional[str] = None
    version: str = "1.0.0"
    mode: typing.Literal["DAG", "CFG"] = "CFG"

    # Paths (automatically set by registry)
    path: Optional[Path] = None

    # Default settings (can override)
    default_timeout: int = 300000
    default_retries: int = 3
    default_auto_cleanup: bool = False

    def __init__(self, workflow_path: Optional[Path] = None):
        """Initialize workflow configuration."""

        # Set by loaders
        self.is_executable = False

        if self.name is None:
            raise ValueError("WorkflowConfig.name must be set")

        if self.verbose_name is None:
            self.verbose_name = self.name.replace("_", " ").title()

        self.path = workflow_path
        if self.path is None:
            pass

        self.module: Optional[types.ModuleType] = None

        # Registry storage
        self._registry = None
        self._settings: ConfigLoader = system_conf

        self._loaded_modules: typing.Dict[str, types.ModuleType] = {}

        self.triggers: TriggerRegistry = TriggerRegistry()

        # Call ready hook for infrastructure setup
        self.ready()

    @abstractmethod
    def ready(self):
        """
        Override this to register infrastructure resources.

        SHOULD DO:
        - Register registries
        - Set default configurations
        - Initialize connections
        - Load environment variables
        """
        pass

    def get_registry(self):
        if self._registry is None:
            self._registry = get_workflow_registry()
        return self._registry

    def register_registry_source(self, source: "WorkflowSource") -> None:
        """Register a registry source (infrastructure resource)."""
        self.get_registry().add_workflow_source(source)

    def register_trigger(self, trigger: "TriggerBase") -> None:
        """Register a trigger."""
        from volnux.engine.workflows.trigger import get_trigger_engine

        engine = get_trigger_engine()
        engine.register(trigger)

    def set_setting(self, key: str, value: Any):
        """Set a configuration setting."""
        self._settings.add(key, value)

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a configuration setting."""
        return self._settings.get(key, default)

    def _load_workflow_module(self) -> typing.Optional[types.ModuleType]:
        """
        Load workflow module from a path.
        Returns:
             (WorkflowConfig) workflow module
        Raises:
            ImportError: if workflow module cannot be loaded
        """
        if not self.path or not self.path.exists():
            raise ImportError(
                "Not validate workflow module path found for workflow configuration"
            )

        if not self.module:
            workflow_init_file = self.path / "__init__.py"
            self.module = load_module_from_path(self.name, workflow_init_file)
        return self.module

    def discover_workflow_submodules(self):
        """
        Load workflow module components
        Raises:
            RuntimeError: if workflow module cannot be loaded
        """
        if self._loaded_modules:
            return self._loaded_modules

        try:
            module = self._load_workflow_module()
        except ImportError as e:
            raise RuntimeError(
                f"Failed to load workflow module from path: {self.path}"
            ) from e

        self._loaded_modules = load_multiple_submodules(
            module, self.path, ["events", "pipeline", "batch_pipeline"]
        )
        return self._loaded_modules

    def get_event_module(self):
        """Get the event module (user code)."""
        return self._loaded_modules.get("events")

    def get_pipeline_module(self):
        """Get the pipeline module (user code)."""
        return self._loaded_modules.get("pipeline")

    def get_batch_pipeline_module(self):
        """Get the batch pipeline module (user code)."""
        return self._loaded_modules.get("batch_pipeline")

    def check(self) -> List[str]:
        """
        Check configuration for issues.
        Only validates infrastructure, not user business logic.
        """
        issues = []

        if not self.is_executable:
            issues.append(f"Workflow '{self.name}' is not executable")

        # Check registries
        # if not self.get_registry().get_workflow_config(self.name):
        #     issues.append(f"Workflow '{self.name}' has no registries registered")
        #
        # for name, registry in self.get_registry().get_workflow_source(self.name):
        #     if not registry.location:
        #         issues.append(f"Registry '{name}' has no location configured")
        #
        #     if registry.credentials and not registry.credentials.is_valid():
        #         issues.append(f"Registry '{name}' has invalid credentials")

        # Check user code exists (but don't validate its logic)
        if not self.get_event_module():
            issues.append(f"Workflow '{self.name}' has no event")

        if not self.get_pipeline_module():
            issues.append(f"Workflow '{self.name}' has no pipeline")

        return issues

    def get_pipeline_class(self) -> typing.Type[Pipeline]:
        """
        Get the pipeline class (user code).
        Returns:
            (Pipeline) pipeline class
        Raises:
            RuntimeError: if pipeline class cannot be loaded or found
        """
        module = self.get_pipeline_module()
        if not module:
            raise RuntimeError(f"Workflow '{self.name}' has no pipeline defined")

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                inspect.isclass(attr)
                and attr != Pipeline
                and issubclass(attr, Pipeline)
            ):
                return typing.cast(typing.Type[Pipeline], attr)
        raise RuntimeError(f"Workflow '{self.name}' has no pipeline defined")

    def get_batch_pipeline_class(self) -> typing.Type[BatchPipeline]:
        """
        Get the batch pipeline class (user code).
        Returns:
            (BatchPipeline) batch pipeline class
        Raises:
            RuntimeError: if batch pipeline class cannot be loaded or found
        """
        module = self.get_batch_pipeline_module()
        if not module:
            raise RuntimeError(f"Workflow '{self.name}' has no batch pipeline defined")

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                inspect.isclass(attr)
                and issubclass(attr, BatchPipeline)
                and attr != BatchPipeline
            ):
                return typing.cast(typing.Type[BatchPipeline], attr)

        raise RuntimeError(f"Workflow '{self.name}' has no batch pipeline defined")

    def run_workflow(
        self,
        params: typing.Dict[str, typing.Any],
        run_type: typing.Literal["batch", "single"] = "single",
    ) -> typing.Union[Pipeline, BatchPipeline, None]:
        """
        Run a workflow.

        Args:
            params (dict): workflow parameters
            run_type (str): workflow run type ('batch' or 'single')
        Returns:
            Pipeline or BatchPipeline or None
        Raises:
            RuntimeError: if a workflow run type is not 'batch' or 'single'
            WorkflowExecutionError: if workflow execution fails
        """
        issues = self.check()
        if issues:
            for issue in issues:
                logger.warning(f"  <UNK> {issue}")
            return None

        if run_type == "single":
            pipeline_class = self.get_pipeline_class()
            try:
                pipeline = pipeline_class(**params)
                pipeline.start(force_rerun=True)
                return pipeline
            except Exception as e:
                logger.error(f"  <UNK> {e}")
                raise WorkflowExecutionError("Failed to run workflow") from e
        elif run_type == "batch":
            batch_pipeline_class = self.get_batch_pipeline_class()
            try:
                batch_pipeline = batch_pipeline_class(**params)
                batch_pipeline.execute()
                return batch_pipeline
            except Exception as e:
                logger.error(f"  <UNK> {e}")
                raise WorkflowExecutionError("Failed to run batched workflow") from e
        else:
            raise RuntimeError(f"Unknown run type '{run_type}'")
