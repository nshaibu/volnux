import logging
import typing
import os
import asyncio
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from collections import ChainMap
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from volnux import Event
from volnux.base import EventType
from volnux import __version__ as version
from volnux.base import RetryPolicy, get_event_registry
from volnux.exceptions import ImproperlyConfigured
from volnux.parser.options import Options
from volnux.registry import RegistryNotReady
from volnux.result import EventResult
from volnux.utils import get_function_call_args

__all__ = ["get_system_events", "WorkflowSource", "get_workflow_registry"]

logger = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from .workflow import WorkflowConfig


def get_system_events() -> List[typing.Type[Event]]:
    system_events = []

    for event in get_event_registry().list_all_classes():
        event = typing.cast(typing.Type["Event"], event)
        if event.event_type == EventType.SYSTEM:
            system_events.append(event)
    return system_events


class RegistrySource(Enum):
    """Source types for workflow registries."""

    LOCAL = "local"
    PYPI = "pypi"
    GIT = "git"

    def loader(self) -> typing.Optional[typing.Type[Event]]:
        for event in get_system_events():
            name = getattr(event, "name", None)
            if name and name == self.value:
                return event
        return None


@dataclass
class SourceCredentials:
    """Credentials for registry authentication."""

    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    email: Optional[str] = None

    def is_valid(self) -> bool:
        """Check if credentials are valid."""
        return bool(self.token or (self.username and self.password))


@dataclass
class WorkflowSource:
    """Configuration for remote workflow sources."""

    name: str
    source_type: RegistrySource
    location: str  # URL, package name, or path
    version: Optional[str] = None
    credentials: Optional[SourceCredentials] = None
    timeout: int = 30000
    retries: int = 3
    priority: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.source_type, str):
            self.source_type = RegistrySource(self.source_type)

    def load_workflow_config(
        self, registry: "WorkflowRegistry", options: Optional[Dict[str, Any]] = None
    ) -> EventResult:
        """
        Load workflow configuration for registry.
        Args:
            registry (WorkflowRegistry): Workflow registry.
            options (Dict[str, Any]): Workflow extra options.
        Returns:
            EventResult: Loader result.
        """
        if options is None:
            options = {}

        loader_class = self.source_type.loader()
        if loader_class is None:
            raise ImproperlyConfigured(
                f"No valid loader found for source type {self.name}"
            )

        # setup retry policy
        if self.retries > 0:
            retry_policy = RetryPolicy(
                max_attempts=self.retries,
                retry_on_exceptions=[Exception],
            )

            setattr(loader_class, "retry_policy", retry_policy)

        loader = loader_class(
            None,
            self.name,
            options=Options.from_dict(options),
        )
        kwargs = {
            "location": self.location,
            "credentials": self.credentials,
            "timeout": self.timeout,
            "retries": self.retries,
            "workflow_dir": self.location,
            "registry": registry,
            "version": self.version,
            **self.metadata,
        }

        actual_kwargs = get_function_call_args(loader.process, kwargs)
        try:
            return loader(**actual_kwargs)
        except Exception as e:
            logger.debug(
                f"Failed to load workflow source '{self.name}': %s",
                e,
            )

            return EventResult(
                error=True,
                event_name=self.name,
                content=f"Failed to load workflow source '{self.name}': {str(e)}",
                task_id=self.name,
            )


class WorkflowRegistry:
    """
    Registry for managing workflow configurations.

    Supports both LOCAL and REMOTE workflow sources:
    - LOCAL: Workflows from local filesystem
    - PYPI: Workflows installed as Python packages
    - GIT: Workflows from any Git repository
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        self._workflows: Dict[str, "WorkflowConfig"] = {}
        self._workflow_local_sources: Dict[str, WorkflowSource] = {}
        self._workflow_remote_sources: Dict[str, WorkflowSource] = {}

        self._ready = False
        self._loading = False

        # Cache directory for remote workflows
        self._cache_dir = cache_dir or Path.home() / ".workflow_cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def combined_workflow_sources(self) -> ChainMap[str, WorkflowSource]:
        return ChainMap(self._workflow_local_sources, self._workflow_remote_sources)

    def register(self, workflow_config: "WorkflowConfig"):
        """Register a workflow configuration."""
        if workflow_config.name in self._workflows:
            raise ValueError(f"Workflow '{workflow_config.name}' already registered")
        self._workflows[workflow_config.name] = workflow_config

    def get_workflow_config(self, name: str) -> Optional["WorkflowConfig"]:
        """Get a workflow configuration by name."""
        return self._workflows.get(name)

    def get_workflow_source(self, name: str) -> Optional["WorkflowSource"]:
        """Get a workflow source by name."""
        return self.combined_workflow_sources.get(name)

    def get_workflow_configs(self) -> List["WorkflowConfig"]:
        """Get all registered workflow configurations."""
        return list(self._workflows.values())

    def add_workflow_source(self, source: WorkflowSource):
        """Add a remote workflow source."""
        sources = self.combined_workflow_sources
        if source.name in sources:
            raise ValueError(f"Workflow source '{source.name}' already registered")
        if source.source_type == RegistrySource.LOCAL:
            self._workflow_local_sources[source.name] = source
        else:
            self._workflow_remote_sources[source.name] = source
        logger.info(f"Added remote source: {source.name} ({source.source_type.value})")

    def check_all(self) -> Dict[str, List[str]]:
        """Run infrastructure checks on all workflows."""
        all_issues = {}
        for name, workflow in self._workflows.items():
            issues = workflow.check()
            if issues:
                all_issues[name] = issues
        return all_issues

    def is_ready(self) -> bool:
        """Check if the registry is ready."""
        return not self._loading and self._ready

    def make_ready(self) -> None:
        self._ready = True

    async def populate_local_workflow_configs(
        self, project_dir: Path, workflow_name: typing.Optional[str] = None
    ) -> None:
        """
        Populate local workflow configurations.
        :param project_dir: Project root directory.
        :param workflow_name: Name of the workflow to load. If None load all workflows.
        :return:
        """
        workflows_dir = project_dir / "workflows"

        for workflows_dir in workflows_dir.iterdir():
            if workflows_dir.is_dir():
                dirname = workflows_dir.name

                local = WorkflowSource(
                    name=dirname,
                    location=workflows_dir,  # type: ignore
                    source_type=RegistrySource.LOCAL,
                    version=version,
                )
                self._workflow_local_sources[dirname] = local

                if workflow_name is not None and workflow_name == dirname:
                    break

    async def load_workflow_configs(self) -> None:
        """
        Load workflows from source.
        Return:
            EventResult. Result of loader execution.
        Raises:
            ImproperlyConfigured: If a workflow source is not configured.
        """
        self._loading = True

        loop = asyncio.get_event_loop()

        try:
            sources = list(self.combined_workflow_sources.values())
            if not sources:
                logger.warning("No workflow sources to load")
                return

            params = {
                "cache_dir": self._cache_dir,
            }

            with ThreadPoolExecutor(max_workers=min(4, len(sources))) as executor:
                futures = [
                    loop.run_in_executor(
                        executor, self.process_workflow_source, source, self, params
                    )
                    for source in sources
                ]

                results = await asyncio.gather(*futures, return_exceptions=True)
                for source, result in zip(sources, results):
                    if isinstance(result, BaseException):
                        logger.error(
                            f"Failed to load config from {source}: {result}",
                            exc_info=True,
                        )
                    else:
                        logger.debug(f"Successfully loaded config from {source}")

            if self._workflows:
                self.make_ready()

        finally:
            self._loading = False

    @staticmethod
    def process_workflow_source(
        workflow_source: WorkflowSource, registry, params: Dict[str, Any]
    ) -> EventResult:
        """
        Process a workflow source.
        :param workflow_source: workflow source to process.
        :param registry: registry to use.
        :param params: extra params to pass to a workflow source.
        :return: EventResult.
        """
        return workflow_source.load_workflow_config(registry, params)

    def get_events(self):
        if not self.is_ready():
            raise RegistryNotReady("No workflow not ready.")

        events = []

        event_registry = get_event_registry()
        for workflow in self._workflows.values():
            if workflow.module:
                events.extend(event_registry.get_classes_for_module(workflow.module))
        return events

    def get_pipeline(self):
        pass


_workflow_registry = WorkflowRegistry(cache_dir=os.environ.get("WORKFLOWS_CACHE_DIR"))


def get_workflow_registry() -> WorkflowRegistry:
    return _workflow_registry
