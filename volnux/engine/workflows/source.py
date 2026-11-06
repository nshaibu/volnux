import logging
import os
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union, Type, cast, TYPE_CHECKING

from volnux import Event
from volnux.mixins.event import RetryPolicy
from volnux.exceptions import ImproperlyConfigured
from volnux.parser.options import Options
from volnux.result import EventResult
from volnux.engine.workflows.loaders import (
    LoadFromPyPi,
    LoadFromGit,
    LoadFromEventHub,
    LoadFromLocal,
)

if TYPE_CHECKING:
    from .registry import WorkflowRegistry

logger = logging.getLogger(__name__)


@dataclass
class SourceCredentials:
    """
    Credentials for registry authentication.

    Either ``token`` alone or both ``username`` and ``password`` must be
    supplied for the credentials to be considered valid. ``email`` is
    optional and only required by certain private index configurations.
    """

    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    email: Optional[str] = None

    def is_valid(self) -> bool:
        """Return True if the credentials are sufficient for authentication."""
        return bool(self.token or (self.username and self.password))

    def __repr__(self) -> str:
        """Never expose credential values in repr."""
        return (
            f"SourceCredentials("
            f"username={'***' if self.username else None}, "
            f"token={'***' if self.token else None})"
        )


class RegistrySource(Enum):
    """
    Supported workflow source types.

    Loader resolution is delegated to ``LoaderResolver`` — this enum is a
    pure data model and carries no runtime engine dependencies.
    """

    LOCAL = "local"
    PYPI = "pypi"
    GIT = "git"
    HUB = "hub"


_LOADER_REGISTRY: Dict[RegistrySource, Type["Event"]] = {
    RegistrySource.LOCAL: LoadFromLocal,
    RegistrySource.PYPI: LoadFromPyPi,
    RegistrySource.GIT: LoadFromGit,
    RegistrySource.HUB: LoadFromEventHub,
}


# How each source type calls its loader. A loader names its own arguments (the local
# loader wants ``workflow_dir``, the git loader a ``workflow_name`` and a ``branch``,
# the git loader counts its timeout in seconds ...), so the call is built per type
# rather than guessed from the loader's signature: guessing passed None for every
# argument it could not find, and no local source could ever load.


def _local_call(source: "WorkflowSource", registry: Any) -> Dict[str, Any]:
    return {"workflow_dir": Path(source.location), "registry": registry}


def _pypi_call(source: "WorkflowSource", registry: Any) -> Dict[str, Any]:
    return {
        "location": str(source.location),
        "registry": registry,
        "version": source.version,
        "credentials": source.credentials,
        "timeout": source.timeout,  # milliseconds
        "index_url": source.metadata.get("index_url"),
    }


def _git_call(source: "WorkflowSource", registry: Any) -> Dict[str, Any]:
    return {
        "location": str(source.location),
        "workflow_name": source.metadata.get("workflow_name", source.name),
        "registry": registry,
        "credentials": source.credentials,
        # a branch, tag or commit: ``branch`` in the metadata, else the version
        "branch": source.metadata.get("branch") or source.version or "main",
        "timeout": source.timeout / 1000,  # this loader counts seconds
        "as_package": bool(source.metadata.get("as_package", False)),
    }


def _hub_call(source: "WorkflowSource", registry: Any) -> Dict[str, Any]:
    return {
        "location": str(source.location),
        "registry": registry,
        "version": source.version,
        "credentials": source.credentials,
        "timeout": source.timeout,  # milliseconds
    }


_CALL_BUILDERS: Dict[
    RegistrySource, Callable[["WorkflowSource", Any], Dict[str, Any]]
] = {
    RegistrySource.LOCAL: _local_call,
    RegistrySource.PYPI: _pypi_call,
    RegistrySource.GIT: _git_call,
    RegistrySource.HUB: _hub_call,
}

#: Metadata keys that configure the loader itself (its ``options.extras``), not one call.
_LOADER_OPTION_KEYS = ("eventhub_url", "cache_dir")

#: Source types that install a pinned release and so cannot do without a version.
_NEEDS_VERSION = (RegistrySource.PYPI, RegistrySource.HUB)


@dataclass
class WorkflowSource:
    """
    Where to get workflows (or event packages) from, and how.

    ``load_workflow_config()`` picks the loader for ``source_type``, calls it and
    leaves what it found in the registry.

    ===========  ===============================================================
    source_type  what ``location`` is, and what ``metadata`` may carry
    ===========  ===============================================================
    ``local``    a workflow directory (holding ``workflow.py``)
    ``pypi``     a package name; ``version`` is required; ``index_url``
    ``git``      a repository URL; ``workflow_name`` (default: ``name``) is the
                 directory in it; ``branch`` (a branch, tag or commit; default the
                 ``version``, else ``main``); ``as_package`` for an event package
    ``hub``      a package name on EventHub; ``version`` is required;
                 ``eventhub_url`` (or the registry's options) says where the hub is
    ===========  ===============================================================

    ``timeout`` is in milliseconds. ``events`` names the events wanted from a
    package; today it is documentation only and every event of the package is
    registered. ``metadata["options"]`` is merged into the loader's options.
    """

    name: str
    source_type: RegistrySource
    location: Union[str, Path]
    version: Optional[str] = None
    credentials: Optional[SourceCredentials] = None
    timeout: int = 30_000  # milliseconds
    retries: int = 3
    priority: int = 1
    events: Optional[List[str]] = None
    # (not in the repr: it may carry tokens or private URLs)
    metadata: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.source_type, str):
            try:
                self.source_type = RegistrySource(self.source_type)
            except ValueError:
                valid = [s.value for s in RegistrySource]
                raise ValueError(
                    f"WorkflowSource '{self.name}': invalid source_type {self.source_type!r}. "
                    f"Expected one of: {valid}."
                )
        elif not isinstance(self.source_type, RegistrySource):
            raise ValueError(
                f"WorkflowSource '{self.name}': source_type must be a RegistrySource "
                f"or its value, got {type(self.source_type).__name__}."
            )

        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("WorkflowSource: 'name' must be a non-empty string.")
        if self.location is None or not str(self.location).strip():
            raise ValueError(f"WorkflowSource '{self.name}': 'location' is required.")
        if (
            not isinstance(self.timeout, int)
            or isinstance(self.timeout, bool)
            or self.timeout <= 0
        ):
            raise ValueError(
                f"WorkflowSource '{self.name}': 'timeout' is milliseconds and must be a "
                f"positive integer, got {self.timeout!r}."
            )
        if (
            not isinstance(self.retries, int)
            or isinstance(self.retries, bool)
            or self.retries < 0
        ):
            raise ValueError(
                f"WorkflowSource '{self.name}': 'retries' must be 0 or more, got {self.retries!r}."
            )
        if self.events is not None:
            if isinstance(self.events, str) or not all(
                isinstance(e, str) for e in self.events
            ):
                raise ValueError(
                    f"WorkflowSource '{self.name}': 'events' must be a list of event names."
                )
            self.events = list(self.events)
            if self.source_type == RegistrySource.LOCAL:
                logger.warning(
                    "WorkflowSource '%s': 'events' has no effect on a local source.",
                    self.name,
                )
        if self.metadata is None:
            self.metadata = {}

    # ------------------------------------------------------------------ calling the loader

    def missing_requirement(self) -> Optional[str]:
        """Why this source cannot be loaded as configured, or None when it can."""
        if self.source_type in _NEEDS_VERSION and not self.version:
            return (
                f"source '{self.name}' ({self.source_type.value}) needs a version: "
                "an unpinned install is refused so loading stays reproducible"
            )
        if (
            self.source_type == RegistrySource.LOCAL
            and not Path(self.location).exists()
        ):
            return f"local workflow directory not found: {self.location}"
        return None

    def build_loader_call(self, registry: "WorkflowRegistry") -> Dict[str, Any]:
        """The keyword arguments for this source's loader."""
        return _CALL_BUILDERS[self.source_type](self, registry)

    def loader_options(self, options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Options for the loader instance: the registry's, then this source's."""
        merged: Dict[str, Any] = dict(options or {})
        for key in _LOADER_OPTION_KEYS:
            if key in self.metadata:
                merged[key] = self.metadata[key]
        merged.update(self.metadata.get("options") or {})
        return merged

    def _failure(self, message: str) -> "EventResult":
        return EventResult(
            error=True,
            event_name=self.name,
            content=message,
            task_id=self.name,
            workflow_id=self.name,
            process_id=os.getpid(),  # type: ignore
            creation_time=time.time(),  # type: ignore
        )

    async def load_workflow_config(
        self,
        registry: "WorkflowRegistry",
        options: Optional[Dict[str, Any]] = None,
    ) -> "EventResult":
        """
        Resolve the loader for this source, configure it, and load the source into
        ``registry``.

        The retry policy is set on the loader *instance* - not the class - so
        concurrent loads of the same source type do not overwrite each other's
        policy. Only transient errors (``TimeoutError``, ``ConnectionError``,
        ``OSError``) are retried; a permanent one (bad credentials, an unknown
        package) fails at once.

        Args:
            registry: The workflow registry to register the loaded config into.
            options:  Options for the loader (``cache_dir``, ``eventhub_url`` ...);
                      this source's own metadata is added to them.

        Returns:
            EventResult with ``error=False`` on success and ``error=True`` on any
            failure, with a message in ``content`` either way.
        """
        loader_class = _LOADER_REGISTRY.get(self.source_type)
        if loader_class is None:
            raise ImproperlyConfigured(
                f"No loader registered for source type '{self.source_type.value}'. "
                f"Ensure the corresponding SYSTEM event is registered before "
                f"loading workflows."
            )

        problem = self.missing_requirement()
        if problem is not None:
            logger.error("WorkflowSource: %s", problem)
            return self._failure(f"Failed to load workflow source: {problem}")

        loader = cast(
            Event,
            loader_class(
                None,
                self.name,
                options=Options.from_dict(self.loader_options(options)),
            ),
        )

        if self.retries > 0:
            loader.retry_policy = RetryPolicy(
                max_attempts=self.retries,
                retry_on_exceptions=[TimeoutError, ConnectionError, OSError],
            )

        try:
            result = await loader(**self.build_loader_call(registry))  # type: ignore
        except Exception as exc:
            logger.error(
                "WorkflowSource '%s': loader %s raised an unexpected error: %s",
                self.name,
                type(loader).__name__,
                exc,
                exc_info=True,
            )
            return self._failure(f"Failed to load workflow source '{self.name}': {exc}")

        if result.error and not result.content:
            # the loaders report why in the log and return only (False, None)
            result.content = (
                f"Failed to load workflow source '{self.name}' "
                f"({self.source_type.value}); see the log for the reason."
            )
        return result
