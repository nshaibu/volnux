import logging
import os
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Type, cast, TYPE_CHECKING

from volnux import Event
from volnux.mixins.event import RetryPolicy
from volnux.exceptions import ImproperlyConfigured
from volnux.parser.options import Options
from volnux.result import EventResult
from volnux.utils import get_function_call_args
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


@dataclass
class WorkflowSource:
    """
    Configuration for a remote workflow source registered by the user.

    The engine calls ``load_workflow_config()`` to resolve, install (if
    necessary), and register the workflow into the active registry.
    """

    name: str
    source_type: RegistrySource
    location: Union[str, Path]
    version: Optional[str] = None
    credentials: Optional[SourceCredentials] = None
    timeout: int = 30_000  # milliseconds
    retries: int = 3
    priority: int = 1
    # None means "all events" — only valid for EventHub sources.
    # For PyPI/GitHub, None registers everything which is discouraged
    # and should emit a warning in production environments.
    events: Optional[List[str]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

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

    async def load_workflow_config(
        self,
        registry: "WorkflowRegistry",
        options: Optional[Dict[str, Any]] = None,
    ) -> "EventResult":
        """
        Resolve the appropriate loader, configure it, and load this source
        into ``registry``.

        The retry policy is set on the loader *instance* — not the class —
        so concurrent loads of the same source type do not overwrite each
        other's policy on the shared class object.

        Transient network errors (``TimeoutError``, ``ConnectionError``) are
        the only exceptions worth retrying; permanent errors (bad credentials,
        unknown package) are propagated immediately so the caller gets a clear
        failure rather than burning through retry attempts unnecessarily.

        Args:
            registry: The workflow registry to register the loaded config into.
            options:  Optional extra options are forwarded to the loader's process().

        Returns:
            EventResult with ``error=False`` on success, ``error=True`` on
            any failure, and a descriptive ``content`` string in both cases.
        """
        if options is None:
            options = {}

        loader_class = _LOADER_REGISTRY.get(self.source_type)
        if loader_class is None:
            raise ImproperlyConfigured(
                f"No loader registered for source type '{self.source_type.value}'. "
                f"Ensure the corresponding SYSTEM event is registered before "
                f"loading workflows."
            )

        loader = loader_class(
            None,
            self.name,
            options=Options.from_dict(options),
        )

        loader: Event = cast(object, loader)  # type: ignore

        # Apply the retry policy to the *instance* so concurrent loads of the
        # same source types cannot overwrite each other's class-level policy.
        if self.retries > 0:
            loader.retry_policy = RetryPolicy(
                max_attempts=self.retries,
                retry_on_exceptions=[TimeoutError, ConnectionError, OSError],
            )

        all_kwargs = {
            "location": self.location,
            "credentials": self.credentials,
            "timeout": self.timeout,
            "retries": self.retries,
            "version": self.version,
            "registry": registry,
            **self.metadata,
        }

        actual_kwargs = get_function_call_args(loader.process, all_kwargs)

        dropped = set(all_kwargs) - set(actual_kwargs)
        if dropped:
            logger.debug(
                "WorkflowSource '%s': the following kwargs were not consumed "
                "by %s.process() and have been dropped: %s",
                self.name,
                type(loader).__name__,
                sorted(dropped),
            )

        try:
            return await loader(**actual_kwargs)  # type: ignore
        except Exception as exc:
            logger.error(
                "WorkflowSource '%s': loader %s raised an unexpected error: %s",
                self.name,
                type(loader).__name__,
                exc,
                exc_info=True,
            )
            return EventResult(
                error=True,
                event_name=self.name,
                content=f"Failed to load workflow source '{self.name}': {exc}",
                task_id=self.name,
                workflow_id=self.name,
                process_id=os.getpid(),  # type: ignore
                creation_time=time.time(),  # type: ignore
            )
