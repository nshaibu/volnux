import abc
import typing
from datetime import datetime
from typing import Optional, Dict, Any, List, TypedDict, Union

from volnux.conf import ConfigLoader

conf = ConfigLoader.get_lazily_loaded_config()


class DeprecationInfo(TypedDict, total=False):
    reason: str
    end_of_life: Union[str, datetime]
    migration_guide: str
    replacement: str


class VersionInfo(TypedDict, total=False):
    version: str
    namespace: Optional[str]
    changelog: Optional[str]
    deprecated: bool
    deprecation_info: Optional[DeprecationInfo]


class BaseVersioning(abc.ABC):
    """
    Base class for event versioning schemes.
    """

    # Default settings (can be overridden in settings.py)
    default_version: str = "1.0.0"
    allowed_versions: Optional[List[str]] = None

    def __init__(self, config_key: str) -> None:
        self.config_key = config_key

    @abc.abstractmethod
    def get_version_info(self, klass: typing.Type[Any]) -> VersionInfo:
        """
        Extract version information from the event class.

        Returns:
            Dict with keys: version, namespace, changelog, deprecated, etc.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def validate_version(self, version: str) -> bool:
        """Check if a version is valid/allowed."""
        raise NotImplementedError()

    def get_namespace(self, klass: typing.Type[Any]) -> str:
        """Get namespace for the event (default: 'local')."""
        namespace = getattr(klass, "namespace", None)
        if namespace is None:
            return conf.get(self.config_key, {}).get("DEFAULT_NAMESPACE", "local")
        return namespace

    def get_event_name(self, klass: typing.Type[Any]) -> str:
        """name of class (default: class name)."""
        return getattr(klass, "name", klass.__name__)

    def is_deprecated(self, klass: typing.Type[Any]) -> bool:
        """Check if this event version is deprecated."""
        return False

    def get_deprecation_info(self, klass: type) -> Optional[Dict[str, Any]]:
        """Get deprecation details if deprecated."""
        return None
