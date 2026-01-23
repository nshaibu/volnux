from typing import Optional, Dict, Type, Any, cast
from dataclasses import dataclass

from .semantic import SemanticVersioning
from .base import BaseVersioning, VersionInfo, DeprecationInfo
from volnux.import_utils import import_string
from volnux.conf import ConfigLoader
from volnux.exceptions import ImproperlyConfigured


conf = ConfigLoader.get_lazily_loaded_config()


@dataclass
class VersionHandler:
    """
    Handles version metadata for any class.
    """

    scheme: BaseVersioning
    version: str
    changelog: Optional[str] = None
    deprecated: bool = False
    deprecation_info: Optional[DeprecationInfo] = None
    namespace: str = "local"
    class_name: Optional[str] = None

    @classmethod
    def from_class(cls, klass: Type[Any], config_key: str) -> "VersionHandler":
        scheme_class = getattr(klass, "versioning_class", None)
        if scheme_class is None:
            scheme_class = conf.get(config_key, {}).get(
                "VERSIONING_CLASS", SemanticVersioning
            )
            if issubclass(scheme_class, str):
                try:
                    scheme_class = cast(str, scheme_class)
                    scheme_class = import_string(scheme_class)
                except ImportError as e:
                    raise ImproperlyConfigured(
                        f"Could not import versioning class '{scheme_class}'"
                    ) from e

            if not issubclass(scheme_class, BaseVersioning):
                raise ImproperlyConfigured(
                    f"Scheme '{scheme_class}' is not a subclass of BaseVersioning"
                )

        scheme_class = cast(Type[BaseVersioning], scheme_class)
        scheme_handler = scheme_class(config_key)

        return cls(
            scheme=scheme_handler,
            version=getattr(klass, "version", scheme_handler.default_version),
            changelog=getattr(klass, "changelog", None),
            deprecated=getattr(klass, "deprecated", False),
            deprecation_info=getattr(klass, "deprecation_info", None),
            namespace=scheme_handler.get_namespace(klass),
            class_name=scheme_handler.get_event_name(klass),
        )

    def get_info(self) -> VersionInfo:
        """Get version information."""
        return self.scheme.get_version_info(self.__class__)

    def is_deprecated(self) -> bool:
        """Check if deprecated."""
        return self.deprecated

    def validate(self) -> bool:
        """Validate version format."""
        return self.scheme.validate_version(self.version)
