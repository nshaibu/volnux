from dataclasses import dataclass
from typing import Any, Optional, Type

from .semantic import DeprecationInfo, SemanticVersioning, VersionInfo


@dataclass
class VersionHandler:
    """
    Handles version metadata for any class.
    """

    scheme: SemanticVersioning
    version: str
    changelog: Optional[str] = None
    deprecated: bool = False
    deprecation_info: Optional[DeprecationInfo] = None
    namespace: str = "local"
    class_name: Optional[str] = None
    #: The class this handler describes; the scheme reads its metadata.
    klass: Optional[Type[Any]] = None

    @classmethod
    def from_class(cls, klass: Type[Any]) -> "VersionHandler":
        scheme = SemanticVersioning()

        return cls(
            scheme=scheme,
            version=getattr(klass, "version", scheme.default_version),
            changelog=getattr(klass, "changelog", None),
            deprecated=getattr(klass, "deprecated", False),
            deprecation_info=getattr(klass, "deprecation_info", None),
            namespace=scheme.get_namespace(klass),
            class_name=scheme.get_event_name(klass),
            klass=klass,
        )

    def get_info(self) -> VersionInfo:
        """Get version information."""
        return self.scheme.get_version_info(self.klass or self.__class__)

    def is_deprecated(self) -> bool:
        """Check if deprecated."""
        return self.deprecated

    def validate(self) -> bool:
        """Validate version format."""
        return self.scheme.validate_version(self.version)
