import logging
from typing import Dict, Any, Optional, Type
from packaging.version import parse as parse_version, Version
from .base import BaseVersioning, VersionInfo,DeprecationInfo

logger = logging.getLogger(__name__)


class SemanticVersioning(BaseVersioning):
    """
    Semantic versioning (e.g., "1.0.0", "2.1.3").

    Usage:
        class MyEvent(EventBase):
            versioning_class = SemanticVersioning
            version = "2.1.0"
            changelog = "Added new feature"
    """

    default_version = "1.0.0"
    allowed_versions = None  # None = all versions allowed

    def get_version_info(self, klass: Type[Any]) -> VersionInfo:
        """Extract a semantic version from event class attributes."""
        version = getattr(klass, "version", self.default_version)

        # Validate semantic version format
        try:
            parse_version(version)
        except Exception as e:
            logger.exception("Failed to parse semantic version: %s", e)
            raise ValueError(f"Invalid semantic version '{version}': {e}")

        return {
            "version": version,
            "namespace": self.get_namespace(klass),
            "changelog": getattr(klass, "changelog", None),
            "deprecated": getattr(klass, "deprecated", False),
            "deprecation_info": getattr(klass, "deprecation_info", None),
        }

    def validate_version(self, version: str) -> bool:
        """Validate semantic version format."""
        try:
            parse_version(version)

            # Check against allowed versions if specified
            if self.allowed_versions:
                return version in self.allowed_versions

            return True
        except Exception as e:
            logger.warning(f"Invalid semantic version '{version}': {e}")
            return False

    def is_deprecated(self, klass: Type[Any]) -> bool:
        """Check if an event version is deprecated."""
        return getattr(klass, "deprecated", False)

    def get_deprecation_info(self, klass: Type[Any]) -> Optional[DeprecationInfo]:
        """Get deprecation information."""
        if self.is_deprecated(klass):
            return getattr(klass, "deprecation_info", None)
        return None
