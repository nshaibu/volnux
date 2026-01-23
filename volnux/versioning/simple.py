from typing import Dict, Any, Optional, Type
from .base import BaseVersioning, VersionInfo


class SimpleVersioning(BaseVersioning):
    """
    Simple versioning using strings (e.g., "v1", "v2", "latest").

    Usage:
        class MyEvent(EventBase):
            versioning_class = SimpleVersioning
            version = "v2"
    """

    default_version = "v1"
    allowed_versions = None

    def get_version_info(self, klass: Type[Any]) -> VersionInfo:
        """Extract a simple version from event class."""
        version = getattr(klass, "version", self.default_version)

        return {
            "version": version,
            "namespace": self.get_namespace(klass),
            "changelog": getattr(klass, "changelog", None),
            "deprecated": getattr(klass, "deprecated", False),
            "deprecation_info": getattr(klass, "deprecation_info", None),
        }

    def validate_version(self, version: str) -> bool:
        """Validate simple version string."""
        if self.allowed_versions:
            return version in self.allowed_versions
        return True
