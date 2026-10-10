from typing import Dict, Any, Type
from .base import BaseVersioning, VersionInfo


class NoVersioning(BaseVersioning):
    """
    No versioning - single version per event.

    Usage:
        class MyEvent(EventBase):
            versioning_class = NoVersioning
    """

    default_version = "1.0.0"

    def get_version_info(self, klass: Type[Any]) -> VersionInfo:
        """Always return a default version."""
        return {
            "version": self.default_version,
            "namespace": self.get_namespace(klass),
            "changelog": None,
            "deprecated": False,
            "deprecation_info": None,
        }

    def validate_version(self, version: str) -> bool:
        """Always valid."""
        return True
