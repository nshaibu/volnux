from enum import Enum
from typing import Optional


class TaskNamespace(Enum):
    """Valid task namespaces for external task sources"""

    LOCAL = "local"  # Default, no namespace prefix
    PYPI = "pypi"  # Python Package Index
    GITHUB = "github"  # GitHub repositories
    EVENTHUB = "eventhub"  # Event Hub
    DOCKER = "docker"  # Docker Hub

    @classmethod
    def from_string(cls, namespace: str) -> Optional["TaskNamespace"]:
        """Convert string to TaskNamespace enum"""
        try:
            return cls[namespace.upper()]
        except KeyError:
            return None

    @classmethod
    def is_valid(cls, namespace: str) -> bool:
        """Check if namespace is valid"""
        return namespace.upper() in cls.__members__


VALID_NAMESPACES = {"pypi", "github", "eventhub", "docker", "local"}
