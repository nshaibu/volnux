"""
Event versions: semantic versioning (https://semver.org), and nothing else.

An event declares ``version = "2.1.0"`` (``"1.0.0"`` if it says nothing); the
version is checked when the event class is created, so a malformed one fails at
import time instead of the first time it is compared.
"""

import functools
import logging
import re
import typing
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Type, TypedDict, Union

__all__ = [
    "DeprecationInfo",
    "SemVer",
    "SemanticVersioning",
    "VersionInfo",
    "is_semver",
    "parse_semver",
]

logger = logging.getLogger(__name__)

# The regular expression of https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
_IDENTIFIER = r"(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
SEMVER_PATTERN = re.compile(
    r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    rf"(?:-(?P<prerelease>{_IDENTIFIER}(?:\.{_IDENTIFIER})*))?"
    r"(?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)
_PARTIAL_PATTERN = re.compile(
    r"^(?P<major>\d+)(?:\.(?P<minor>\d+))?(?:\.(?P<patch>\d+))?$"
)


class DeprecationInfo(TypedDict, total=False):
    reason: str
    end_of_life: Union[str, datetime]
    migration_guide: str
    replacement: str
    deprecated_at: str


class VersionInfo(TypedDict, total=False):
    version: str
    namespace: Optional[str]
    changelog: Optional[str]
    deprecated: bool
    deprecation_info: Optional[DeprecationInfo]


@functools.total_ordering
class SemVer:
    """A parsed semantic version, ordered by the rules of semver.org: numbers
    first, a pre-release before its release, pre-release identifiers compared one
    by one (numbers below words), build metadata ignored."""

    __slots__ = ("major", "minor", "patch", "prerelease", "build")

    def __init__(
        self,
        major: int,
        minor: int,
        patch: int,
        prerelease: Tuple[str, ...] = (),
        build: Tuple[str, ...] = (),
    ) -> None:
        self.major, self.minor, self.patch = major, minor, patch
        self.prerelease, self.build = prerelease, build

    def _key(self) -> tuple:
        if self.prerelease:
            pre: tuple = (
                0,
                tuple(
                    (0, int(i), "") if i.isdigit() else (1, 0, i)
                    for i in self.prerelease
                ),
            )
        else:
            pre = (1, ())
        return (self.major, self.minor, self.patch, pre)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SemVer) and self._key() == other._key()

    def __lt__(self, other: "SemVer") -> bool:
        if not isinstance(other, SemVer):
            return NotImplemented
        return self._key() < other._key()

    def __hash__(self) -> int:
        return hash(self._key())

    def __str__(self) -> str:
        text = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            text += "-" + ".".join(self.prerelease)
        if self.build:
            text += "+" + ".".join(self.build)
        return text

    def __repr__(self) -> str:
        return f"SemVer('{self}')"


def parse_semver(text: str, partial: bool = False) -> SemVer:
    """Parse ``MAJOR.MINOR.PATCH[-prerelease][+build]``.

    :param partial: Also accept ``"2"`` and ``"2.1"`` (as 2.0.0 and 2.1.0), for
        the base of a version constraint such as ``^2``.
    :raises ValueError: the text is not a semantic version.
    """
    if isinstance(text, str):
        match = SEMVER_PATTERN.match(text)
        if match:
            return SemVer(
                int(match["major"]),
                int(match["minor"]),
                int(match["patch"]),
                tuple(match["prerelease"].split(".")) if match["prerelease"] else (),
                tuple(match["build"].split(".")) if match["build"] else (),
            )
        if partial:
            match = _PARTIAL_PATTERN.match(text)
            if match:
                return SemVer(
                    int(match["major"]),
                    int(match["minor"] or 0),
                    int(match["patch"] or 0),
                )
    raise ValueError(
        f"{text!r} is not a semantic version: expected MAJOR.MINOR.PATCH, "
        'optionally with -prerelease and +build, e.g. "2.1.0" or "1.0.0-rc.1"'
    )


def is_semver(text: object) -> bool:
    return isinstance(text, str) and SEMVER_PATTERN.match(text) is not None


class SemanticVersioning:
    """The versioning of events: semantic versions.

    Usage::

        class MyEvent(EventBase):
            version = "2.1.0"
            changelog = "Added new feature"
            namespace = "trading"      # default: the DEFAULT_EVENT_NAMESPACE setting
    """

    default_version = "1.0.0"
    #: None: any semantic version is allowed.
    allowed_versions: Optional[List[str]] = None

    def get_version_info(self, klass: Type[Any]) -> VersionInfo:
        """Extract the version information from an event class.

        :raises ValueError: the class's ``version`` is not a semantic version.
        """
        version = getattr(klass, "version", self.default_version)
        parse_semver(version)  # raises with a message that says what is expected

        return {
            "version": version,
            "namespace": self.get_namespace(klass),
            "changelog": getattr(klass, "changelog", None),
            "deprecated": getattr(klass, "deprecated", False),
            "deprecation_info": getattr(klass, "deprecation_info", None),
        }

    def validate_version(self, version: str) -> bool:
        """Whether ``version`` is a semantic version (and an allowed one)."""
        if not is_semver(version):
            logger.warning("Invalid semantic version %r", version)
            return False
        if self.allowed_versions:
            return version in self.allowed_versions
        return True

    def get_namespace(self, klass: Type[Any]) -> str:
        """The event's namespace: its ``namespace`` attribute, else the
        ``DEFAULT_EVENT_NAMESPACE`` setting."""
        namespace = getattr(klass, "namespace", None)
        if namespace is None:
            from volnux.config import setting

            return str(setting("DEFAULT_EVENT_NAMESPACE", "local"))
        return namespace

    def get_event_name(self, klass: Type[Any]) -> str:
        """The event's name: its ``name`` attribute, else the class name."""
        # `name` exists on EventBase as None, so getattr's default never applies
        return getattr(klass, "name", None) or klass.__name__

    def is_deprecated(self, klass: Type[Any]) -> bool:
        return getattr(klass, "deprecated", False)

    def get_deprecation_info(self, klass: Type[Any]) -> Optional[DeprecationInfo]:
        if self.is_deprecated(klass):
            return getattr(klass, "deprecation_info", None)
        return None
