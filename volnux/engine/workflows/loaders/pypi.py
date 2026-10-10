import logging
import re
import sys
import typing
from pathlib import Path
from typing import Any, List, Optional, Union

from volnux import Event
from volnux.event.base import EventType
from volnux.exceptions import SubprocessTimeoutError
from volnux.utils import run_command
from volnux.concurrency.async_utils import to_thread
from volnux.manifest.utils import build_authenticated_url, redact_credentials


if typing.TYPE_CHECKING:
    from ..source import SourceCredentials
    from ..registry import WorkflowRegistry

logger = logging.getLogger(__name__)

# PEP 508 package name: letters, digits, hyphens, underscores, dots.
_SAFE_PACKAGE_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$")

# PEP 440 version: digits and dots, with optional pre/post/dev suffixes.
_SAFE_VERSION_RE = re.compile(
    r"^\d+(\.\d+)*"
    r"([-_.]?(alpha|beta|preview|rc|a|b|c)\d*)?"
    r"([-_.]?(post|rev|r)\d*)?"
    r"([-_.]?dev\d*)?$",
    re.IGNORECASE,
)


class LoadFromPyPi(Event):
    """
    Represents an event for loading packages from PyPI with validation, installation,
    and optional authentication for private indices.

    This class is responsible for ensuring that packages loaded via PyPI conform
    to certain requirements, such as valid package names and version specifications.
    It optionally accepts credentials for private indices and handles embedding
    these credentials into the appropriate URL during installation. The class
    utilizes pip for package management and performs validation before installation
    to prevent misconfiguration.

    :ivar name: Event name identifier.
    :type name: str
    :ivar event_type: Type of event, represents it as a system-level event.
    :type event_type: EventType
    :ivar checkpointing_enabled: Indicates if checkpointing is enabled for this event.
    :type checkpointing_enabled: bool
    """

    name = "pypi"
    event_type = EventType.SYSTEM
    checkpointing_enabled = False

    async def process(
        self,
        location: Union[str, Path],
        registry: "WorkflowRegistry",
        version: Optional[str] = None,
        credentials: Optional["SourceCredentials"] = None,
        timeout: int = 30_000,
        index_url: Optional[str] = None,
        **kwargs: Any,
    ) -> typing.Tuple[bool, Optional[str]]:
        """
        Validate and install a PyPI package.

        Args:
            location: PyPI package name.
            registry:    Passed through for interface compatibility; not used
                         by this loader — registration is handled downstream.
            version:     Required. Exact version to install (PEP 440).
            credentials: Optional credentials for private index authentication.
            timeout:     pip timeout in milliseconds. Defaults to 30 000 ms.
            index_url:   Private index URL. When present and credentials are
                         valid, credentials are embedded into the URL and passed
                         as ``--extra-index-url``. Ignored when absent.
            **kwargs:    Absorbed; unexpected kwargs are silently dropped.

        Returns:
            ``(True, package_spec)`` — e.g. ``(True, "mypackage==1.2.3")``
            on successful installation.
            ``(False, None)`` on any validation or installation failure.
        """
        package_name = str(location)

        if not _SAFE_PACKAGE_RE.match(package_name):
            logger.error(
                "LoadFromPyPi: %r is not a valid PEP 508 package name.",
                package_name,
            )
            return False, None

        if not version:
            logger.error(
                "LoadFromPyPi: 'version' is required for %r. "
                "Unpinned installs are rejected to guarantee reproducible loading.",
                package_name,
            )
            return False, None

        if not _SAFE_VERSION_RE.match(version):
            logger.error(
                "LoadFromPyPi: %r is not a valid PEP 440 version specifier.",
                version,
            )
            return False, None

        package_spec = f"{package_name}=={version}"
        logger.info("LoadFromPyPi: installing %r", package_spec)

        cmd = self._build_pip_command(package_spec, credentials, index_url)

        try:
            result = await to_thread(
                run_command, cmd=cmd, timeout_ms=timeout
            )  # run_command(cmd, timeout_ms=timeout)
        except SubprocessTimeoutError as exc:
            logger.error(
                "LoadFromPyPi: pip timed out after %.1fs for %r.",
                exc.timeout_seconds,
                package_spec,
            )
            return False, None

        if result.returncode != 0:
            # stderr may echo the index URL with embedded credentials — redact
            # before logging.
            logger.error(
                "LoadFromPyPi: pip failed (exit %d) for %r:\n%s",
                result.returncode,
                package_spec,
                redact_credentials(result.stderr or result.stdout or "(no output)"),
            )
            return False, None

        logger.info("LoadFromPyPi: installed %r successfully", package_spec)
        return True, package_spec

    @staticmethod
    def _build_pip_command(
        package_spec: str,
        credentials: Optional["SourceCredentials"],
        index_url: Optional[str],
    ) -> List[Union[str, bytes]]:
        """
        Build the ``pip install`` command.

        Credentials are embedded in the index URL rather than passed as
        plaintext CLI arguments, so they cannot be captured by process-listing
        tools or shell history.

        A warning is emitted when credentials are supplied without an
        ``index_url`` — public PyPI does not require authentication, so this
        is almost always a misconfiguration.
        """
        cmd = [sys.executable, "-m", "pip", "install", package_spec]

        if credentials and credentials.is_valid():
            if not index_url:
                logger.warning(
                    "LoadFromPyPi: credentials supplied for %r but no "
                    "'index_url' was provided. Credentials will be ignored "
                    "and the public PyPI index will be used.",
                    package_spec,
                )
            else:
                authed_url = build_authenticated_url(credentials, index_url, "pypi")
                cmd += ["--extra-index-url", authed_url]

        return cmd
