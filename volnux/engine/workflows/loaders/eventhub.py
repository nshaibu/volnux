import importlib
import json
import logging
import tempfile
import typing
import httpx
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from volnux import EventBase
from volnux.event.base import EventType
from .git import LoadFromGit
from .pypi import LoadFromPyPi
from volnux.manifest.utils import (
    check_compatibility,
    load_manifest,
    redact_credentials,
    register_workflow_config_and_events_from_manifest,
)

if typing.TYPE_CHECKING:
    from ..source import SourceCredentials
    from ..registry import WorkflowRegistry

logger = logging.getLogger(__name__)

# JSON Schema paths
_SCHEMA_DIR = Path(__file__).parent.parent.parent / "manifest" / "schema"
_EVENT_SCHEMA_PATH = _SCHEMA_DIR / "event" / "v1.json"
_WORKFLOW_SCHEMA_PATH = _SCHEMA_DIR / "workflow" / "v1.json"

# ``package.source.type`` values (from both JSON schemas)
_SOURCE_PYPI = "pypi"
_SOURCE_GIT = "git"
_SOURCE_HUB = "hub"


def _load_json_schema(path: Path) -> Dict[str, Any]:
    with path.open() as fh:
        return json.load(fh)


def _validate_manifest_against_schema(
    manifest: Dict[str, Any], package_name: str
) -> bool:
    """
    Validate *manifest* against the appropriate JSON schema.

    Dispatches on ``package.package_type``:
    - ``"event"`` → ``manifest/schema/event/v1.json``
    - ``"workflow"`` → ``manifest/schema/workflow/v1.json``

    ``jsonschema`` is imported lazily — installations that never use EventHub
    do not need to carry it as a hard dependency. When not installed, validation
    is skipped with a warning.

    Returns ``True`` on success, ``False`` on failure.
    """
    try:
        import jsonschema
    except ImportError:
        logger.warning(
            "LoadFromEventHub: 'jsonschema' is not installed — "
            "full manifest schema validation is skipped. "
            "Install with: pip install jsonschema"
        )
        return True

    package_type = manifest.get("package", {}).get("package_type", "")
    if package_type == "event":
        schema = _load_json_schema(_EVENT_SCHEMA_PATH)
    elif package_type == "workflow":
        schema = _load_json_schema(_WORKFLOW_SCHEMA_PATH)
    else:
        logger.error(
            "LoadFromEventHub: unknown package_type %r for %r. "
            "Expected 'event' or 'workflow'.",
            package_type,
            package_name,
        )
        return False

    try:
        jsonschema.validate(instance=manifest, schema=schema)
        return True
    except jsonschema.ValidationError as exc:
        logger.error(
            "LoadFromEventHub: manifest schema validation failed for %r: %s",
            package_name,
            exc.message,
        )
        return False


def _jwt_headers(credentials: Optional["SourceCredentials"]) -> Dict[str, str]:
    """
    Build HTTP headers for EventHub JWT authentication.

    EventHub uses Bearer tokens exclusively. Only ``credentials.token`` is
    used — username/password credentials are not supported by the EventHub
    HTTP API.
    """
    if credentials and credentials.token:
        return {"Authorization": f"Bearer {credentials.token}"}
    return {}


class LoadFromEventHub(EventBase):
    """
    Async loader that fetches and installs a package from the EventHub registry.

    Responsibilities
    ----------------
    - Manifest fetching (httpx, JWT auth).
    - Full manifest schema validation (jsonschema).
    - Identity cross-check (name + version vs. request).
    - Compatibility warnings via ``check_compatibility``.
    - Sub-loader routing on ``package.source.type``.
    - Post-install registration via
      ``register_workflow_config_and_events_from_manifest``.
    """

    name = "eventhub"
    event_type = EventType.SYSTEM
    checkpointing_enabled = False

    async def process(
        self,
        location: str,
        registry: "WorkflowRegistry",
        version: Optional[str] = None,
        credentials: Optional["SourceCredentials"] = None,
        timeout: int = 30_000,
        **kwargs: Any,
    ) -> Tuple[bool, Any]:
        """
        Fetch, validate, install, and register a package from EventHub.

        Args:
            location:    Package name or EventHub slug.
            registry:    Target workflow registry.
            version:     Required. Exact package version (semver).
            credentials: JWT credentials for EventHub authentication.
                         ``credentials.token`` is the Bearer token.
                         Also forwarded to sub-loaders for private PyPI / Git.
            timeout:     HTTP and installation timeout in milliseconds.
            **kwargs:    Absorbed.

        Returns:
            ``(True, WorkflowConfig class)`` for workflow packages.
            ``(True, manifest dict)`` for event-only packages.
            ``(False, None)`` on any failure.
        """
        if not version:
            logger.error("LoadFromEventHub: 'version' is required for %r.", location)
            return False, None

        eventhub_url: Optional[str] = self.options.extras.get("eventhub_url")
        if not eventhub_url:
            logger.error(
                "LoadFromEventHub: 'eventhub_url' must be set in options.extras."
            )
            return False, None

        timeout_s = timeout / 1000

        manifest = await self._fetch_manifest(
            eventhub_url=eventhub_url,
            package_name=location,
            version=version,
            credentials=credentials,
            timeout_s=timeout_s,
        )
        if manifest is None:
            return False, None

        if not _validate_manifest_against_schema(manifest, location):
            return False, None

        if not self._check_identity(manifest, location, version):
            return False, None

        check_compatibility(manifest, location)

        source = manifest.get("package", {}).get("source", {})
        source_type = source.get("type", _SOURCE_PYPI)

        installed = await self._fetch_package(
            source_type=source_type,
            source=source,
            package_name=location,
            version=version,
            credentials=credentials,
            timeout=timeout,
            timeout_s=timeout_s,
            registry=registry,
            eventhub_url=eventhub_url,
        )
        if not installed:
            return False, None

        return await self._register_from_manifest(manifest, location, version, registry)

    @staticmethod
    async def _fetch_manifest(
        eventhub_url: str,
        package_name: str,
        version: str,
        credentials: Optional["SourceCredentials"],
        timeout_s: float,
    ) -> Optional[Dict[str, Any]]:
        """
        GET ``{eventhub_url}/packages/{name}/{version}/manifest`` (JWT Bearer).
        """
        url = f"{eventhub_url.rstrip('/')}/packages/{package_name}/{version}/manifest"
        headers = _jwt_headers(credentials)

        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                response = await client.get(url, headers=headers)
        except httpx.TimeoutException:
            logger.error(
                "LoadFromEventHub: manifest request timed out after %.1fs for %r.",
                timeout_s,
                package_name,
            )
            return None
        except httpx.RequestError as exc:
            logger.error(
                "LoadFromEventHub: manifest request failed for %r: %s",
                package_name,
                exc,
            )
            return None

        if response.status_code == 401:
            logger.error(
                "LoadFromEventHub: authentication failed for %r. "
                "Verify credentials.token is a valid JWT.",
                package_name,
            )
            return None
        if response.status_code == 404:
            logger.error(
                "LoadFromEventHub: %r version %r not found on EventHub.",
                package_name,
                version,
            )
            return None
        if not response.is_success:
            logger.error(
                "LoadFromEventHub: HTTP %d fetching manifest for %r.",
                response.status_code,
                package_name,
            )
            return None

        try:
            return response.json()
        except Exception as exc:
            logger.error(
                "LoadFromEventHub: manifest for %r is not valid JSON: %s",
                package_name,
                exc,
            )
            return None

    @staticmethod
    def _check_identity(
        manifest: Dict[str, Any], package_name: str, version: str
    ) -> bool:
        """Verify name and version in the manifest match the request."""
        pkg = manifest.get("package", {})

        manifest_name = pkg.get("name", "")
        if manifest_name != package_name:
            logger.error(
                "LoadFromEventHub: name mismatch for %r — manifest declares %r.",
                package_name,
                manifest_name,
            )
            return False

        manifest_version = pkg.get("version", "")
        if manifest_version != version:
            logger.error(
                "LoadFromEventHub: version mismatch for %r — "
                "requested %r but manifest declares %r.",
                package_name,
                version,
                manifest_version,
            )
            return False

        return True

    async def _fetch_package(
        self,
        source_type: str,
        source: Dict[str, Any],
        package_name: str,
        version: str,
        credentials: Optional["SourceCredentials"],
        timeout: int,
        timeout_s: float,
        registry: "WorkflowRegistry",
        eventhub_url: str,
    ) -> bool:
        """Route to the correct sub-loader based on ``package.source.type``."""
        loader: Optional[EventBase] = None
        params: Dict[str, Any] = {}

        if source_type == _SOURCE_PYPI:
            loader = LoadFromPyPi(
                self._execution_context, self._task_id, options=self.options
            )
            loader.retry_policy = self.retry_policy
            params = {
                "location": source.get("package", package_name),
                "registry": registry,
                "version": version,
                "credentials": credentials,
                "timeout": timeout,
                "index_url": source.get("index_url"),
            }

        if source_type == _SOURCE_GIT:
            repo_url = source.get("package")
            git_ref = source.get("git_ref")
            if not repo_url:
                logger.error(
                    "LoadFromEventHub: manifest for %r is missing "
                    "'source.package' (git URL) for git channel.",
                    package_name,
                )
                return False
            loader = LoadFromGit(
                self._execution_context, self._task_id, options=self.options
            )
            loader.retry_policy = self.retry_policy
            params = {
                "location": repo_url,
                "workflow_name": package_name,
                "registry": registry,
                "credentials": credentials,
                # git_ref is a pinned tag/SHA — use it as the branch argument, so
                # git checks out the exact ref, not a mutable branch head.
                "branch": git_ref or "main",
                "timeout": timeout / 1000 if timeout else None,
            }

        if loader is not None:
            try:
                result = await loader(**params)
                return result.error is False
            except Exception as exc:
                logger.error(
                    "LoadFromEventHub: %r failed to install: %s", package_name, exc
                )
                return False

        if source_type == _SOURCE_HUB:
            return await self._fetch_via_hub(
                eventhub_url=eventhub_url,
                package_name=package_name,
                version=version,
                credentials=credentials,
                timeout_s=timeout_s,
                timeout=timeout,
                registry=registry,
            )

        logger.error(
            "LoadFromEventHub: unknown source type %r for %r. " "Expected one of: %r.",
            source_type,
            package_name,
            [_SOURCE_PYPI, _SOURCE_GIT, _SOURCE_HUB],
        )
        return False

    async def _fetch_via_hub(
        self,
        eventhub_url: str,
        package_name: str,
        version: str,
        credentials: Optional["SourceCredentials"],
        timeout_s: float,
        timeout: int,
        registry: "WorkflowRegistry",
    ) -> bool:
        """
        Download the wheel from EventHub and install it with pip.

        Endpoint:

            GET {eventhub_url}/packages/{name}/{version}/download

        The wheel is streamed to a temp file, then passed to: class:`LoadFromPyPi` as a ``file://`` path.
        """

        download_url = (
            f"{eventhub_url.rstrip('/')}/packages/{package_name}/{version}/download"
        )
        headers = _jwt_headers(credentials)

        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                async with client.stream(
                    "GET", download_url, headers=headers
                ) as response:
                    if response.status_code == 401:
                        logger.error(
                            "LoadFromEventHub: authentication failed downloading %r.",
                            package_name,
                        )
                        return False
                    if not response.is_success:
                        logger.error(
                            "LoadFromEventHub: HTTP %d downloading %r.",
                            response.status_code,
                            package_name,
                        )
                        return False

                    try:
                        with tempfile.NamedTemporaryFile(
                            suffix=".whl", delete=False
                        ) as tmp:
                            async for chunk in response.aiter_bytes(chunk_size=65536):
                                tmp.write(chunk)
                            wheel_path = tmp.name
                    except OSError as exc:
                        logger.error(
                            "LoadFromEventHub: failed to write wheel for %r: %s",
                            package_name,
                            exc,
                        )
                        return False

        except httpx.TimeoutException:
            logger.error(
                "LoadFromEventHub: wheel download timed out for %r.", package_name
            )
            return False
        except httpx.RequestError as exc:
            logger.error(
                "LoadFromEventHub: wheel download failed for %r: %s", package_name, exc
            )
            return False

        logger.info(
            "LoadFromEventHub: installing wheel for %r from EventHub.", package_name
        )
        loader = LoadFromPyPi(
            self._execution_context, self._task_id, options=self.options
        )
        params = {
            "location": f"file://{wheel_path}",
            "registry": registry,
            "version": version,
            "credentials": None,  # local file — no index auth needed
            "timeout": timeout,
            "index_url": None,
        }

        try:
            result = await loader(**params)
            return result.error is False
        except Exception as exc:
            logger.error(
                "LoadFromEventHub: failed to install wheel for %r: %s",
                package_name,
                exc,
            )
            return False

    @staticmethod
    async def _register_from_manifest(
        manifest: Dict[str, Any],
        package_name: str,
        version: str,
        registry: "WorkflowRegistry",
    ) -> Tuple[bool, Any]:
        """
        Import the installed package, then use
        ``register_workflow_config_and_events_from_manifest`` from utils to
        register all declared events and any ``WorkflowConfig`` subclasses.

        The utility already handles:
        - Resolving each event class from its ``module`` + ``class`` fields.
        - Deprecation warnings.
        - ``WorkflowConfig`` discovery via ``resolve_workflow_config_classes``.
        - Registry insertion for both events and workflow configs.

        Returns:
            ``(True, first WorkflowConfig instance)`` when workflow configs are
            registered.
            ``(True, manifest dict)`` for event-only packages.
            ``(False, None)`` on import failure or empty registration.
        """

        try:
            module = importlib.import_module(package_name)
        except ImportError as exc:
            logger.error(
                "LoadFromEventHub: %r installed but cannot be imported: %s",
                package_name,
                exc,
            )
            return False, None

        # register_workflow_config_and_events_from_manifest is sync (no I/O)
        # but may be slow for large manifests — run in thread to stay non-blocking.
        registered_events, registered_configs = await asyncio.to_thread(
            register_workflow_config_and_events_from_manifest,
            manifest,
            module,
            package_name,
            registry,
        )

        if not registered_events and not registered_configs:
            logger.error(
                "LoadFromEventHub: nothing was registered from %r==%s. "
                "Check the manifest events[] / workflows[] entries.",
                package_name,
                version,
            )
            return False, None

        if registered_configs:
            # Return the first registered WorkflowConfig instance.
            first_config = next(iter(registered_configs))
            logger.info(
                "LoadFromEventHub: registered %d event(s) and %d workflow config(s) "
                "from %r==%s.",
                len(registered_events),
                len(registered_configs),
                package_name,
                version,
            )
            return True, type(first_config)

        # Event-only package.
        logger.info(
            "LoadFromEventHub: registered %d event(s) from %r==%s "
            "(event-only package).",
            len(registered_events),
            package_name,
            version,
        )
        return True, manifest
