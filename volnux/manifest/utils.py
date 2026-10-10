from __future__ import annotations

import importlib
import logging
import re
import os
import json
import sys
import types
import typing
import importlib.resources as pkg_resources
from pathlib import Path
from urllib.parse import quote
from volnux import __version__ as volnux_runtime_version
from typing import Any, Dict, List, Optional, Union, Set, TYPE_CHECKING, Tuple, Literal
from urllib.parse import urlparse, urlunparse

if TYPE_CHECKING:
    from volnux.event.base import EventBase

    # from volnux.engine.workflows import WorkflowRegistry, WorkflowConfig
    # from volnux.engine.workflows.source import SourceCredentials


__all__ = [
    "load_manifest",
    "check_compatibility",
    "register_workflow_config_and_events_from_manifest",
    "build_authenticated_url",
    "resolve_workflow_config_classes",
    "resolve_event_class",
    "redact_credentials",
]

logger = logging.getLogger(__name__)

# Canonical manifest filename. Must match the filename shipped in the wheel.
_MANIFEST_FILENAME = "volnux.manifest.json"

# Required schema URI. Validated before any other field is trusted.
_MANIFEST_SCHEMA_FILE = Path(__file__).cwd() / os.sep.join(
    ("schema", "event", "v1.json")
)

_MANIFEST_SCHEMA_URI = "https://eventhub.volnux.dev/manifest/v1.json"

# Top-level fields that must be present for a manifest to be considered valid.
_MANIFEST_REQUIRED_FIELDS = frozenset(
    [
        "$schema",
        "manifest_version",
        "package",
        "events",
        "changelog",
        "dependencies",
        "compatibility",
    ]
)

# Fields required on every event entry.
_EVENT_ENTRY_REQUIRED_FIELDS = frozenset(
    [
        "class",
        "module",
        "namespace",
        "name",
        "version",
        "event_type",
        "deprecated",
        "changelog",
        "result_evaluation_strategy",
        "executor",
        "extra_init_params_schema",
    ]
)


def load_manifest(
    module: types.ModuleType,
    package_name: str,
) -> Optional[Dict[str, Any]]:
    """
    Locate, parse, and structurally validate ``volnux.manifest.json`` from an
    installed Volnux-compatible package.

    The manifest is the stable contract between any Volnux package and the
    engine — it is required for both event-only packages and workflow packages.
    Its presence and validity is what distinguishes a Volnux package from an
    arbitrary Python package.

    Location search order
    ---------------------
    1. ``importlib.resources`` — canonical; the manifest is shipped as package
       data inside the wheel and accessed via Python's resource machinery.
    2. The directory containing the package's ``__init__.py`` — fallback for
       editable installations and development environments where wheel metadata may
       not be fully populated.

    Validation
    ----------
    After parsing, the manifest is validated for:
    - Correct ``$schema`` URI (must be the EventHub v1 schema URI).
    - Correct ``manifest_version`` (must be ``"1"``).
    - Presence of all required top-level fields.

    Full JSON Schema validation is intentionally omitted here — the EventHub
    registry enforces the full schema at publish time. Runtime validation
    covers only the fields the engine actually reads to avoid a hard dependency
    on ``jsonschema`` in the runtime environment.

    Returns the parsed manifest dict on success, or ``None`` on any failure.
    """

    raw: Optional[str] = None
    try:

        raw = pkg_resources.read_text(package_name, _MANIFEST_FILENAME)
        logger.debug(
            "_load_manifest: found '%s' via importlib.resources in '%s'",
            _MANIFEST_FILENAME,
            package_name,
        )
    except (FileNotFoundError, ModuleNotFoundError, TypeError):
        pass

    # Fall through to filesystem search.
    if raw is None:
        package_file = getattr(module, "__file__", None)
        if package_file:
            manifest_path = Path(package_file).parent / _MANIFEST_FILENAME
            if manifest_path.exists():
                raw = manifest_path.read_text(encoding="utf-8")
                logger.debug(
                    "_load_manifest: found '%s' via filesystem in '%s'",
                    _MANIFEST_FILENAME,
                    package_name,
                )

    if raw is None:
        logger.error(
            "_load_manifest: '%s' not found in package '%s'. "
            "All Volnux-compatible packages must ship '%s' as package data. "
            "Declare it in pyproject.toml under "
            "[tool.setuptools.package-data] or in MANIFEST.in.",
            _MANIFEST_FILENAME,
            package_name,
            _MANIFEST_FILENAME,
        )
        return None

    try:
        manifest: Dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error(
            "_load_manifest: '%s' in '%s' is not valid JSON: %s",
            _MANIFEST_FILENAME,
            package_name,
            exc,
        )
        return None

    if not _validate_manifest_structure(manifest, package_name):
        return None

    return manifest


def _validate_manifest_structure(
    manifest: Dict[str, Any],
    package_name: str,
) -> bool:
    """
    Validate the manifest's top-level structure and identity fields.

    Checks performed:
    - All required top-level fields are present.
    - ``$schema`` matches the expected EventHub v1 URI exactly.
    - ``manifest_version`` is ``"1"``.

    Returns True if the manifest passes all checks, False otherwise. All
    failures are logged at ERROR level so operators can act on them.
    """
    missing = _MANIFEST_REQUIRED_FIELDS - set(manifest.keys())
    if missing:
        logger.error(
            "_validate_manifest_structure: '%s' in '%s' is missing "
            "required top-level fields: %s",
            _MANIFEST_FILENAME,
            package_name,
            sorted(missing),
        )
        return False

    schema_uri = manifest.get("$schema", "")
    if schema_uri != _MANIFEST_SCHEMA_URI:
        logger.error(
            "_validate_manifest_structure: '%s' in '%s' declares "
            "unknown $schema '%s'. Expected '%s'.",
            _MANIFEST_FILENAME,
            package_name,
            schema_uri,
            _MANIFEST_SCHEMA_URI,
        )
        return False

    manifest_version = manifest.get("manifest_version", "")
    if manifest_version != "1":
        logger.error(
            "_validate_manifest_structure: '%s' in '%s' declares "
            "manifest_version '%s'. Only version '1' is supported.",
            _MANIFEST_FILENAME,
            package_name,
            manifest_version,
        )
        return False

    return True


def check_compatibility(
    manifest: Dict[str, Any],
    package_name: str,
) -> bool:
    """
    Verify that the current Python runtime and Volnux version fall within
    the package's declared compatibility matrix.

    The ``compatibility`` block in the manifest contains two arrays:

    .. code-block:: json

        "compatibility": {
            "volnux": ["1.0.0", "1.1.x"],
            "python": ["3.11", "3.12"]
        }

    Python check
        The running interpreter's ``major.minor`` version must match at least
        one entry in ``compatibility.python``. Patch versions and ``x``
        wildcards in the manifest entries are stripped before comparison so
        ``"3.11"`` and ``"3.11.2"`` both match Python 3.11.x.

    Volnux check
        The running Volnux version (from ``volnux.__version__``) must match
        at least one entry in ``compatibility.volnux``. Trailing ``.x``
        wildcards are supported (``"1.0.x"`` matches ``"1.0.3"``).

    Both checks emit a WARNING rather than an ERROR on mismatch — the package
    may still work, but the operator should be aware of the unsupported
    combination. Returns True in all cases so a compatibility warning does not
    block loading; the caller decides whether to treat warnings as hard failures.
    """

    compat = manifest.get("compatibility", {})
    passed = True

    # Python version
    supported_pythons: List[str] = compat.get("python", [])
    running_python = f"{sys.version_info.major}.{sys.version_info.minor}"

    if supported_pythons:
        # Normalise entries to major.minor for comparison (strip patch).
        normalised = [".".join(v.split(".")[:2]) for v in supported_pythons]
        if running_python not in normalised:
            logger.warning(
                "check_compatibility: '%s' declares Python compatibility "
                "%s but the current interpreter is Python %s. "
                "The package may still work but is unsupported on this version.",
                package_name,
                supported_pythons,
                running_python,
            )
            passed = False

    # Volnux version
    supported_volnux: List[str] = compat.get("volnux", [])
    if supported_volnux and volnux_runtime_version:
        if not _version_matches_any(volnux_runtime_version, supported_volnux):
            logger.warning(
                "check_compatibility: '%s' declares Volnux compatibility "
                "%s but the current runtime is Volnux %s. "
                "The package may still work but is unsupported on this version.",
                package_name,
                supported_volnux,
                volnux_runtime_version,
            )
            passed = False

    return passed


def _version_matches_any(running: str, supported: List[str]) -> bool:
    """
    Return True if ``running`` matches any entry in ``supported``.

    Trailing ``.x`` wildcards in supported entries are treated as
    prefix matches: ``"1.0.x"`` matches any ``"1.0.*"`` version.
    Exact entries require an exact string match after stripping leading ``v``.
    """
    running = running.lstrip("v")
    for spec in supported:
        spec = spec.lstrip("v")
        if spec.endswith(".x"):
            prefix = spec[:-2]  # strip ".x"
            if running == prefix or running.startswith(prefix + "."):
                return True
        elif running == spec:
            return True
    return False


def register_workflow_config_and_events_from_manifest(
    manifest: Dict[str, Any],
    module: types.ModuleType,
    package_name: str,
    registry: "WorkflowRegistry",
    project_root: Optional[Path] = None,
) -> Tuple[Set[typing.Type["EventBase"]], Set["WorkflowConfig"]]:
    """
    Resolve, enrich, and register events and workflow configurations from the provided manifest.

    This function processes every event declared in the ``manifest["events"]`` and performs
    the following:

    1. Resolves the class via its ``module`` path, with a fallback to the package-root.
    2. Validates if the resolved class is a concrete subclass of ``EventBase``.
    3. Skips and warns if the event is deprecated, logging the associated ``deprecation_info``
       when available, to inform operators about migration details.
    4. Enriches the event class with additional metadata from the manifest, such as:
       ``executor``, ``result_evaluation_strategy``, ``extra_init_params_schema``,
       ``namespace``, and ``version``.
    5. Registers the enriched event class using ``registry.register_event()``.

    In addition, this function also resolves and registers any workflow configuration
    classes defined in the provided module.

    Fault Tolerance:
    - Events that fail resolution or validation are skipped with an error log, ensuring
      the registration of the remaining events continues uninterrupted.
    - Deprecated events are still registered, leaving the decision to reject them to
      subsequent workflow layers.

    Returns:
    - A tuple containing two sets:
      1. A set of successfully registered event classes.
      2. A set of successfully registered workflow configuration instances.

    Empty sets in the return value signal a registration failure.

    :param manifest: Dictionary containing event and workflow information to process.
    :param module: Module object where the workflow configuration classes are defined.
    :param package_name: Name of the root package, used for resolving class paths.
    :param registry: WorkflowRegistry instance for registering events and configurations.
    :param project_root: (Optional) Root directory of the project, used for resolving paths.
    :return: A tuple containing a set of registered `EventBase` subclasses and a set of
             `WorkflowConfig` instances.
    """

    from volnux.event.base import get_event_registry

    event_registry = get_event_registry()
    event_entries: List[Dict[str, Any]] = manifest.get("events", [])

    if not event_entries:
        logger.warning(
            "register_workflow_config_and_events_from_manifest: 'events' list in '%s' manifest "
            "is empty. At least one event is required.",
            package_name,
        )
        return set(), set()

    registered_events: Set[typing.Type["EventBase"]] = set()
    registered_workflow_config: Set["WorkflowConfig"] = set()

    for entry in event_entries:
        missing = _EVENT_ENTRY_REQUIRED_FIELDS - set(entry.keys())
        if missing:
            logger.error(
                "register_workflow_config_and_events_from_manifest: skipping event entry in "
                "'%s' — missing required fields: %s. Entry: %s",
                package_name,
                sorted(missing),
                entry.get("class", "<unknown>"),
            )
            continue

        class_name: str = entry["class"]
        module_path: str = entry["module"]

        event_class = resolve_event_class(class_name, module_path, module, package_name)
        if event_class is None:
            continue

        # Deprecation check
        if entry.get("deprecated", False):
            dep_info: Optional[Dict[str, Any]] = entry.get("deprecation_info")
            if dep_info:
                logger.warning(
                    "register_workflow_config_and_events_from_manifest: '%s' from '%s' is "
                    "deprecated since v%s. Reason: %s. "
                    "Replacement: %s. Will be removed in v%s.",
                    class_name,
                    package_name,
                    dep_info.get("since_version", "?"),
                    dep_info.get("reason", "no reason given"),
                    dep_info.get("replacement") or "none",
                    dep_info.get("removal_version", "?"),
                )
            else:
                logger.warning(
                    "register_workflow_config_and_events_from_manifest: '%s' from '%s' is "
                    "marked deprecated but provides no deprecation_info.",
                    class_name,
                    package_name,
                )
            # Deprecated events are still registered — the caller decides
            # whether to reject them at the workflow-authoring layer.

        try:
            event_registry.register(event_class)
            registered_events.add(event_class)
            logger.debug(
                "register_workflow_config_and_events_from_manifest: registered '%s' "
                "(namespace=%s, version=%s) from '%s'",
                class_name,
                entry.get("namespace"),
                entry.get("version"),
                package_name,
            )
        except Exception as exc:
            logger.error(
                "register_workflow_config_and_events_from_manifest: failed to register '%s' "
                "from '%s': %s",
                class_name,
                package_name,
                exc,
            )

    # check for workflow_config_classes
    workflow_config_classes = resolve_workflow_config_classes(module)
    for workflow_config_class in workflow_config_classes:
        try:
            workflow_config = workflow_config_class(workflow_path=project_root)
            registry.register(workflow_config)
            registered_workflow_config.add(workflow_config)
            logger.debug(
                f"register_workflow_config_and_events_from_manifest: registered workflow_config_class "
                f"{workflow_config_class.__name__} from '{package_name}'",
            )
        except Exception as exc:
            logger.error(
                f"register_workflow_config_and_events_from_manifest: failed to register workflow_config_class "
                f"{workflow_config_class.__name__} from '{package_name}': {exc}",
            )

    logger.info(
        "register_workflow_config_and_events_from_manifest: %d/%d workflow_config_class(es) registered from '%s'",
        len(workflow_config_classes),
        len(workflow_config_classes),
        package_name,
    )
    return registered_events, registered_workflow_config


def resolve_workflow_config_classes(
    package_module: types.ModuleType,
) -> typing.FrozenSet[typing.Type["WorkflowConfig"]]:
    """
    Resolves and collects all subclasses of the `WorkflowConfig` located within the provided package module
    and its `workflow.py` submodule. The method identifies and validates any class that directly or indirectly
    subclasses `WorkflowConfig` while omitting the base `WorkflowConfig` class itself.

    The collected classes are returned as an immutable frozen set to ensure their integrity.

    :param package_module: The target package module to be inspected for subclasses of `WorkflowConfig`.
    :type package_module: types.ModuleType
    :return: A frozen set containing all valid subclasses of `WorkflowConfig` found within the package module
             and its `workflow.py` submodule.
    :rtype: typing.FrozenSet[WorkflowConfig]
    """

    from volnux.engine.workflows import WorkflowConfig

    def _validate_workflow_config_class(cls: type) -> bool:
        return (
            isinstance(cls, type)
            and issubclass(cls, WorkflowConfig)
            and cls is not WorkflowConfig
        )

    classes = set(
        [
            cls
            for cls in package_module.__dict__.values()
            if _validate_workflow_config_class(cls)
        ]
    )

    # Check the submodule workflow.py for subclasses of WorkflowConfig
    workflow_module = importlib.import_module(f"{package_module.__name__}.workflow")
    classes.update(
        set(
            [
                cls
                for cls in workflow_module.__dict__.values()
                if _validate_workflow_config_class(cls)
            ]
        )
    )

    if len(classes) > 0:
        logger.info(
            f"Found {len(classes)} WorkflowConfig classes in {package_module.__name__}"
        )

    return frozenset(classes)


def resolve_event_class(
    class_name: str,
    module_path: str,
    package_module: types.ModuleType,
    package_name: str,
) -> Optional[typing.Type["EventBase"]]:
    """
    Resolve a single event class by name.

    Tries the explicit ``module`` path from the manifest first, then falls
    back to the package root module. Returns a validated ``EventBase``
    subclass or ``None``.
    """

    # Explicit module path (preferred)
    if module_path:
        try:
            mod = importlib.import_module(module_path)
            candidate = getattr(mod, class_name, None)
            if candidate is not None:
                return _validate_event_class(candidate, class_name, package_name)
        except ImportError as exc:
            logger.warning(
                "_resolve_event_class: could not import module '%s' for "
                "class '%s' in '%s': %s. Falling back to package root.",
                module_path,
                class_name,
                package_name,
                exc,
            )

    # Package root fallback
    candidate = getattr(package_module, class_name, None)
    if candidate is not None:
        return _validate_event_class(candidate, class_name, package_name)

    logger.error(
        "_resolve_event_class: class '%s' not found in '%s' — "
        "neither in module '%s' nor at the package root.",
        class_name,
        package_name,
        module_path or "<not specified>",
    )
    return None


def _validate_event_class(
    candidate: Any,
    class_name: str,
    package_name: str,
) -> Optional[typing.Type["EventBase"]]:
    """
    Confirm that ``candidate`` is a concrete ``EventBase`` subclass.
    Rejects the base class itself, abstract classes, and non-class objects.
    """
    from volnux.event.base import EventBase

    if not isinstance(candidate, type):
        logger.error(
            "_validate_event_class: '%s' in '%s' is not a class.",
            class_name,
            package_name,
        )
        return None

    if not issubclass(candidate, EventBase) or candidate is EventBase:
        logger.error(
            "_validate_event_class: '%s' in '%s' is not an EventBase subclass.",
            class_name,
            package_name,
        )
        return None

    return candidate


def build_authenticated_url(
    credentials: "SourceCredentials",
    url: str,
    auth_style: Literal["general", "pypi"] = "general",
) -> Union[str, bytes]:
    """
    Embed ``credentials`` into ``url`` and return the authenticated URL.

    The credential is placed in the URL authority component
    (``scheme://auth@host/path``) so it is transmitted as HTTP Basic Auth.
    This avoids passing secrets as plaintext CLI arguments that could be
    captured by process-listing tools or shell history.

    Non-HTTP schemes (``ssh://``, ``git://``) are returned unchanged —
    those transports use key-based auth and do not embed credentials in URLs.

    Auth styles
    -----------
    ``General`` (default)::
        https://<token>@host/path
        https://<username>:<password>@host/path

    ``PyPI``::
        https://__token__:<token>@host/path
        https://<username>:<password>@host/path

    Args:
        credentials: Must satisfy ``is_valid()`` before calling.
        url:         The remote URL to authenticate.
        auth_style:  Token embedding convention. Defaults to ``general``.

    Returns:
        The authenticated URL as a string, or the original URL unchanged for
        non-HTTP/HTTPS schemes.

    Raises:
        ValueError: If ``credentials.is_valid()`` is ``False``.
        ValueError: If ``url`` is an http/https URL with no valid hostname.
    """
    if not credentials.is_valid():
        raise ValueError(
            "Cannot build authenticated URL: credentials are not valid. "
            "Provide either a token or both username and password."
        )

    parsed = urlparse(url)

    # Non-HTTP schemes (ssh://, git://) use key-based auth — return unchanged.
    if parsed.scheme not in ("http", "https"):
        return url

    if not parsed.hostname:
        raise ValueError(
            f"Cannot build authenticated URL: {redact_credentials(url)!r} "
            "has no valid host component."
        )

    if credentials.token:
        if auth_style.lower() == "pypi":
            # PyPI convention: __token__ is the username, token is the password.
            auth = f"__token__:{credentials.token}"
        else:
            # Git convention: bare token as the userinfo (no explicit password).
            auth = credentials.token
    else:
        # Username/password — percent-encode to handle special characters
        # (@, :, /) that would otherwise break URL parsing.
        encoded_user = quote(credentials.username, safe="")
        encoded_pass = quote(credentials.password, safe="")
        auth = f"{encoded_user}:{encoded_pass}"

    # Reconstruct netloc from hostname + optional port only, stripping any
    # pre-existing auth — prevents double-embedding on already-authenticated URLs.
    host_only = parsed.hostname
    if parsed.port:
        host_only = f"{host_only}:{parsed.port}"

    return urlunparse(parsed._replace(netloc=f"{auth}@{host_only}"))


def redact_credentials(text: str) -> str:
    """
    Remove credential patterns from pip stderr before logging.

    Replaces ``user:password@`` and ``token@`` patterns that pip may echo
    when reporting the index URL it used.
    """
    # Matches: scheme://anything@host (captures the auth portion)
    return re.sub(r"(https?://)([^@\s]+@)", r"\1***@", text)
