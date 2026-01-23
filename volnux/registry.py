import logging
import sys
import threading
import typing
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from packaging.version import Version, parse as parse_version

from volnux.result import ResultSet
from volnux.versioning import DeprecationInfo, BaseVersioning

logger = logging.getLogger(__name__)


class RegistryNotReady(Exception):
    """The registry isn't fully populated yet."""

    pass


@dataclass
class RegistryEntry:
    """The registry entry with version support."""

    name: str  # custom name (e.g., "DataProcessEvent")
    module_path: str
    handler: typing.Any
    handler_name: str
    namespace: str
    version: str = "1.0.0"
    deprecated: bool = False
    deprecation_info: typing.Optional[DeprecationInfo] = None
    changelog: typing.Optional[str] = None
    scheme_handler: typing.Optional[BaseVersioning] = None
    registered_at: datetime = field(default_factory=datetime.now)
    extra: typing.Dict[str, typing.Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(
            (
                self.module_path,
                self.name,
                self.namespace,
                self.handler_name,
                self.version,
            )
        )

    @property
    def version_obj(self) -> Version:
        """Parse version string to Version object for comparison"""
        return parse_version(self.version)

    @property
    def fully_qualified_name(self) -> str:
        """Get a fully qualified name with version"""
        return f"{self.namespace}::{self.name}@{self.version}"

    def is_compatible_with(self, version_constraint: str) -> bool:
        """
        Check if this version satisfies the requested version constraint.

        Supports:
        - Exact: "2.1.0"
        - Caret: "^2.0.0" (>=2.0.0 <3.0.0)
        - Tilde: "~1.2.3" (>=1.2.3 <1.3.0)
        """
        if version_constraint.startswith("^"):
            # ^2.0.0 means >=2.0.0 <3.0.0 (same major version)
            base_version = parse_version(version_constraint[1:])
            return (
                self.version_obj.major == base_version.major
                and self.version_obj >= base_version
            )

        elif version_constraint.startswith("~"):
            # ~1.2.3 means >=1.2.3 <1.3.0 (same major.minor)
            base_version = parse_version(version_constraint[1:])
            return (
                self.version_obj.major == base_version.major
                and self.version_obj.minor == base_version.minor
                and self.version_obj >= base_version
            )

        else:
            # Exact match
            return self.version == version_constraint


class Registry:

    def __init__(self) -> None:
        self._handler_registry: ResultSet[RegistryEntry] = ResultSet()

        self._lock = threading.RLock()
        self.ready = False

        # Process ID for debugging
        self._process_id = sys.modules[__name__]

    def register(
        self,
        klass: typing.Type[typing.Any],
        name: typing.Optional[str] = None,
        namespace: typing.Optional[str] = "local",
        version: str = "1.0.0",
        changelog: typing.Optional[str] = None,
        deprecated: bool = False,
        deprecation_info: typing.Optional[DeprecationInfo] = None,
        scheme_handler: typing.Optional[BaseVersioning] = None,
        **extra: typing.Any,
    ) -> None:
        """
        Register event classes with version support.

        Args:
            klass: The event class to register
            name: Optional name for the class
            namespace: Optional namespace for the class
            version: Semantic version string (e.g., "1.0.0", "2.1.3")
            changelog: Optional description of changes in this version
            deprecated: Whether this version is deprecated
            deprecation_info: Additional deprecation details
            scheme_handler: Optional schema handler for this version
            extra: Optional extra arguments

        Raises:
            ValueError: If a version is invalid
            RuntimeWarning: If event already registered
            RuntimeError: If conflicting event found
        """
        with self._lock:
            # try:
            #     parse_version(version)
            # except Exception as e:
            #     raise ValueError(f"Invalid version format '{version}': {e}")

            if scheme_handler is not None and not scheme_handler.validate_version(version):
                raise ValueError(f"Invalid version format '{version}': {scheme_handler}")

            module_label = klass.__module__
            klass_name = klass.__name__
            name = name or klass_name

            # Check if this exact version already exists using queryset
            existing_qs = self._handler_registry.filter(
                name=name, namespace=namespace, version=version
            )

            if not existing_qs.is_empty():
                existing_entry = typing.cast(RegistryEntry, existing_qs.first())

                # Same class re-registered - warn but allow
                if (
                    klass.__name__ == existing_entry.handler.__name__
                    and klass.__module__ == existing_entry.module_path
                ):
                    warnings.warn(
                        f"Class '{module_label}.{klass_name}@{version}' was already registered. "
                        f"Reloading events is not advised as it can lead to inconsistencies.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    return  # Don't re-register
                else:
                    raise RuntimeError(
                        f"Conflicting '{klass_name}@{version}' in '{module_label}': "
                        f"{existing_entry.handler} vs {klass}."
                    )

            # Create entry
            entry = RegistryEntry(
                name=name,
                module_path=module_label,
                handler=klass,
                handler_name=klass_name,
                namespace=namespace,
                version=version,
                deprecated=deprecated,
                deprecation_info=deprecation_info,
                changelog=changelog,
                scheme_handler=scheme_handler,
                extra=extra,
            )

            self._handler_registry.add(entry)

            if not self.ready:
                self.set_ready()

            logger.debug(f"Registered: {module_label}.{klass_name}@{version}")

    def get_by_name(
        self,
        klass_name: str,
        namespace: str = "local",
        version: typing.Optional[str] = None,
    ) -> typing.Optional[typing.Type[typing.Any]]:
        """
        Fast lookup by event name.

        Args:
            klass_name: The class name
            namespace: The namespace
            version: Optional version (e.g., "2.1.0", "^2.0.0", None for latest)

        Returns:
            The event class or None if not found

        Examples:
            # Get latest version
            get_by_name("DataProcessEvent")

            # Get specific version
            get_by_name("DataProcessEvent", version="2.1.0")

            # Get compatible version (any 2.x.x)
            get_by_name("DataProcessEvent", version="^2.0.0")
        """
        entry = self.get_entry(klass_name, namespace, version)
        return entry.handler if entry else None

    def get_entry(
        self,
        klass_name: str,
        namespace: str = "local",
        version: typing.Optional[str] = None,
    ) -> typing.Optional[RegistryEntry]:
        """
        Get full registry entry.

        Args:
            klass_name: Event name
            namespace: Namespace
            version: Optional version constraint

        Returns:
            RegistryEntry or None
        """
        with self._lock:
            if version is None:
                return self._get_latest_version(klass_name, namespace)

            elif version.startswith("^") or version.startswith("~"):
                return self._resolve_version_range(klass_name, namespace, version)

            else:
                try:
                    entry = self._handler_registry.get(
                        name=klass_name, namespace=namespace, version=version
                    )
                    return typing.cast(RegistryEntry, entry)
                except KeyError:
                    return None

    def _get_latest_version(
        self, klass_name: str, namespace: str
    ) -> typing.Optional[RegistryEntry]:
        """
        Get the latest version of an event using queryset filtering.
        Args:
            klass_name: The event name
            namespace: Namespace
        Returns:
            RegistryEntry or None
        """
        versions_qs = self._handler_registry.filter(
            name=klass_name, namespace=namespace
        )

        if versions_qs.is_empty():
            return None

        all_entries = typing.cast(typing.Set[RegistryEntry], versions_qs)
        latest_entry = max(all_entries, key=lambda e: e.version_obj)

        return latest_entry

    def _resolve_version_range(
        self, klass_name: str, namespace: str, version_constraint: str
    ) -> typing.Optional[RegistryEntry]:
        """
        Resolve a version range constraint to the best matching version.

        Args:
            klass_name: The event name
            namespace: Namespace
            version_constraint: Version constraint

        Returns:
            RegistryEntry or None

        Examples:
            "^2.0.0" -> Any 2.x.x version (highest available)
            "~1.2.3" -> Any 1.2.x version >= 1.2.3
        """
        # Get all versions for this event using queryset
        all_versions_qs = self._handler_registry.filter(
            name=klass_name, namespace=namespace
        )

        if all_versions_qs.is_empty():
            return None

        compatible_versions = [
            entry
            for entry in typing.cast(typing.Set[RegistryEntry], all_versions_qs)
            if entry.is_compatible_with(version_constraint)
        ]

        if not compatible_versions:
            return None

        # Return highest compatible version
        best_match = max(compatible_versions, key=lambda e: e.version_obj)
        return best_match

    def list_versions(
        self, klass_name: str, namespace: str = "local"
    ) -> typing.List[str]:
        """
        List all available versions of an event.

        Returns:
            Sorted list of version strings (newest first)
        """
        versions_qs = self._handler_registry.filter(
            name=klass_name, namespace=namespace
        )

        versions = [typing.cast(RegistryEntry, entry).version for entry in versions_qs]

        # Sort by semantic version (newest first)
        return sorted(versions, key=parse_version, reverse=True)

    def get_latest_version(
        self, klass_name: str, namespace: str = "local"
    ) -> typing.Optional[str]:
        """Get the latest version string for an event."""
        latest_entry = self._get_latest_version(klass_name, namespace)
        return latest_entry.version if latest_entry else None

    def deprecate_version(
        self,
        klass_name: str,
        version: str,
        namespace: str = "local",
        reason: typing.Optional[str] = None,
        end_of_life: typing.Optional[datetime] = None,
        migration_guide: typing.Optional[str] = None,
    ) -> None:
        """
        Mark a specific version as deprecated.

        Args:
            klass_name: Event name
            version: Version to deprecate
            namespace: Namespace
            reason: Why it's deprecated
            end_of_life: When it will be removed
            migration_guide: URL or text for migration
        """
        with self._lock:
            try:
                entry = self._handler_registry.get(
                    name=klass_name, namespace=namespace, version=version
                )
                entry = typing.cast(RegistryEntry, entry)

                # Modify the entry in place
                entry.deprecated = True
                entry.deprecation_info = {
                    "reason": reason,
                    "end_of_life": end_of_life.isoformat() if end_of_life else None,
                    "migration_guide": migration_guide,
                    "deprecated_at": datetime.now().isoformat(),
                }

                logger.warning(
                    f"Version {version} of {namespace}::{klass_name} has been deprecated. "
                    f"Reason: {reason}"
                )
            except KeyError:
                raise LookupError(
                    f"Version {version} of {klass_name} not found in {namespace}"
                )

    def is_deprecated(
        self,
        klass_name: str,
        version: typing.Optional[str] = None,
        namespace: str = "local",
    ) -> bool:
        """Check if a version is deprecated."""
        entry = self.get_entry(klass_name, namespace, version)
        return entry.deprecated if entry else False

    def get_deprecation_info(
        self,
        klass_name: str,
        version: str,
        namespace: str = "local",
    ) -> typing.Optional[typing.Dict[str, typing.Any]]:
        """Get deprecation information for a version."""
        entry = self.get_entry(klass_name, namespace, version)
        return entry.deprecation_info if entry else None

    def get_class(self, module_label: str, klass_name: str) -> typing.Type[typing.Any]:
        """
        Get an event class by module label and event name (latest version).

        Args:
            module_label: The module path (e.g., 'myapp.events')
            klass_name: The event class name

        Returns:
            The event class

        Raises:
            LookupError: If event not found
        """
        entry_qs = self._handler_registry.filter(
            module_path=module_label, handler_name=klass_name
        )

        if entry_qs.is_empty():
            raise LookupError(f"Class '{module_label}.{klass_name}' not registered.")

        # Get latest version if multiple exists
        all_entries = typing.cast(typing.List[RegistryEntry], list(entry_qs))
        latest_entry = max(all_entries, key=lambda e: e.version_obj)

        return latest_entry.handler

    def get_classes_for_module(
        self, module_label: str
    ) -> typing.Dict[str, typing.Type[typing.Any]]:
        """
        Get all classes registered under a specific module (latest versions only).

        Args:
            module_label: The module path

        Returns:
            Dictionary of event_name => event_class
        """
        class_dict = {}
        entries_qs = self._handler_registry.filter(module_path=module_label)

        by_name = defaultdict(list)

        for entry in entries_qs:
            entry = typing.cast(RegistryEntry, entry)
            by_name[entry.handler_name].append(entry)

        # For each name, pick the latest version
        for handler_name, entries in by_name.items():
            latest = max(entries, key=lambda e: e.version_obj)
            class_dict[handler_name] = latest.handler

        return class_dict

    def list_classes_names(self) -> typing.List[str]:
        """List all registered class names (unique, regardless of version)."""
        all_entries = typing.cast(typing.Set[RegistryEntry], self._handler_registry)

        # Get unique handler names
        names = set(entry.handler_name for entry in all_entries)
        return sorted(names)

    def list_all_classes(self) -> typing.FrozenSet[typing.Type[typing.Any]]:
        """List all classes in the registry (all versions)."""
        return frozenset(
            entry.handler
            for entry in typing.cast(typing.Set[RegistryEntry], self._handler_registry)
        )

    def list_modules(self) -> typing.List[str]:
        """List all modules that have registered classes."""
        all_entries = typing.cast(typing.Set[RegistryEntry], self._handler_registry)

        # Get unique module paths
        modules = set(entry.module_path for entry in all_entries)
        return sorted(modules)

    def is_registered(
        self,
        klass_name: str,
        namespace: str = "local",
        version: typing.Optional[str] = None,
    ) -> bool:
        """
        Check if a class is registered.

        Args:
            klass_name: Class name
            namespace: Namespace
            version: Optional version (checks latest if None)
        """
        if version:
            # Check specific version
            entry_qs = self._handler_registry.filter(
                name=klass_name, namespace=namespace, version=version
            )
        else:
            # Check if any version exists
            entry_qs = self._handler_registry.filter(
                name=klass_name, namespace=namespace
            )

        return not entry_qs.is_empty()

    def check_events_ready(self) -> None:
        """Raise an exception if events aren't ready yet."""
        if not self.ready:
            raise RegistryNotReady("Registry isn't ready yet.")

    def set_ready(self) -> None:
        """Mark the registry as populated and ready."""
        self.ready = True

    def clear(self) -> None:
        """Completely clear the registry."""
        with self._lock:
            self._handler_registry.clear()
            self.ready = False
