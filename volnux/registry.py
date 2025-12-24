import logging
import sys
import threading
import typing
import warnings
from dataclasses import dataclass, field
from collections import defaultdict

from volnux.result import ResultSet

logger = logging.getLogger(__name__)


class RegistryNotReady(Exception):
    """The registry isn't fully populated yet."""

    pass


@dataclass
class RegistryEntry:
    """The registry entry."""

    name: str  # custom name
    module_path: str
    handler: typing.Any
    handler_name: str
    namespace: str
    extra: typing.Dict[str, typing.Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash((self.module_path, self.name, self.namespace, self.handler_name))


class Registry:
    """
    A registry that stores event classes similar to Django's Apps registry.

    Inspired by django.apps.registry.Apps - stores event classes organized
    by module labels and provides thread-safe registration.
    """

    def __init__(self) -> None:
        self._handler_registry: ResultSet[RegistryEntry] = ResultSet()

        # Thread lock for thread-safe operations
        self._lock = threading.RLock()

        # Ready state
        self.ready = False

        # Process ID for debugging
        self._process_id = sys.modules[__name__]

    def register(
        self,
        klass: typing.Type[typing.Any],
        name: typing.Optional[str] = None,
        namespace: typing.Optional[str] = "local",
        **extra: typing.Any,
    ) -> None:
        """
        Register for classes.

        Args:
            klass: The event class to register
            name: Optional name for the class
            namespace: Optional namespace for the class
            extra: Optional extra arguments
        Raises:
            RuntimeWarning: If event already registered (same module/name)
            RuntimeError: If conflicting event found (different module, same name)
        """
        with self._lock:
            module_label = klass.__module__
            klass_name = klass.__name__
            name = name or klass_name

            entry_qs = self._handler_registry.filter(name=name, namespace=namespace)

            existing_entry = typing.cast(RegistryEntry, entry_qs.first())
            if existing_entry:
                # Same class re-registered - warn but allow
                if (
                    klass.__name__ == existing_entry.handler.__name__
                    and klass.__module__ == existing_entry.module_path
                ):
                    warnings.warn(
                        f"Class '{module_label}.{klass_name}' was already registered. "
                        f"Reloading events is not advised as it can lead to inconsistencies.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                else:
                    raise RuntimeError(
                        f"Conflicting '{klass_name}' class in module '{module_label}': "
                        f"{existing_entry.handler} and {klass}."
                    )

            entry = RegistryEntry(
                name=name,
                module_path=module_label,
                handler=klass,
                handler_name=klass_name,
                namespace=namespace,
                extra=extra,
            )

            self._handler_registry.add(entry)

            if not self.ready:
                self.set_ready()

            logger.debug(f"Registered: {module_label}.{klass_name}")

    def get_class(self, module_label: str, klass_name: str) -> typing.Type[typing.Any]:
        """
        Get an event class by module label and event name.

        Args:
            module_label: The module path (e.g., 'myapp.events')
            klass_name: The event class name

        Returns:
            The event class

        Raises:
            LookupError: If event not found
        """
        entry_qs = self._handler_registry.filter(
            module_label=module_label, handler_name=klass_name
        )
        if entry_qs.is_empty():
            raise LookupError(f"Class '{module_label}.{klass_name}' not registered.")
        entry = typing.cast(RegistryEntry, entry_qs.first())
        return entry.handler

    def get_by_name(
        self, klass_name: str, namespace: str = "local"
    ) -> typing.Optional[typing.Type[typing.Any]]:
        """
        Fast lookup by event name only (without module).

        Args:
            klass_name: The class name
            namespace: The namespace

        Returns:
            The event class or None if not found
        Raises:
            MultiValueError: If multiple class have the same name
        """
        try:
            entry_qs = self._handler_registry.get(name=klass_name, namespace=namespace)
            entry = typing.cast(RegistryEntry, entry_qs)
            return entry.handler
        except KeyError:
            return None

    def get_classes_for_module(
        self, module_label: str
    ) -> typing.Dict[str, typing.Type[typing.Any]]:
        """
        Get all classes registered under a specific module.

        Args:
            module_label: The module path

        Returns:
            Dictionary of event_name => event_class
        """
        class_dict = {}
        for entry in self._handler_registry.filter(module_path=module_label):
            entry = typing.cast(RegistryEntry, entry)
            class_dict[entry.handler_name] = entry.handler

        return class_dict

    def list_classes_names(self) -> typing.List[str]:
        """List all registered class names."""
        return sorted(
            [
                entry.handler_name
                for entry in typing.cast(
                    typing.Set[RegistryEntry], self._handler_registry
                )
            ]
        )

    def list_all_classes(self) -> typing.FrozenSet[typing.Type[typing.Any]]:
        """List all classes with the registry"""
        return frozenset(
            [
                entry.handler
                for entry in typing.cast(
                    typing.Set[RegistryEntry], self._handler_registry
                )
            ]
        )

    def list_modules(self) -> typing.List[str]:
        """List all modules that have registered classes."""
        return sorted(
            [
                entry.module_path
                for entry in typing.cast(
                    typing.Set[RegistryEntry], self._handler_registry
                )
            ]
        )

    def is_registered(self, klass_name: str) -> bool:
        """Check if a class is registered."""
        return not self._handler_registry.filter(handler_name=klass_name).is_empty()

    def check_events_ready(self) -> None:
        """
        Raise an exception if events aren't ready yet.
        """
        if not self.ready:
            raise RegistryNotReady("Registry isn't ready yet.")

    def set_ready(self) -> None:
        """Mark the registry as populated and ready."""
        self.ready = True

    def clear(self) -> None:
        """
        Completely clear the registry.
        """
        with self._lock:
            self._handler_registry.clear()
            self.ready = False
