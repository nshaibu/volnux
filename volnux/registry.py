import sys
import logging
import threading
import warnings
import typing
from collections import defaultdict


logger = logging.getLogger(__name__)


class RegistryNotReady(Exception):
    """registry isn't fully populated yet."""

    pass


class Registry:
    """
    A registry that stores event classes similar to Django's Apps registry.

    Inspired by django.apps.registry.Apps - stores event classes organized
    by module labels and provides thread-safe registration.
    """

    def __init__(self) -> None:
        # {'myapp.events': {'UserLoginEvent': UserLoginEvent}}
        self.all_classes: typing.Dict[
            str, typing.Dict[str, typing.Type[typing.Any]]
        ] = defaultdict(dict)

        # Direct name-to-class mapping {'UserLoginEvent': UserLoginEvent}
        self._name_registry: typing.Dict[str, typing.Type[typing.Any]] = {}

        # Thread lock for thread-safe operations
        self._lock = threading.RLock()

        # Ready state
        self.ready = False

        # Process ID for debugging
        self._process_id = sys.modules[__name__]

    def register(self, klass: typing.Type[typing.Any]) -> None:
        """
        Register an classes in the registry..

        Args:
            klass: The event class to register

        Raises:
            RuntimeWarning: If event already registered (same module/name)
            RuntimeError: If conflicting event found (different module, same name)
        """
        with self._lock:
            module_label = klass.__module__
            klass_name = klass.__name__
            module_classes = self.all_classes[module_label]

            # Check for duplicate registration
            if klass_name in module_classes:
                existing = module_classes[klass_name]

                # Same class re-registered - warn but allow
                if (
                    klass.__name__ == existing.__name__
                    and klass.__module__ == existing.__module__
                ):
                    warnings.warn(
                        f"Event '{module_label}.{klass_name}' was already registered. "
                        f"Reloading events is not advised as it can lead to inconsistencies.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                else:
                    raise RuntimeError(
                        f"Conflicting '{klass_name}' events in module '{module_label}': "
                        f"{existing} and {klass}."
                    )

            module_classes[klass_name] = klass
            self._name_registry[klass_name] = klass

            if not self.ready:
                self.set_ready()

            logger.info(f"Registered: {module_label}.{klass_name}")

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
        try:
            return self.all_classes[module_label][klass_name]
        except KeyError:
            raise LookupError(f"Event '{module_label}.{klass_name}' not registered.")

    def get_by_name(self, klass_name: str) -> typing.Optional[typing.Type[typing.Any]]:
        """
        Fast lookup by event name only (without module).

        Args:
            klass_name: The event class name

        Returns:
            The event class or None if not found
        """
        return self._name_registry.get(klass_name)

    def get_classes_for_module(
        self, module_label: str
    ) -> typing.Dict[str, typing.Type[typing.Any]]:
        """
        Get all events registered under a specific module.

        Args:
            module_label: The module path

        Returns:
            Dictionary of event_name => event_class
        """
        return dict(self.all_classes.get(module_label, {}))

    def list_classes_names(self) -> typing.List[str]:
        """List all registered class names."""
        return sorted(self._name_registry.keys())

    def list_all_classes(self) -> typing.FrozenSet[typing.Type[typing.Any]]:
        """List all classes with the registry"""
        return frozenset(list(self._name_registry.values()))

    def list_modules(self) -> typing.List[str]:
        """List all modules that have registered classes."""
        return sorted(self.all_classes.keys())

    def is_registered(self, klass_name: str) -> bool:
        """Check if an event is registered."""
        return klass_name in self._name_registry

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
            self.all_classes.clear()
            self._name_registry.clear()
            self.ready = False
