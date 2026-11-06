"""
Resource Provider System for Event Checkpointing

This module provides an abstract base class and utilities for implementing
resource providers that handle serialization and restoration of external
resources (like database connections, file handles, network sockets, etc.)
during event checkpointing and resumption.
"""

import abc
import logging
import typing
from typing import Any, Dict, Generic, TypeVar, Optional

__all__ = [
    "ResourceProvider",
    "ResourceProviderRegistry",
    "register_provider",
    "get_provider",
]

logger = logging.getLogger(__name__)

TResource = TypeVar("TResource")


class ResourceProvider(abc.ABC, Generic[TResource]):
    """
    Abstract base class for resource providers in the checkpointing system.

    A resource provider is responsible for:
    - Serializing the resource state during checkpointing
    - Restoring resources from the serialized state during resumption
    - (Optional) Cleaning up resources when no longer needed

    Type Parameters:
        TResource: The type of resource this provider handles

    Example:
        ```python
        class FileHandleProvider(ResourceProvider[typing.IO]):
            @classmethod
            def save_state(cls, resource: typing.IO) -> Dict[str, Any]:
                return {
                    "path": resource.name,
                    "mode": resource.mode,
                    "position": resource.tell(),
                }

            @classmethod
            def restore_state(cls, data: Dict[str, Any]) -> typing.IO:
                file_handle = open(data["path"], data["mode"])
                file_handle.seek(data["position"])
                return file_handle

            @classmethod
            def cleanup(cls, resource: typing.IO) -> None:
                if not resource.closed:
                    resource.close()
        ```
    """

    @classmethod
    @abc.abstractmethod
    def save_state(cls, resource: TResource) -> Dict[str, Any]:
        """
        Serialize the resource state to a dictionary.

        This method is called during checkpoint creation to capture the
        current state of the resource. The returned dictionary must be
        JSON-serializable (primitives, dicts, lists only).

        Args:
            resource: The resource instance to serialize

        Returns:
            Dictionary containing the serialized resource state

        Raises:
            ValueError: If the resource cannot be serialized
            TypeError: If the resource is of the wrong type

        Note:
            - Do NOT return the resource object itself
            - Only serialize minimal state needed for restoration
            - Ensure all values are JSON-serializable
        """
        raise NotImplementedError(f"{cls.__name__} must implement save_state() method")

    @classmethod
    @abc.abstractmethod
    def restore_state(cls, data: Dict[str, Any]) -> TResource:
        """
        Restore a resource from serialized state.

        This method is called during event resumption to recreate the
        resource from checkpoint data. It should reconstruct the resource
        to the exact state it was in when save_state() was called.

        Args:
            data: Dictionary containing the serialized resource state
                  (as returned by save_state())

        Returns:
            The restored resource instance

        Raises:
            ValueError: If the data is invalid or incomplete
            RuntimeError: If resource restoration fails

        Note:
            - Handle missing keys gracefully
            - Validate data before using it
            - Re-establish connections/handles as needed
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement restore_state() method"
        )

    @classmethod
    def cleanup(cls, resource: TResource) -> None:
        """
        Clean up the resource when no longer needed.

        This optional method is called after event completion to release
        any resources (close files, connections, etc.). The default
        implementation does nothing.

        Args:
            resource: The resource instance to clean up

        Note:
            - This method should be idempotent
            - Handle errors gracefully (don't raise exceptions)
            - Close connections, file handles, etc.
        """
        pass  # Default: no cleanup

    @classmethod
    def validate_state_data(
        cls, data: Dict[str, Any], required_keys: typing.List[str]
    ) -> None:
        """
        Utility method to validate state data has required keys.

        Args:
            data: The state dictionary to validate
            required_keys: List of required key names

        Raises:
            ValueError: If any required keys are missing
        """
        missing_keys = [key for key in required_keys if key not in data]
        if missing_keys:
            raise ValueError(
                f"{cls.__name__}: Missing required keys in state data: {missing_keys}"
            )

    @classmethod
    def get_provider_name(cls) -> str:
        """
        Get the canonical name for this provider.

        Returns:
            The provider class name by default. Override for custom naming.
        """
        return cls.__name__

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Validate that subclasses implement required methods.

        This is called automatically when a class inherits from ResourceProvider.
        """
        super().__init_subclass__(**kwargs)

        # Check that save_state and restore_state are implemented
        if not callable(getattr(cls, "save_state", None)):
            raise TypeError(f"{cls.__name__} must implement save_state() class method")

        if not callable(getattr(cls, "restore_state", None)):
            raise TypeError(
                f"{cls.__name__} must implement restore_state() class method"
            )


class ResourceProviderRegistry:
    """
    Registry for resource providers.

    Allows registration and lookup of providers by name or resource type.
    This is useful for dynamic provider discovery and configuration.
    """

    def __init__(self) -> None:
        self._providers: Dict[str, type[ResourceProvider]] = {}
        self._type_map: Dict[type, type[ResourceProvider]] = {}

    def register(
        self,
        provider_class: type[ResourceProvider],
        resource_type: Optional[type] = None,
        name: Optional[str] = None,
    ) -> None:
        """
        Register a resource provider.

        Args:
            provider_class: The provider class to register
            resource_type: Optional resource type this provider handles
            name: Optional custom name (defaults to class name)
        """
        if not issubclass(provider_class, ResourceProvider):
            raise TypeError(f"{provider_class} must inherit from ResourceProvider")

        provider_name = name or provider_class.get_provider_name()

        if provider_name in self._providers:
            logger.warning(
                f"Overwriting existing provider registration: {provider_name}"
            )

        self._providers[provider_name] = provider_class

        if resource_type:
            self._type_map[resource_type] = provider_class

        logger.debug(f"Registered resource provider: {provider_name}")

    def get(self, name: str) -> Optional[type[ResourceProvider]]:
        """Get a provider by name."""
        return self._providers.get(name)

    def get_by_type(self, resource_type: type) -> Optional[type[ResourceProvider]]:
        """Get a provider by resource type."""
        return self._type_map.get(resource_type)

    def list_providers(self) -> typing.List[str]:
        """List all registered provider names."""
        return list(self._providers.keys())

    def clear(self) -> None:
        """Clear all registered providers."""
        self._providers.clear()
        self._type_map.clear()


# Global registry instance
_global_registry = ResourceProviderRegistry()


def register_provider(
    resource_type: Optional[type] = None, name: Optional[str] = None
) -> typing.Callable[[type[ResourceProvider]], type[ResourceProvider]]:
    """
    Decorator to register a resource provider.

    Args:
        resource_type: Optional resource type this provider handles
        name: Optional custom name for the provider

    Returns:
        Decorator function

    Example:
        ```python
        @register_provider(resource_type=typing.IO)
        class FileHandleProvider(ResourceProvider[typing.IO]):
            ...
        ```
    """

    def decorator(provider_class: type[ResourceProvider]) -> type[ResourceProvider]:
        _global_registry.register(provider_class, resource_type, name)
        return provider_class

    return decorator


def get_provider(name: str) -> Optional[type[ResourceProvider]]:
    """
    Get a registered provider by name.

    Args:
        name: Provider name

    Returns:
        Provider class or None if not found
    """
    return _global_registry.get(name)


def get_provider_by_type(resource_type: type) -> Optional[type[ResourceProvider]]:
    """
    Get a registered provider by resource type.

    Args:
        resource_type: The type of resource

    Returns:
        Provider class or None if not found
    """
    return _global_registry.get_by_type(resource_type)
