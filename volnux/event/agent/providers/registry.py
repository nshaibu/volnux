import logging
import warnings

from typing import Dict, Type, TYPE_CHECKING
from volnux.exceptions import LLMProviderError


if TYPE_CHECKING:
    from .base import LLMProviderAdapterBase


logger = logging.getLogger(__name__)


class LLMProviderRegistry:
    """
    Singleton registry mapping provider name strings to adapter classes.
    Extended at runtime via ``register()``.
    """

    _registry: Dict[str, Type["LLMProviderAdapterBase"]] = {}

    @classmethod
    def register(
        cls,
        provider_name: str,
        adapter_class: Type["LLMProviderAdapterBase"],
    ) -> None:
        if provider_name in cls._registry:
            warnings.warn(
                f"LLMProviderRegistry: overwriting provider '{provider_name}'.",
                stacklevel=2,
            )
        cls._registry[provider_name] = adapter_class
        logger.debug(
            "LLMProviderRegistry: registered '%s' → %s",
            provider_name,
            adapter_class.__name__,
        )

    @classmethod
    def resolve(cls, provider_name: str) -> "LLMProviderAdapterBase":
        adapter_class = cls._registry.get(provider_name)
        if adapter_class is None:
            raise LLMProviderError(
                f"No adapter registered for LLM provider '{provider_name}'. "
                f"Available: {sorted(cls._registry)}. "
                f"Register via LLMProviderRegistry.register()."
            )
        return adapter_class()
