import logging
import warnings
from typing import Any, Dict, List, Type, TYPE_CHECKING, Union

from volnux.exceptions import LLMProviderError

if TYPE_CHECKING:
    from .base import LLMProvider, LLMProviderAdapterBase


logger = logging.getLogger(__name__)


def _key(name: Any) -> str:
    return str(getattr(name, "value", name)).strip().lower()


class LLMProviderRegistry:
    """
    Registry mapping provider names to adapter classes. Names are case-insensitive
    and may be given as an :class:`LLMProvider`. Extended at runtime with
    :meth:`register`; the built-in adapters register themselves when imported.
    """

    _registry: Dict[str, Type["LLMProviderAdapterBase"]] = {}

    @classmethod
    def register(
        cls,
        provider_name: Union[str, "LLMProvider"],
        adapter_class: Type["LLMProviderAdapterBase"],
    ) -> None:
        key = _key(provider_name)
        existing = cls._registry.get(key)
        if existing is not None and existing is not adapter_class:
            warnings.warn(
                f"LLMProviderRegistry: overwriting provider '{key}'.",
                stacklevel=2,
            )
        cls._registry[key] = adapter_class
        logger.debug(
            "LLMProviderRegistry: registered '%s' → %s", key, adapter_class.__name__
        )

    @classmethod
    def unregister(cls, provider_name: Union[str, "LLMProvider"]) -> bool:
        return cls._registry.pop(_key(provider_name), None) is not None

    @classmethod
    def available(cls) -> List[str]:
        return sorted(cls._registry)

    @classmethod
    def is_registered(cls, provider_name: Union[str, "LLMProvider"]) -> bool:
        return _key(provider_name) in cls._registry

    @classmethod
    def resolve(
        cls, provider_name: Union[str, "LLMProvider"], **options: Any
    ) -> "LLMProviderAdapterBase":
        """A new adapter for ``provider_name``, built with ``options``."""
        adapter_class = cls._registry.get(_key(provider_name))
        if adapter_class is None:
            raise LLMProviderError(
                f"No adapter registered for LLM provider '{provider_name}'. "
                f"Available: {cls.available()}. "
                f"Register via LLMProviderRegistry.register().",
                retryable=False,
            )
        return adapter_class(**options)
