"""Importable ResourceProviders (the mixin stores and re-imports them by dotted path)."""

from typing import Any, Dict, List

from volnux.execution.rehydrator.event.resources.base import ResourceProvider

CLEANED: List[Any] = []


class Counter:
    def __init__(self, value: int = 0) -> None:
        self.value = value


class CounterProvider(ResourceProvider[Counter]):
    @classmethod
    def save_state(cls, resource: Counter) -> Dict[str, Any]:
        return {"value": resource.value}

    @classmethod
    def restore_state(cls, data: Dict[str, Any]) -> Counter:
        return Counter(data.get("value", 0))

    @classmethod
    def cleanup(cls, resource: Counter) -> None:
        CLEANED.append(("sync", resource))


class AsyncCounterProvider(ResourceProvider[Counter]):
    @classmethod
    async def save_state(cls, resource: Counter) -> Dict[str, Any]:  # type: ignore[override]
        return {"value": resource.value}

    @classmethod
    async def restore_state(cls, data: Dict[str, Any]) -> Counter:  # type: ignore[override]
        return Counter(data.get("value", 0))

    @classmethod
    async def cleanup(cls, resource: Counter) -> None:  # type: ignore[override]
        CLEANED.append(("async", resource))


class ExplodingCleanupProvider(CounterProvider):
    @classmethod
    def cleanup(cls, resource: Counter) -> None:
        raise RuntimeError("cleanup failed")


class BadSaveProvider(CounterProvider):
    @classmethod
    def save_state(cls, resource: Counter) -> Any:  # type: ignore[override]
        return "not a dict"


class NotAProvider:
    pass
