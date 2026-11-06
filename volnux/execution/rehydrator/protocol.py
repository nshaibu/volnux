from typing import Protocol, Type, Any, Optional


class Persistable(Protocol):
    """
    Protocol for objects the CheckpointManager can save
    (any KeyValueStoreIntegrationMixin model).
    """

    id: str

    async def save(self, force_insert: bool = False, ttl: Optional[int] = None): ...


class Monitorable(Persistable, Protocol):
    """
    Protocol for long-lived objects that are periodically saved as they are
    (e.g. ExecutionContext).
    """


class Snapshot(Persistable, Protocol):
    """
    Protocol for snapshot objects that can be saved and restored.
    """

    async def restore(self) -> object: ...


class Builder(Protocol):
    """
    Protocol for builder objects that can be used to build objects.
    """

    def __init__(self, serializer: Type[Any]): ...

    async def build(self, *args, **kwargs) -> Snapshot: ...
