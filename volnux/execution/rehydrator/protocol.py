from typing import Protocol, Type, Any


class Monitorable(Protocol):
    """
    Protocol for monitorable objects that can be periodically snapshotted.
    """

    id: str

    async def create_snapshot(self, *args, **kwargs) -> "Snapshot": ...


class Snapshot(Protocol):
    """
    Protocol for snapshot objects that can be saved and restored.
    """

    id: str

    async def save_async(self, force_inert: bool = False, ttl: int = 0): ...

    async def restore(self) -> object: ...


class Builder(Protocol):
    """
    Protocol for builder objects that can be used to build objects.
    """

    def __init__(self, serializer: Type[Any]): ...

    async def build(self, *args, **kwargs) -> Snapshot: ...
