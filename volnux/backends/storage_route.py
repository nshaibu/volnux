from dataclasses import dataclass
from typing import List, Dict, Union, Callable, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .store import KeyValueStoreBackendBase


@dataclass(frozen=True)
class StorageRoute:
    """
    Defines the physical topology and routing rules for a model.
    Strictly separated from the Data Schema (fields/types).
    """

    # The logical path segments (e.g., ["volnux", "{project_id}", "saga", "dlq"])
    components: List[str]

    # How to resolve the dynamic segments (e.g., {"project_id": "acme"} or a lambda)
    routing_keys: Union[Dict[str, str], Callable[[], Dict[str, str]], None] = None

    def resolve(
        self,
        backend: "KeyValueStoreBackendBase",
        override: Optional[Dict[str, str]] = None,
    ) -> str:
        return backend.resolve_physical_target(self, override)
