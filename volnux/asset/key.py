from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .utils import FreshnessPolicy


@dataclass(frozen=True)
class AssetKey:
    """Uniquely identifies an asset in the catalog.

    An asset key is a namespaced identifier for a data product.
    Convention: asset keys match the producing event name by default.

    Example:
        AssetKey("cleaned_orders")
        AssetKey("order_metrics", namespace="finance")
    """

    name: str
    namespace: str = "default"

    def __str__(self) -> str:
        if self.namespace == "default":
            return self.name
        return f"{self.namespace}/{self.name}"

    @classmethod
    def from_string(cls, key: str) -> "AssetKey":
        """Parse an asset key from a string.

        Supports 'name' and 'namespace/name' formats.
        """
        parts = key.split("/", 1)
        if len(parts) == 2:
            return cls(name=parts[1], namespace=parts[0])
        return cls(name=parts[0])

    def __hash__(self) -> int:
        return hash((self.namespace, self.name))


@dataclass
class AssetDeclaration:
    """In-memory declaration of an asset's metadata.

    Stored in the AssetCatalog's _declared dict. The persistent
    record of asset production is AssetMaterialisation.
    """

    key: AssetKey
    producing_event: str
    description: Optional[str] = None
    group: Optional[str] = None
    freshness_policy: Optional["FreshnessPolicy"] = None
    upstream_keys: List[AssetKey] = field(default_factory=list)
