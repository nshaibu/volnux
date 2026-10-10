from datetime import datetime, timezone
from typing import Any, Dict

from formax import BaseModel, MiniAnnotated, Attrib, InitStrategy

from volnux.mixins import KeyValueStoreIntegrationMixin


class AssetMaterialisation(KeyValueStoreIntegrationMixin, BaseModel):
    """A record that an asset was produced.

    Stored in the persistence backend alongside other governance models.
    Each materialization is a point-in-time snapshot of an asset's
    production — who produced it, what version of code was used,
    what upstream assets were consumed, and when it was created.
    """

    asset_key: str
    asset_version: str
    producing_event: str
    producing_event_version: str
    workflow_id: str
    execution_id: str
    task_id: str
    upstream_versions: MiniAnnotated[Dict[str, str], Attrib(default_factory=dict)]
    metadata: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    created_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]

    # is_persisted: bool = False
    # autosave: bool = False

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux_AssetMaterialisation"
