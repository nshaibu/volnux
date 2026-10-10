from enum import Enum


class AssetTriggerCondition(str, Enum):
    """When an AssetTrigger should fire."""

    MATERIALISED = "materialised"  # Fire when the watched asset is produced
    STALE = "stale"  # Fire when the watched asset exceeds freshness
