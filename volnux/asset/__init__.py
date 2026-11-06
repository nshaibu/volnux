"""
Volnux Asset Layer

Data-centric operations for Volnux workflows. Assets represent named,
versioned, trackable data products produced by events. The asset layer
provides materialization tracking, lineage, staleness detection, and
asset-triggered execution — all built on the existing persistence and
trigger infrastructure.

An asset is what an event produces. The event is the computation.
The asset is the data. Both are first-class.
"""

from .utils import FreshnessPolicy
from .key import AssetKey, AssetDeclaration
from .catalog import get_asset_catalog, set_asset_catalog
from .materialisation import AssetMaterialisation
from .enums import AssetTriggerCondition
