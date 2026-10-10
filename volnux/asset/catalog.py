import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from .key import AssetKey, AssetDeclaration
from .utils import FreshnessPolicy
from .materialisation import AssetMaterialisation

if TYPE_CHECKING:
    from volnux.result.stream import ResultStream


logger = logging.getLogger(__name__)


class AssetCatalog:
    """
    Registry of declared assets and their materialization history.

    The catalog is the single source of truth for what assets exist,
    when they were last produced, and how they relate to each other.
    It is backed by the persistence engine — the same backends that
    store workflows, executions, and audit entries.

    Example:
        >>> catalog = AssetCatalog()
        >>> await catalog.register(AssetKey("cleaned_orders"), ...)
        >>> mat = await catalog.get_materialisation(AssetKey("cleaned_orders"))
        >>> stale = await catalog.is_stale(AssetKey("cleaned_orders"))
    """

    def __init__(self):
        # In-memory registry of declared assets
        self._declared: Dict[AssetKey, "AssetDeclaration"] = {}
        # Subscribers for asset-triggered execution
        self._subscribers: Dict[
            AssetKey, List[Callable[[AssetMaterialisation], Any]]
        ] = {}

    async def register(
        self,
        key: AssetKey,
        producing_event: str,
        description: Optional[str] = None,
        group: Optional[str] = None,
        freshness_policy: Optional[FreshnessPolicy] = None,
        upstream_keys: Optional[List[AssetKey]] = None,
    ) -> None:
        """Register an asset in the catalog.

        Called automatically by the @asset decorator when an EventBase
        subclass is defined. Does not need to be called manually.

        Args:
            key: The unique identifier for this asset.
            producing_event: The fully-qualified event class name.
            description: Human-readable description of the asset.
            group: Logical grouping (e.g., 'order_processing').
            freshness_policy: How fresh this asset should be kept.
            upstream_keys: Assets this asset depends on.
        """
        self._declared[key] = AssetDeclaration(
            key=key,
            producing_event=producing_event,
            description=description,
            group=group,
            freshness_policy=freshness_policy,
            upstream_keys=upstream_keys or [],
        )
        logger.info(
            "Registered asset '%s' produced by %s",
            key,
            producing_event,
        )

    def is_registered(self, key: AssetKey) -> bool:
        """Check if an asset key is registered in the catalog."""
        return key in self._declared

    def get_declaration(self, key: AssetKey) -> Optional["AssetDeclaration"]:
        """Get the declaration for a registered asset."""
        return self._declared.get(key)

    def list_assets(self, group: Optional[str] = None) -> List["AssetDeclaration"]:
        """List all registered assets, optionally filtered by group."""
        assets = list(self._declared.values())
        if group:
            assets = [a for a in assets if a.group == group]
        return assets

    async def record_materialisation(
        self,
        key: AssetKey,
        asset_version: str,
        producing_event: str,
        producing_event_version: str,
        workflow_id: str,
        execution_id: str,
        task_id: str,
        upstream_versions: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AssetMaterialisation:
        """Record that an asset was produced.

        Called automatically by the stream engine when an @asset event
        completes with is_persisted=True.

        Args:
            key: The asset that was produced.
            asset_version: Version identifier for this materialisation.
            producing_event: The event class that produced this asset.
            producing_event_version: The version of the producing event.
            workflow_id: The workflow execution that produced this asset.
            execution_id: The specific execution instance.
            task_id: The task that executed the producing event.
            upstream_versions: Versions of upstream assets consumed.
            metadata: Additional metadata about this materialisation.

        Returns:
            The persisted materialisation record.
        """
        mat = AssetMaterialisation(
            asset_key=str(key),
            asset_version=asset_version,
            producing_event=producing_event,
            producing_event_version=producing_event_version,
            workflow_id=workflow_id,
            execution_id=execution_id,
            task_id=task_id,
            upstream_versions=upstream_versions or {},
            metadata=metadata or {},
        )
        await mat.save_async()
        logger.info("Recorded materialisation of '%s' (v%s)", key, asset_version)

        # Notify subscribers
        await self._notify_subscribers(key, mat)

        return mat

    async def get_materialisation(
        self,
        key: AssetKey,
        version: Optional[str] = None,
    ) -> Optional[AssetMaterialisation]:
        """Return the most recent materialisation of an asset.

        Args:
            key: The asset to retrieve.
            version: If provided, return this specific version.

        Returns:
            The materialisation record, or None if the asset has
            never been produced.
        """
        filters = {"asset_key": str(key)}
        if version:
            filters["asset_version"] = version

        results = await AssetMaterialisation.filter_async(
            order_by="-created_at",
            limit=1,
            **filters,
        )
        return results[0] if results else None

    async def get_materialisation_history(
        self,
        key: AssetKey,
        limit: int = 20,
    ) -> "ResultStream[AssetMaterialisation]":
        """Return the materialisation history for an asset."""
        return await AssetMaterialisation.filter_async(
            asset_key=str(key),
            order_by="-created_at",
            limit=limit,
        )

    async def is_stale(self, key: AssetKey) -> bool:
        """Check if an asset is stale.

        An asset is stale if:
        1. It has never been materialised.
        2. Its freshness policy has been exceeded.
        3. Any upstream asset was materialised after it was.

        Args:
            key: The asset to check.

        Returns:
            True if the asset needs to be rematerialised.
        """
        declaration = self._declared.get(key)
        if declaration is None:
            logger.warning("Staleness check for unregistered asset '%s'", key)
            return False

        materialisation = await self.get_materialisation(key)
        if materialisation is None:
            return True  # Never materialised

        materialised_at = datetime.fromtimestamp(
            materialisation.created_at, tz=timezone.utc
        )

        # Check freshness policy
        if declaration.freshness_policy:
            if declaration.freshness_policy.is_exceeded(materialised_at):
                return True

        # Check upstream staleness
        for upstream_key in declaration.upstream_keys:
            upstream_mat = await self.get_materialisation(upstream_key)
            if upstream_mat and upstream_mat.created_at > materialisation.created_at:
                return True  # Upstream was updated after this asset

        return False

    async def get_upstream_keys(self, key: AssetKey) -> List[AssetKey]:
        """Return the direct upstream assets for a given key."""
        declaration = self._declared.get(key)
        if declaration is None:
            return []
        return declaration.upstream_keys

    async def get_downstream_keys(self, key: AssetKey) -> List[AssetKey]:
        """Return all assets that depend on the given key."""
        downstream = []
        for asset_key, declaration in self._declared.items():
            if key in declaration.upstream_keys:
                downstream.append(asset_key)
        return downstream

    async def get_lineage_graph(self, key: AssetKey) -> Dict[str, Any]:
        """Return the full upstream and downstream lineage for an asset.

        Returns a dict with 'upstream', 'downstream', and
        'materialisation' fields suitable for rendering in a UI.
        """
        upstream_keys = await self.get_upstream_keys(key)
        downstream_keys = await self.get_downstream_keys(key)
        materialisation = await self.get_materialisation(key)

        return {
            "asset_key": str(key),
            "declaration": (
                self._declared[key].__dict__ if key in self._declared else None
            ),
            "materialisation": (
                materialisation.__getstate__() if materialisation else None
            ),
            "upstream": [
                {
                    "key": str(uk),
                    "materialisation": (
                        (await self.get_materialisation(uk)).__getstate__()
                        if await self.get_materialisation(uk)
                        else None
                    ),
                }
                for uk in upstream_keys
            ],
            "downstream": [str(dk) for dk in downstream_keys],
        }

    async def subscribe(
        self,
        key: AssetKey,
        callback: Callable[[AssetMaterialisation], Any],
    ) -> None:
        """Subscribe to materialisation events for an asset.

        The callback is invoked whenever the asset is materialised.
        Used by AssetTrigger to fire workflows in response to
        asset production.

        Args:
            key: The asset to watch.
            callback: Async callable receiving the materialisation.
        """
        if key not in self._subscribers:
            self._subscribers[key] = []
        self._subscribers[key].append(callback)
        logger.debug("Subscribed to materialisation of '%s'", key)

    async def unsubscribe(
        self,
        key: AssetKey,
        callback: Callable[[AssetMaterialisation], Any],
    ) -> None:
        """Remove a subscription."""
        if key in self._subscribers:
            self._subscribers[key] = [
                cb for cb in self._subscribers[key] if cb is not callback
            ]
            if not self._subscribers[key]:
                del self._subscribers[key]
            logger.debug("Unsubscribed from materialisation of '%s'", key)

    async def _notify_subscribers(
        self,
        key: AssetKey,
        materialisation: AssetMaterialisation,
    ) -> None:
        """Notify all subscribers of a materialisation event."""
        subscribers = self._subscribers.get(key, [])
        for callback in subscribers:
            try:
                await callback(materialisation)
            except Exception as e:
                logger.error(
                    "Error notifying subscriber for asset '%s': %s",
                    key,
                    e,
                )


# Global asset catalog instance
_asset_catalog: Optional[AssetCatalog] = None


def get_asset_catalog() -> AssetCatalog:
    """Get the global asset catalog instance.

    Creates the catalog on first access. In production, the catalog
    is initialised by the Volnux runtime and shared across all
    components.
    """
    global _asset_catalog
    if _asset_catalog is None:
        _asset_catalog = AssetCatalog()
    return _asset_catalog


def set_asset_catalog(catalog: AssetCatalog) -> None:
    """Set the global asset catalog instance.

    Called by the Volnux runtime during initialisation.
    """
    global _asset_catalog
    _asset_catalog = catalog
