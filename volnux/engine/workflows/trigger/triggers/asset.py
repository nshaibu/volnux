import asyncio
import logging
from typing import Set


from .base import TriggerBase, TriggerType
from volnux.asset.catalog import AssetCatalog
from volnux.asset import AssetKey, AssetTriggerCondition, AssetMaterialisation


logger = logging.getLogger(__name__)


class AssetTrigger(TriggerBase):
    """Fires a workflow when an asset is materialised or becomes stale.

    Composes with the existing trigger layer — AssetTrigger can be
    chained with other triggers for complex scheduling.

    Example:
        AssetTrigger(
            workflow_name="order_metrics_pipeline",
            asset_catalog=catalog,
            watch_key=AssetKey("raw_orders"),
            trigger_on=AssetTriggerCondition.MATERIALISED,
        )
    """

    trigger_type = TriggerType.ASSET

    def __init__(
        self,
        workflow_name: str,
        asset_catalog: AssetCatalog,
        watch_key: AssetKey,
        trigger_on: AssetTriggerCondition = AssetTriggerCondition.MATERIALISED,
        **kwargs,
    ):
        super().__init__(workflow_name, **kwargs)
        self._catalog = asset_catalog
        self._watch_key = watch_key
        self._trigger_on = trigger_on
        # Track which materialisation IDs have been processed
        self._processed_materialisations: Set[str] = set()

    async def start(self) -> None:
        """Start watching for asset events."""
        if self._trigger_on == AssetTriggerCondition.MATERIALISED:
            await self._catalog.subscribe(
                self._watch_key,
                self._on_materialised,
            )
        elif self._trigger_on == AssetTriggerCondition.STALE:
            # Poll for staleness
            self._stale_task = asyncio.create_task(self._poll_staleness())

    async def stop(self) -> None:
        """Stop watching for asset events."""
        if self._trigger_on == AssetTriggerCondition.MATERIALISED:
            await self._catalog.unsubscribe(
                self._watch_key,
                self._on_materialised,
            )
        elif self._trigger_on == AssetTriggerCondition.STALE:
            if hasattr(self, "_stale_task"):
                self._stale_task.cancel()

    async def _on_materialised(self, materialisation: AssetMaterialisation) -> None:
        """Handle a materialisation event.

        Deduplicates by materialisation ID to prevent infinite
        trigger loops when asset-triggered workflows produce assets.
        """
        if materialisation.id in self._processed_materialisations:
            return

        self._processed_materialisations.add(materialisation.id)

        # Clean up old processed IDs (keep last 1000)
        if len(self._processed_materialisations) > 1000:
            self._processed_materialisations = set(
                list(self._processed_materialisations)[-500:]
            )

        await self.activate(
            asset_key=str(self._watch_key),
            materialisation_id=materialisation.id,
            materialised_at=materialisation.created_at,
            upstream_versions=materialisation.upstream_versions,
        )

    async def _poll_staleness(self) -> None:
        """Periodically check if the watched asset is stale."""
        poll_interval = 60  # Check every 60 seconds

        while True:
            try:
                if await self._catalog.is_stale(self._watch_key):
                    await self.activate(
                        asset_key=str(self._watch_key),
                        reason="freshness_policy_exceeded",
                    )
            except Exception as e:
                logger.error(
                    "Error polling staleness for asset '%s': %s",
                    self._watch_key,
                    e,
                )

            await asyncio.sleep(poll_interval)
