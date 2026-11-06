import asyncio
import logging
from collections import OrderedDict
from typing import Optional, Union

from .base import TriggerBase, TriggerType
from volnux.asset.catalog import AssetCatalog, get_asset_catalog
from volnux.asset import AssetKey, AssetTriggerCondition, AssetMaterialisation


logger = logging.getLogger(__name__)


class AssetTrigger(TriggerBase):
    """Fires a workflow when an asset is materialised or becomes stale.

    Composes with the existing trigger layer — AssetTrigger can be
    chained with other triggers for complex scheduling.

    ``MATERIALISED`` fires once per materialisation the catalog reports.
    ``STALE`` polls the catalog every ``poll_interval`` seconds and fires once
    each time the asset *becomes* stale; it fires again only after the asset has
    been seen fresh in between (i.e. it was rematerialised), so a stale asset does
    not start the workflow again every poll.

    Notifications come from the catalog in this process: a materialisation
    recorded in another process does not reach the trigger.

    Example:
        AssetTrigger(
            workflow_name="order_metrics_pipeline",
            asset_catalog=catalog,
            watch_key=AssetKey("raw_orders"),
            trigger_on=AssetTriggerCondition.MATERIALISED,
        )
    """

    trigger_type = TriggerType.ASSET

    #: Materialisation ids remembered for de-duplication (oldest forgotten first).
    _MAX_PROCESSED = 1000

    def __init__(
        self,
        workflow_name: str,
        asset_catalog: Optional[AssetCatalog] = None,
        watch_key: Union[AssetKey, str, None] = None,
        trigger_on: Union[
            AssetTriggerCondition, str
        ] = AssetTriggerCondition.MATERIALISED,
        poll_interval: float = 60.0,
        **kwargs,
    ):
        if watch_key is None:
            raise ValueError("AssetTrigger requires a watch_key.")
        if isinstance(watch_key, str):
            watch_key = AssetKey.from_string(watch_key)
        if not isinstance(watch_key, AssetKey):
            raise TypeError(
                f"'watch_key' must be an AssetKey or a string, got "
                f"{type(watch_key).__name__}."
            )
        try:
            trigger_on = AssetTriggerCondition(trigger_on)
        except ValueError:
            raise ValueError(
                f"Unknown trigger_on {trigger_on!r}; expected one of "
                f"{[c.value for c in AssetTriggerCondition]}."
            ) from None
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive.")

        super().__init__(workflow_name, **kwargs)
        self._catalog = (
            asset_catalog if asset_catalog is not None else get_asset_catalog()
        )
        self._watch_key = watch_key
        self._trigger_on = trigger_on
        self.poll_interval = float(poll_interval)

        # One callback object for subscribe and unsubscribe alike.
        self._callback = self._on_materialised
        self._subscribed = False
        self._stale_task: Optional[asyncio.Task] = None
        # Whether the current stale episode has already been reported.
        self._stale_reported = False
        # Materialisation ids already handled, oldest first.
        self._processed_materialisations: "OrderedDict[str, None]" = OrderedDict()

    @property
    def watch_key(self) -> AssetKey:
        return self._watch_key

    @property
    def trigger_on(self) -> AssetTriggerCondition:
        return self._trigger_on

    async def start(self) -> None:
        """Start watching for asset events (idempotent)."""
        if self._trigger_on == AssetTriggerCondition.MATERIALISED:
            if self._subscribed:
                return
            await self._catalog.subscribe(self._watch_key, self._callback)
            self._subscribed = True
        elif self._trigger_on == AssetTriggerCondition.STALE:
            if self._stale_task is not None and not self._stale_task.done():
                return
            self._stale_reported = False
            self._stale_task = asyncio.create_task(
                self._poll_staleness(), name=f"AssetTrigger.{self.trigger_id}.stale"
            )

    async def stop(self) -> None:
        """Stop watching for asset events (idempotent)."""
        if self._subscribed:
            try:
                await self._catalog.unsubscribe(self._watch_key, self._callback)
            finally:
                self._subscribed = False

        task, self._stale_task = self._stale_task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _on_materialised(self, materialisation: AssetMaterialisation) -> None:
        """Handle a materialisation event.

        Deduplicates by materialisation ID to prevent infinite
        trigger loops when asset-triggered workflows produce assets.
        """
        if materialisation.id in self._processed_materialisations:
            return

        self._processed_materialisations[materialisation.id] = None
        # Forget the oldest ids, never the newest
        while len(self._processed_materialisations) > self._MAX_PROCESSED:
            self._processed_materialisations.popitem(last=False)

        await self.activate(
            asset_key=str(self._watch_key),
            materialisation_id=materialisation.id,
            materialised_at=materialisation.created_at,
            upstream_versions=materialisation.upstream_versions,
        )

    async def _poll_staleness(self) -> None:
        """Periodically check if the watched asset is stale."""
        while True:
            try:
                await self.check_staleness()
            except Exception as e:
                logger.error(
                    "Error polling staleness for asset '%s': %s",
                    self._watch_key,
                    e,
                )

            await asyncio.sleep(self.poll_interval)

    async def check_staleness(self) -> bool:
        """One staleness check; fires the workflow when the asset has just become
        stale. Returns whether the asset is stale."""
        stale = await self._catalog.is_stale(self._watch_key)
        if not stale:
            self._stale_reported = False  # fresh again: the next staleness is new
            return False
        if not self.enabled:
            return True  # paused: report the episode once it is enabled again
        if not self._stale_reported:
            # Marked before activating: a failing activation is not retried every
            # poll either (activate() counts the error).
            self._stale_reported = True
            await self.activate(
                asset_key=str(self._watch_key),
                reason="freshness_policy_exceeded",
            )
        return True
