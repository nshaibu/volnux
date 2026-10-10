import logging
from typing import Dict, Any, Optional
from formax import Attrib, BaseModel, MiniAnnotated

from .triggers import TriggerLifecycle
from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.backends.fields import DateTimeField, DTConfig

logger = logging.getLogger(__name__)


class TriggerStateRecord(KeyValueStoreIntegrationMixin, BaseModel):
    workflow_name: str
    lifecycle: TriggerLifecycle
    enabled: bool
    fire_count: MiniAnnotated[int, Attrib(default=0)]
    error_count: MiniAnnotated[int, Attrib(default=0)]
    last_fired: Optional[str]
    # dirty=True means the CLI wrote a change the engine hasn't applied yet
    dirty: MiniAnnotated[bool, Attrib(default=False)]
    updated_at: DateTimeField[DTConfig("updated_at", auto_now=True)]

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        return {
            "ENGINE": "volnux.backends.stores.sqlite.SqliteStoreBackend",
            "CONNECTOR_CONFIG": {
                "database": "volnux.db",
            },
        }

    @classmethod
    async def get_dirty(cls):
        records = await cls.filter(dirty=True)
        return records

    @classmethod
    async def mark_clean(cls, trigger_id: str) -> None:
        record = await cls.get_or_none(record_id=trigger_id)
        if record is None:
            logging.warning(
                f"TriggerStateRecord not found for trigger_id: {trigger_id}"
            )
            return

        record.dirty = False
        await record.save()

    async def start(self):
        if self.lifecycle == TriggerLifecycle.ACTIVE:
            raise RuntimeError(
                f"Trigger '{self.id}' is already active and cannot be started again."
            )

        async with self.transaction():
            self.lifecycle = TriggerLifecycle.ACTIVE
            self.enabled = True
            self.dirty = True
            await self.save()
        logger.info(f"Trigger '{self.id}' started.")

    async def stop(self):
        if self.lifecycle == TriggerLifecycle.STOPPED:
            raise RuntimeError(
                f"Trigger '{self.id}' is already stopped and cannot be stopped again."
            )

        async with self.transaction():
            self.lifecycle = TriggerLifecycle.STOPPED
            self.enabled = False
            self.dirty = True
            await self.save()
        logger.info(f"Trigger '{self.id}' stopped.")

    async def pause(self):
        """
        Pause the trigger (temporarily disable).

        Raises:
            RuntimeError: If the trigger is not in the ACTIVE state.
        """

        if self.lifecycle not in (TriggerLifecycle.ACTIVE,):
            raise RuntimeError(
                f"Cannot pause trigger '{self.id}': "
                f"current lifecycle is '{self.lifecycle.value}', expected 'active'."
            )
        async with self.transaction():
            self.enabled = False
            self.lifecycle = TriggerLifecycle.PAUSED
            self.dirty = True
            await self.save()
        logger.info(f"Trigger '{self.id}' paused.")

    async def resume(self):
        """
        Resume a paused trigger.

        Raises:
            RuntimeError: If the trigger is not in the PAUSED state.
        """

        if self.lifecycle not in (TriggerLifecycle.PAUSED,):
            raise RuntimeError(
                f"Cannot resume trigger '{self.id}': "
                f"current lifecycle is '{self.lifecycle.value}', expected 'paused'."
            )
        async with self.transaction():
            self.enabled = True
            self.lifecycle = TriggerLifecycle.ACTIVE
            self.dirty = True
            await self.save()
        logger.info(f"Trigger '{self.id}' resumed.")
