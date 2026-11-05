import logging
from enum import Enum
from typing import Any, Dict, Optional
from formax import Attrib, BaseModel, MiniAnnotated

from .triggers import TriggerLifecycle
from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.backends.fields import DateTimeField, DTConfig

logger = logging.getLogger(__name__)


def _lifecycle_to_state(instance: Any, value: Any) -> Optional[str]:
    return value.value if isinstance(value, Enum) else value


def _lifecycle_from_state(instance: Any, raw: Any) -> Optional[TriggerLifecycle]:
    """A store hands the lifecycle back as its plain value ('active'); the record
    works with the enum, so ``lifecycle == TriggerLifecycle.ACTIVE`` holds."""
    if raw is None or isinstance(raw, TriggerLifecycle):
        return raw
    return TriggerLifecycle(raw)


def _bool_from_state(instance: Any, raw: Any) -> Optional[bool]:
    """SQL stores return booleans as 0/1."""
    return None if raw is None else bool(raw)


_LIFECYCLE_CODEC = {
    "to_state": _lifecycle_to_state,
    "from_state": _lifecycle_from_state,
}
_BOOL_CODEC = {"from_state": _bool_from_state}


class TriggerStateRecord(KeyValueStoreIntegrationMixin, BaseModel):
    """Persisted state of one trigger, shared between the engine and the CLI.

    ``dirty`` means "a change was written from outside (the CLI) that the engine has
    not applied yet". The engine's own transitions (``start``, ``stop``, ``pause``,
    ``resume``) therefore leave it alone.

    ``last_fired`` is a ``datetime`` (UTC) or ``None`` if the trigger never fired, and
    ``updated_at`` is stamped by every ``save()``.
    """

    workflow_name: str
    lifecycle: MiniAnnotated[TriggerLifecycle, Attrib(metadata=_LIFECYCLE_CODEC)]
    enabled: MiniAnnotated[bool, Attrib(metadata=_BOOL_CODEC)]
    fire_count: MiniAnnotated[int, Attrib(default=0)]
    error_count: MiniAnnotated[int, Attrib(default=0)]
    last_fired: DateTimeField[DTConfig(nullable=True)]
    dirty: MiniAnnotated[bool, Attrib(default=False, metadata=_BOOL_CODEC)]
    updated_at: DateTimeField[DTConfig(auto_now=True)]

    #: The engine and the CLI share these records, so they live in their own
    #: database (a SQLite file unless ``DATABASES["triggers"]`` says otherwise).
    @classmethod
    def get_database_alias(cls) -> str:
        return "triggers"

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
        """Mark the trigger active. A stopped trigger that is started again may fire
        again; one constructed disabled stays disabled."""
        if self.lifecycle == TriggerLifecycle.ACTIVE:
            raise RuntimeError(
                f"Trigger '{self.id}' is already active and cannot be started again."
            )

        if self.lifecycle == TriggerLifecycle.STOPPED:
            self.enabled = True
        self.lifecycle = TriggerLifecycle.ACTIVE
        await self.save()
        logger.info(f"Trigger '{self.id}' started.")

    async def stop(self):
        if self.lifecycle == TriggerLifecycle.STOPPED:
            raise RuntimeError(
                f"Trigger '{self.id}' is already stopped and cannot be stopped again."
            )

        self.lifecycle = TriggerLifecycle.STOPPED
        self.enabled = False
        await self.save()
        logger.info(f"Trigger '{self.id}' stopped.")

    async def mark_error(self):
        """Record that the trigger could not start or keep running."""
        self.lifecycle = TriggerLifecycle.ERROR
        await self.save()
        logger.error(f"Trigger '{self.id}' is in error.")

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
        self.enabled = False
        self.lifecycle = TriggerLifecycle.PAUSED
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
        self.enabled = True
        self.lifecycle = TriggerLifecycle.ACTIVE
        await self.save()
        logger.info(f"Trigger '{self.id}' resumed.")
