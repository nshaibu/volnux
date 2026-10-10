import asyncio

from volnux.cli.command.base import SubCommand
from volnux.engine.workflows.trigger.triggers.base import TriggerLifecycle
from volnux.engine.workflows.trigger.state import TriggerStateRecord


class StopTriggerSubCommand(SubCommand):
    """Stop a running trigger by setting its lifecycle to PAUSED and marking it dirty."""

    help = "Stop a running trigger"
    name = "stop"

    def add_arguments(self, parser) -> None:
        parser.add_argument("trigger_id", help="ID of the trigger to stop")

    def handle(self, *args, **options) -> None:
        trigger_id = options.get("trigger_id")
        asyncio.run(self._stop_trigger(trigger_id))

    async def _stop_trigger(self, trigger_id: str):
        record = await TriggerStateRecord.get_async(record_id=trigger_id)
        if record is None:
            self.error(f"Trigger '{trigger_id}' not found.")
            return
        record.lifecycle = TriggerLifecycle.STOPPED
        record.dirty = True
        await record.save_async()
        self.success(f"Trigger '{trigger_id}' marked as STOPPED.")
