import asyncio

from volnux.cli.command.base import SubCommand
from volnux.engine.workflows.trigger.triggers.base import TriggerLifecycle
from volnux.engine.workflows.trigger.state import TriggerStateRecord


class ResumeTriggerSubCommand(SubCommand):
    """Resume a trigger by setting its lifecycle to RUNNING and marking it dirty."""

    help = "Resume a trigger"
    name = "resume"

    def add_arguments(self, parser) -> None:
        parser.add_argument("trigger_id", help="ID of the trigger to resume")

    def handle(self, *args, **options) -> None:
        trigger_id = options.get("trigger_id")
        asyncio.run(self._run(trigger_id))

    async def _run(self, trigger_id: str):
        record = await TriggerStateRecord.get_async(record_id=trigger_id)
        if record is None:
            self.error(f"Trigger '{trigger_id}' not found.")
            return
        record.lifecycle = TriggerLifecycle.ACTIVE
        record.dirty = True
        await record.save_async()
        self.success(f"Trigger '{trigger_id}' marked as RUNNING.")
