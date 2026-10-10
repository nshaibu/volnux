import asyncio
from tabulate import tabulate

from volnux.cli.command.base import SubCommand
from volnux.engine.workflows.trigger.state import TriggerStateRecord


class InspectTriggerSubCommand(SubCommand):
    """Inspect a trigger"""

    help = "Inspect a trigger"
    name = "inspect"

    def add_arguments(self, parser) -> None:
        parser.add_argument("trigger_id", type=str, help="ID of the trigger to inspect")

    def handle(self, *args, **options) -> None:
        trigger_id = options.get("trigger_id")
        asyncio.run(self._run(trigger_id))

    async def _run(self, trigger_id: str):
        record = await TriggerStateRecord.get_async(record_id=trigger_id)
        if record is None:
            self.error(f"Trigger '{trigger_id}' not found.")
            return
        rows = [
            ["trigger_id", record.trigger_id],
            ["workflow_name", record.workflow_name],
            [
                "lifecycle",
                (
                    record.lifecycle.value
                    if hasattr(record.lifecycle, "value")
                    else record.lifecycle
                ),
            ],
            ["enabled", "yes" if record.enabled else "no"],
            ["fire_count", record.fire_count],
            ["error_count", record.error_count],
            ["last_fired", record.last_fired or "—"],
            ["dirty", "yes" if record.dirty else "no"],
            ["updated_at", record.updated_at],
        ]
        self.stdout.write(tabulate(rows, headers=["field", "value"], tablefmt="github"))
