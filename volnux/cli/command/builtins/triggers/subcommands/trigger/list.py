import asyncio
from tabulate import tabulate

from volnux.cli.command.base import SubCommand
from volnux.engine.workflows.trigger.state import TriggerStateRecord


class ListTriggerSubCommand(SubCommand):
    """List all triggers"""

    help = "List all triggers"
    name = "list"

    def add_arguments(self, parser) -> None:
        return

    def handle(self, *args, **options) -> None:
        async def _run():
            return await TriggerStateRecord.all_async()

        records: list[TriggerStateRecord] = asyncio.run(_run())

        if not records:
            self.warning("No trigger states found.")
            return

        rows = [
            [
                r.trigger_id,
                r.workflow_name,
                r.lifecycle.value if hasattr(r.lifecycle, "value") else r.lifecycle,
                "yes" if r.enabled else "no",
                r.fire_count,
                r.error_count,
                r.last_fired or "—",
                r.updated_at,
            ]
            for r in records
        ]
        headers = [
            "trigger_id",
            "workflow_name",
            "lifecycle",
            "enabled",
            "fire_count",
            "error_count",
            "last_fired",
            "updated_at",
        ]
        self.stdout.write(tabulate(rows, headers=headers, tablefmt="github"))
