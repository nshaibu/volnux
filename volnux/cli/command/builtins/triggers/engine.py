from volnux.cli.command.mixins import SubCommandMixin
from volnux.cli.command.base import CommandCategory, BaseCommand
from volnux.cli.command.builtins.triggers.subcommands.engine import (
    StatusTriggerEngineSubCommand,
    StartTriggerEngineSubCommand,
    StopTriggerEngineSubCommand,
)


class TriggerEngineCommand(SubCommandMixin, BaseCommand):
    """
    Manage the Volnux TriggerEngine — the runtime that listens for workflow
    triggers and dispatches executions across the mesh.

    Sub-subcommands
    ───────────────
        start — Start the engine (optionally in the background)
        stop — Stop a running engine
        restart — Stop then start (rolling restart)
        status — Report engine health and active workflow count
    """

    name = "trigger_engine"
    help = "Manage the Volnux TriggerEngine lifecycle."
    category = CommandCategory.EXECUTION

    subcommands = {
        "start": StartTriggerEngineSubCommand,
        "stop": StopTriggerEngineSubCommand,
        # "restart": "_handle_restart",
        "status": StatusTriggerEngineSubCommand,
    }
