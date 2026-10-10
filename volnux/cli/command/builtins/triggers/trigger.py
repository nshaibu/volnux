from volnux.cli.command.mixins import SubCommandMixin
from volnux.cli.command.base import CommandCategory, BaseCommand
from volnux.cli.command.builtins.triggers.subcommands.trigger import (
    InspectTriggerSubCommand,
    PauseTriggerSubCommand,
    ResumeTriggerSubCommand,
    StopTriggerSubCommand,
    ListTriggerSubCommand,
)


class TriggerCommand(SubCommandMixin, BaseCommand):
    """
    Handles trigger management commands.

    This class facilitates the management of triggers via a set of subcommands. It
    enables operations such as inspecting, pausing, resuming, stopping, and listing
    triggers. The class belongs to the EXECUTION command category.

    Subcommands
    -----------
    stop: Stop a trigger.
    resume: Resume a paused trigger.
    list: List all triggers.
    inspect: Inspect a trigger.
    pause: Pause a trigger.

    :ivar category: The category of the command to which this class belongs.
    :type category: CommandCategory
    :ivar name: The name of the command.
    :type name: str
    :ivar help: A brief description of the command's purpose.
    :type help: str
    :ivar subcommands: A mapping of subcommand names to their corresponding
        implementations.
    :type subcommands: dict
    """

    category = CommandCategory.EXECUTION
    name = "trigger"
    help = "Manage triggers"

    subcommands = {
        "inspect": InspectTriggerSubCommand,
        "pause": PauseTriggerSubCommand,
        "resume": ResumeTriggerSubCommand,
        "stop": StopTriggerSubCommand,
        "list": ListTriggerSubCommand,
    }
