from volnux.cli.command.mixins import SubCommandMixin
from volnux.cli.command.base import CommandCategory, BaseCommand
from volnux.cli.command.builtins.workflow.subcommands import (
    InitWorkflowCommand,
    ListWorkflowsCommand,
    RunWorkflowCommand,
    ValidateWorkflowCommand,
)


class WorkflowCommand(SubCommandMixin, BaseCommand):
    """
    Represents a command for managing workflows in a development context.

    The WorkflowCommand class provides a set of subcommands to assist
    with the initialization, listing, execution, and validation of workflows.
    This command is categorized under development commands and intended to
    enhance workflow-related operations.

    :ivar help: Brief description of the command's purpose.
    :type help: str
    :ivar name: The name of the command.
    :type name: str
    :ivar category: The category to which this command belongs.
    :type category: CommandCategory
    :ivar subcommands: A dictionary mapping subcommand names to their
        respective command classes.
    :type subcommands: dict[str, type]
    """

    help = "Workflow management commands"
    name = "workflow"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    subcommands = {
        "init": InitWorkflowCommand,
        "list": ListWorkflowsCommand,
        "run": RunWorkflowCommand,
        "validate": ValidateWorkflowCommand,
    }
