from volnux.cli.command.mixins import SubCommandMixin
from volnux.cli.command.base import CommandCategory, BaseCommand
from volnux.cli.command.builtins.manifest.subcommands import (
    GenerateManifestSubCommand,
    ValidateManifestSubCommand,
)


class ManifestCommand(SubCommandMixin, BaseCommand):
    """
    Command for managing manifest files.
    """

    category = CommandCategory.DEVELOPMENT
    name = "manifest"
    help = "Manage manifest files"

    subcommands = {
        "generate": GenerateManifestSubCommand,
        "validate": ValidateManifestSubCommand,
    }
