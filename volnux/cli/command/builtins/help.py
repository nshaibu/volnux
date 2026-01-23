import argparse
from typing import Optional

from ..base import (
    BaseCommand,
    CommandCategory,
    CommandError,
    get_command_registry,
    get_commands_by_category,
)


class HelpCommand(BaseCommand):
    help = "Show help information and list all available commands"
    name = "help"
    category = CommandCategory.HELP

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "command", nargs="?", help="Command to show help for (optional)"
        )

    def handle(self, *args, **options) -> Optional[str]:
        command_name = options.get("command")

        if command_name:
            # Show help for specific command
            loader = get_command_registry()
            command_class = loader.get_by_name(command_name)

            if not command_class:
                raise CommandError(f"Unknown command: '{command_name}'")
            command = command_class()
            command.print_help("volnux", command_name)
            return None

        # Show general help and list all commands
        self._print_general_help()
        return None

    def _print_general_help(self) -> None:
        """Print comprehensive help information."""
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(
            self.style.BOLD("\n  VOLNUX - Workflow Orchestration Framework")
        )
        self.stdout.write("\n" + "=" * 70 + "\n")

        self.stdout.write(self.style.NOTICE("\nUSAGE:"))
        self.stdout.write("  volnux <command> [options] [args]\n")

        self.stdout.write(self.style.NOTICE("\nDESCRIPTION:"))
        self.stdout.write(
            "  Volnux is a powerful workflow orchestration framework that allows"
        )
        self.stdout.write(
            "  you to define, manage, and execute complex workflows with ease.\n"
        )

        # List all commands grouped by category
        self._print_commands_by_category()

        self.stdout.write(self.style.NOTICE("\n\nGETTING STARTED:\n"))
        self.stdout.write("  1. Create a new project:\n")
        self.stdout.write("     \t$ volnux startproject myproject\n")
        self.stdout.write("  2. Navigate to your project:\n")
        self.stdout.write("     \t$ cd myproject\n")
        self.stdout.write("  3. List available workflows:\n")
        self.stdout.write("     \t$ volnux list\n")
        self.stdout.write("  4. Run a workflow:\n")
        self.stdout.write("     \t$ volnux run sample_workflow\n")

        self.stdout.write(self.style.NOTICE("\nEXAMPLES:\n"))
        self.stdout.write("  # Create a new project\n")
        self.stdout.write("  \t$ volnux startproject data_pipeline\n")
        self.stdout.write("  # Create a new workflow with DAG template\n")
        self.stdout.write("  \t$ volnux startworkflow etl_process --template=dag\n")
        self.stdout.write("  # Run workflow with parameters\n")
        self.stdout.write(
            '  \t$ volnux run etl_process --params \'{"source": "db", "target": "s3"}\'\n'
        )
        self.stdout.write("  # Validate all workflows\n")
        self.stdout.write("  \t$ volnux validate\n")
        self.stdout.write("  # Get help for specific command\n")
        self.stdout.write("  \t$ volnux help run\n")

        self.stdout.write(self.style.NOTICE("\nFOR MORE INFORMATION:\n"))
        self.stdout.write('  • Use "volnux help <command>" for detailed command help\n')
        self.stdout.write('  • Use "volnux <command> --help" for command options\n')
        self.stdout.write("  • Visit documentation: https://volnux.io/docs\n")

        self.stdout.write("=" * 70 + "\n")

    def _print_commands_by_category(self) -> None:
        loader = get_command_registry()
        self.stdout.write(self.style.NOTICE("\nAVAILABLE COMMANDS:\n"))

        for category in CommandCategory:
            self.stdout.write(f"\n  {self.style.BOLD(category.value)}:\n")

            for command in get_commands_by_category(category):
                command_class = loader.get_by_name(command.name)
                if command_class is None:
                    continue
                command = command_class()
                help_text = command.help if command else ""
                self.stdout.write(f"   • {command.name:<6}: {help_text}\n")

        self.stdout.write("")
