import argparse
import sys
from typing import List, Optional

from volnux import __version__ as version

from .command.base import get_command_registry
from .command.style import Style


class VolnuxCLI:
    """
    Main CLI interface for Volnux.
    """

    def __init__(self):
        self.prog_name = "volnux"
        self.loader = get_command_registry()
        self.style = Style()

    def main(self, argv: Optional[List[str]] = None) -> None:
        """Main entry point for the CLI."""
        if argv is None:
            argv = sys.argv[1:]

        parser = argparse.ArgumentParser(
            prog=self.prog_name,
            description="Volnux - Modern Workflow Orchestration Framework",
            epilog=f"Type '{self.prog_name} <command> --help' for help on a specific command.",
        )

        parser.add_argument("--version", action="version", version=f"Volnux {version}")

        parser.add_argument("command", nargs="?", help="Command to execute")

        # Parse known args to get command
        args, remaining = parser.parse_known_args(argv)

        if not args.command:
            self.print_help()
            return

        command_name = args.command
        command_class = self.loader.get_by_name(command_name)

        if not command_class:
            sys.stderr.write(self.style.ERROR(f"Unknown command: '{command_name}'\n"))
            sys.stderr.write(f"Type '{self.prog_name} help' for usage.\n")
            sys.exit(1)

        command = command_class()

        # Parse command-specific arguments
        cmd_parser = command.create_parser(self.prog_name, command_name)
        cmd_options = vars(cmd_parser.parse_args(remaining))

        # Execute command
        command.execute(**cmd_options)

    def print_help(self) -> None:
        """Print main help message."""
        print(f"\n{self.style.BOLD('Volnux - Workflow Orchestration Framework')}\n")
        print("Usage: volnux <command> [options]\n")
        print(f"{self.style.BOLD('Available commands:')}\n")

        commands = self.loader.list_all_classes()
        for cmd in commands:
            # cmd = self.loader.get_by_name(cmd_name)
            help_text = cmd.help if cmd else ""
            print(f"  {cmd.name:<20} {help_text}")

        print(
            f"\nUse 'volnux <command> --help' for more information on a specific command.\n"
        )


def main():
    """Entry point for the volnux command."""
    cli = VolnuxCLI()
    cli.main()
