import argparse
import logging
import sys
from abc import ABCMeta, abstractmethod, ABC
from enum import Enum
from typing import Optional, List

from volnux.exceptions import CommandError
from volnux.event.registry import Registry

from .style import Style

_command_registry = Registry()


logger = logging.getLogger(__name__)

__all__ = [
    "BaseCommand",
    "CommandCategory",
    "TemplateType",
    "SubCommand",
]


def get_command_registry():
    return _command_registry


class CommandCategory(Enum):
    PROJECT_MANAGEMENT = "Project Management"
    WORKFLOW_MANAGEMENT = "Workflow Management"
    EXECUTION = "Execution"
    DEVELOPMENT = "Development"
    HELP = "Help"
    OTHER = "Other"


class TemplateType(Enum):
    TEXT = "text"
    CLASS = "class"


# class CommandMeta(ABCMeta):
#     def __new__(mcs, name, bases, namespace, **kwargs):
#         """
#         Called when a new class is created.
#         Automatically registers the class with the global registry.
#         """
#         cls = super().__new__(mcs, name, bases, namespace)
#
#         # Register it if it's not the base class
#         if name != "BaseCommand" and any(
#             isinstance(base, CommandMeta) for base in bases
#         ):
#             try:
#                 _command_registry.register(cls, getattr(cls, "name", None))
#             except RuntimeError as e:
#                 logger.warning(str(e))
#
#         return cls


def get_commands_by_category(category: CommandCategory) -> List["BaseCommand"]:
    """
    Return commands registered for the given category.
    Args:
        category (CommandCategory): Category to look up commands for.
    Returns:
         List["BaseCommand"]: Commands registered for the given category.
    """
    commands = []
    for command in _command_registry.list_all_classes():
        # command = typing.cast(BaseCommand, command)
        if command.category == category:
            if command not in commands:
                commands.append(command)

    return commands


class SubCommand(metaclass=ABCMeta):
    """
    SubCommand serves as a base class for defining CLI subcommands. It provides
    common functionalities such as argument parsing, help message printing, and
    execution handling. Subclasses must implement specific logic by overriding
    `handle` and `add_arguments` methods.

    :ivar help: Optional description of the subcommand for help messages.
    :type help: str
    :ivar name: Identifier for the command. Defaults to the class name if not set.
    :type name: Optional[str]
    :ivar category: Category of the command to group similar commands.
    :type category: CommandCategory
    """

    help = ""

    # The name to use for this command. If not provided, it uses the class name
    name = None

    # command category
    category = CommandCategory.OTHER

    def __init__(self):
        self.stdout = sys.stdout
        self.stderr = sys.stderr
        self.style = Style()

    @classmethod
    def get_command_name(cls):
        return cls.name or cls.__name__

    def create_parser(
        self, prog_name: str, command: str, subcommand: Optional[str] = None
    ) -> argparse.ArgumentParser:
        """
        Create and return the ArgumentParser for this command.
        """
        command_str = f"{prog_name} {command}"
        if subcommand:
            command_str = f"{command_str} {subcommand}"

        parser = argparse.ArgumentParser(
            prog=command_str,
            description=self.help or None,
        )
        self.add_arguments(parser)
        return parser

    @abstractmethod
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """
        Entry point for subclassed commands to add custom arguments.
        """
        pass

    def print_help(
        self, prog_name: str, command: str, subcommand: Optional[str] = None
    ) -> None:
        """
        Print the help message for this command.
        """
        parser = self.create_parser(prog_name, command, subcommand)
        parser.print_help()

    def execute(self, *args, **options) -> None:
        """
        Execute the command.
        """
        try:
            output = self.handle(*args, **options)
            if output:
                self.stdout.write(output)
        except CommandError as e:
            self.stderr.write(self.style.ERROR(f"Error: {e}"))
            sys.exit(1)
        except KeyboardInterrupt:
            self.stderr.write(self.style.WARNING("\nOperation cancelled."))
            sys.exit(1)

    @abstractmethod
    def handle(self, *args, **options) -> Optional[str]:
        """
        The actual logic of the command. Subclasses must implement this.
        """
        pass

    def success(self, message: str) -> None:
        """Write a success message."""
        self.stdout.write(self.style.SUCCESS(message))

    def warning(self, message: str) -> None:
        """Write a warning message."""
        self.stdout.write(self.style.WARNING(message))

    def error(self, message: str) -> None:
        """Write an error message."""
        self.stderr.write(self.style.ERROR(message))


class BaseCommand(SubCommand, ABC):
    """
    Base class for all commands.

    This class serves as the base for all command implementations. It provides
    the foundational structure and functionality for creating and managing command
    objects. Subclasses of this class can be automatically registered in the
    command registry unless they are abstract. To effectively use this class,
    inherit from it and implement the required command-specific behavior.

    :ivar name: Optional name of the command. If specified, it determines how the
        command is identified in the registry.
    :type name: str
    """

    def __init_subclass__(cls, **kwargs):

        super().__init_subclass__(**kwargs)

        if getattr(cls, "__abstractmethods__", None):
            return

        command_name = cls.get_command_name()
        try:
            _command_registry.register(cls, command_name)
        except Exception as e:
            logger.warning(f"Could not register {cls.__name__}: {e}")
