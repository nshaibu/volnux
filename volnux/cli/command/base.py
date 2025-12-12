import argparse
import logging
import sys
import types
import typing
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Optional, Any, Dict
from pathlib import Path

from volnux.registry import Registry
from volnux.setup import initialise_workflows
from volnux.import_utils import load_module_from_path
from volnux.engine.workflows import WorkflowRegistry
from volnux.engine.workflows.trigger.engine import TriggerEngine, WorkflowExecutionError

from .style import Style

_command_registry = Registry()


logger = logging.getLogger(__name__)


def get_command_registry():
    return _command_registry


class CommandError(Exception):
    """Exception raised for command errors."""

    pass


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


class CommandMeta(ABCMeta):
    def __new__(mcs, name, bases, namespace, **kwargs):
        """
        Called when a new class is created.
        Automatically registers the class with the global registry.
        """
        cls = super().__new__(mcs, name, bases, namespace)

        # Register it if it's not the base class
        if name != "BaseCommand" and any(
            isinstance(base, CommandMeta) for base in bases
        ):
            try:
                _command_registry.register(cls, getattr(cls, "name", None))
            except RuntimeError as e:
                logger.warning(str(e))

        return cls


def get_commands_by_category(category: CommandCategory) -> typing.List["BaseCommand"]:
    """
    Return commands registered for the given category.
    Args:
        category (CommandCategory): Category to look up commands for.
    Returns:
         List["BaseCommand"]: Commands registered for the given category.
    """
    commands = []
    for command in _command_registry.list_all_classes():
        command = typing.cast(BaseCommand, command)
        if command.category == category:
            if command not in commands:
                commands.append(command)

    return commands


class BaseCommand(metaclass=CommandMeta):
    """
    Base class for all Volnux commands.
    Similar to Django's BaseCommand.
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

    def create_parser(self, prog_name: str, subcommand: str) -> argparse.ArgumentParser:
        """
        Create and return the ArgumentParser for this command.
        """
        parser = argparse.ArgumentParser(
            prog=f"{prog_name} {subcommand}",
            description=self.help or None,
        )
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """
        Entry point for subclassed commands to add custom arguments.
        """
        pass

    def print_help(self, prog_name: str, subcommand: str) -> None:
        """
        Print the help message for this command.
        """
        parser = self.create_parser(prog_name, subcommand)
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

    @classmethod
    def _get_rendered_template(
        cls, template_name: str, params: typing.Dict[str, typing.Any]
    ) -> str:
        """Get and render workflow template content with provided parameters.

        Args:
            template_name: Name of the template file (e.g., 'workflow.yaml')
            params: Dictionary of parameters to substitute in the template

        Returns:
            Rendered template content as a string

        Raises:
            FileNotFoundError: If the template file doesn't exist
            ValueError: If template_name is empty or contains path traversal
            KeyError: If required template parameters are missing
            Exception: For other rendering errors
        """
        # Validate template name
        if not template_name or not template_name.strip():
            raise ValueError("Template name cannot be empty")

        # Prevent directory traversal attacks
        if ".." in template_name or template_name.startswith("/"):
            raise ValueError(f"Invalid template name: {template_name}")

        # Construct template path
        current_dir = Path(__file__).parent
        templates_dir = current_dir / "builtins" / "templates"
        template_file_path = templates_dir / template_name

        # Ensure the resolved path is still within templates_dir
        try:
            template_file_path = template_file_path.resolve()
            templates_dir = templates_dir.resolve()
            if not str(template_file_path).startswith(str(templates_dir)):
                raise ValueError(
                    f"Template path outside allowed directory: {template_name}"
                )
        except (OSError, RuntimeError) as e:
            raise ValueError(f"Invalid template path: {template_name}") from e

        if not template_file_path.exists():
            raise FileNotFoundError(
                f"Template file not found: {template_name} "
                f"(expected at: {template_file_path})"
            )

        if not template_file_path.is_file():
            raise ValueError(f"Template path is not a file: {template_name}")

        try:
            content = template_file_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            raise IOError(f"Failed to read template file {template_name}: {e}") from e

        # Render template with parameters
        try:
            return content.format(**params)
        except KeyError as e:
            raise KeyError(
                f"Missing required parameter in template {template_name}: {e}"
            ) from e
        except (ValueError, IndexError) as e:
            raise Exception(f"Error rendering template {template_name}: {e}") from e

    async def _initialise_workflows(
        self, project_dir: Path, workflow_name: typing.Optional[str] = None
    ) -> TriggerEngine:
        """
        Initialise workflow registry.
        :param project_dir: Project directory
        :param workflow_name: workflow name
        :return: Registry
        """
        if workflow_name is None:
            workflows_initialiser = load_module_from_path(
                "initialiser", project_dir / "init.py"
            )
            if not workflows_initialiser:
                raise CommandError(
                    f"Failed to load workflow initializer module from path: {project_dir / 'init.py'}"
                )

            engine = typing.cast(TriggerEngine, workflows_initialiser.engine)
        else:
            engine = await initialise_workflows(project_dir, workflow_name)

        try:
            workflows_registry = engine.get_workflows_registry()
        except WorkflowExecutionError:
            raise CommandError(
                "Workflow executor was not provided. The framework has not been initialized yet."
            )

        if not workflows_registry.is_ready():
            raise CommandError("Workflow registry is not ready yet, try again later.")
        return engine

    def load_project_config(self) -> Optional[types.ModuleType]:
        """
        Load the config.py file from the current directory.

        Returns:
            Dictionary containing the configuration attributes, or None if not found.

        Raises:
            CommandError: If a config file exists but cannot be loaded or is invalid.
        """
        config_path = Path.cwd() / "config.py"

        if not config_path.exists():
            self.warning("No config.py found in current directory")
            return None

        if not config_path.is_file():
            raise CommandError(f"config.py exists but is not a file: {config_path}")

        try:
            config = load_module_from_path("project_config", config_path)

            self.success(f"Loaded project configuration from {config_path}\n")
            return config

        except SyntaxError as e:
            raise CommandError(f"Syntax error in config.py at line {e.lineno}: {e.msg}")
        except Exception as e:
            raise CommandError(f"Failed to load config.py: {str(e)}")

    def get_project_root_and_config_module(
        self,
    ) -> typing.Tuple[Path, types.ModuleType]:
        """
        Get project root directory and module path.
        Returns:
             Project root directory and module path.
        Raises:
            CommandError: If a config file exists but cannot be loaded or is invalid.
        """
        config_module = self.load_project_config()
        if not config_module:
            raise CommandError(
                "You are not in any active project. Run 'volnux startproject' first."
            )

        project_dir: Path = getattr(config_module, "PROJECT_DIR", None)
        if not project_dir:
            raise CommandError(
                "You are not in any project. Run 'volnux startproject' first."
            )
        return project_dir, config_module

    def success(self, message: str) -> None:
        """Write a success message."""
        self.stdout.write(self.style.SUCCESS(message))

    def warning(self, message: str) -> None:
        """Write a warning message."""
        self.stdout.write(self.style.WARNING(message))

    def error(self, message: str) -> None:
        """Write an error message."""
        self.stderr.write(self.style.ERROR(message))
