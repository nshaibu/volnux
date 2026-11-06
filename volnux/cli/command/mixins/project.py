import asyncio
import types
from typing import Optional, Tuple, cast
from pathlib import Path

from volnux.setup import get_project_init_module, initialise_workflows
from volnux.import_utils import load_module_from_path
from volnux.exceptions import CommandError
from volnux.engine.workflows.trigger.engine import TriggerEngine, WorkflowExecutionError


class ProjectMixin:
    """Provides project loading and engine initialization for CLI commands."""

    def resolve_project_dir(self) -> Path:
        """Walk up from cwd to find the project root."""
        cwd = Path.cwd()
        for directory in [cwd, *cwd.parents]:
            if (directory / "init.py").exists() and (
                (directory / "settings.py").exists()
                or (directory / "config.py").exists()  # (before settings.py)
            ):
                return directory
        raise CommandError(
            "No Volnux project found in the current directory or any parent.\n"
            "Run this command from your project root, or create a project with:\n"
            "  volnux init <project_name>"
        )

    async def _initialise_workflows(
        self, project_dir: Path, workflow_name: Optional[str] = None
    ) -> TriggerEngine:
        """Initialize the engine for a project directory."""
        # init.py is run once, by initialise_workflows (as the project's
        # configuration); the engine it declares is read from that run rather than
        # by running init.py again.
        engine = await initialise_workflows(project_dir, workflow_name)
        if workflow_name is not None:
            return engine

        module = get_project_init_module(project_dir)
        if module is None:
            return engine  # a project without an init.py
        declared = getattr(module, "engine", None)
        if declared is None:
            raise CommandError(
                f"init.py in {project_dir} does not define an 'engine' variable.\n"
                "Ensure init.py contains:\n"
                "  engine = initialise_workflows(project_dir)"
            )
        if not isinstance(declared, TriggerEngine):
            raise CommandError(
                f"init.py 'engine' is not a TriggerEngine. "
                f"Got {type(declared).__name__}."
            )
        return declared

    def initialise_workflows(
        self, project_dir: Path, workflow_name: Optional[str] = None
    ) -> TriggerEngine:
        """Sync wrapper for CLI commands outside an async context."""
        try:
            return asyncio.run(self._initialise_workflows(project_dir, workflow_name))
        except CommandError:
            raise
        except Exception as e:
            raise CommandError(f"Failed to initialize workflows: {e}") from e

    def load_project_config(self) -> Optional[types.ModuleType]:
        """
        Load the project's settings.py from the current directory (or the config.py
        projects had before they had a settings file).

        Returns:
            Dictionary containing the configuration attributes, or None if not found.

        Raises:
            CommandError: If a config file exists but cannot be loaded or is invalid.
        """
        config_path = Path.cwd() / "settings.py"
        if not config_path.exists() and (Path.cwd() / "config.py").exists():
            config_path = Path.cwd() / "config.py"
            self.warning(
                "This project has a config.py: rename it settings.py (Volnux "
                "settings are read from settings.py)"
            )

        if not config_path.exists():
            self.warning("No settings.py found in current directory")
            return None

        if not config_path.is_file():
            raise CommandError(
                f"{config_path.name} exists but is not a file: {config_path}"
            )

        try:
            config = load_module_from_path("project_config", config_path)

            self.success(f"Loaded project configuration from {config_path}\n")
            return config

        except SyntaxError as e:
            raise CommandError(
                f"Syntax error in {config_path.name} at line {e.lineno}: {e.msg}"
            )
        except Exception as e:
            raise CommandError(f"Failed to load {config_path.name}: {str(e)}")

    def get_project_root_and_config_module(
        self,
    ) -> Tuple[Path, types.ModuleType]:
        """
        Get the project root directory and module path.
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
