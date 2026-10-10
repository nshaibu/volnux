import re
import argparse
import json
import shutil
from pathlib import Path
from typing import Optional

from volnux import __version__ as version

from ..base import BaseCommand, CommandCategory
from volnux.exceptions import CommandError


class InitProjectCommand(BaseCommand):
    """
    Scaffold a new Volnux project in the given directory.

    :ivar help: Help message describing the command functionality.
    :type help: str
    :ivar name: The name of the command as recognized in the CLI.
    :type name: str
    :ivar category: The category under which the command will be organized.
    :type category: CommandCategory

    Creates the standard project layout:

        <project_folder>/
            config.py — project configuration
            init.py — workflow initialiser (defines engine)
            workflows/
                __init__.py
            events/
                __init__.py

    Usage
    ─────
        volnux init my_project
        volnux init my_project --template etl
        volnux init my_project --template ai-agent --no-events
    """

    help = "Scaffold a new Volnux project directory."
    name = "init"
    category = CommandCategory.PROJECT_MANAGEMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "name",
            help="Name of the project (alphanumeric, hyphens, and underscores only)",
        )
        parser.add_argument(
            "--path",
            default=None,
            help="Custom path where the project should be created (default: current directory)",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Overwrite existing project directory if it exists",
        )

    def handle(self, *args, **options) -> Optional[str]:
        project_name = options["name"]
        custom_path = options.get("path")
        force = options.get("force", False)

        self._validate_project_name(project_name)

        base_path = Path(custom_path) if custom_path else Path.cwd()
        project_path = base_path / project_name

        self._check_project_existence(project_path, project_name, force)

        self.success(f"Creating Volnux project '{project_name}'...")

        try:
            self._create_directory_structure(project_path)

            workflow_initializer_file = project_path / "init.py"
            workflow_initializer_file.touch(exist_ok=True)
            initialiser_content = self._get_rendered_template(
                "workflow_initialiser_template.txt", params={}
            )
            workflow_initializer_file.write_text(initialiser_content, encoding="utf-8")

            self._create_config_file(project_path, project_name)
            self._create_readme(project_path, project_name)
            self._create_gitignore(project_path)

            self._display_success_message(project_name, project_path)

        except Exception as e:
            # Cleanup on failure
            self._cleanup_on_failure(project_path)
            raise CommandError(f"Failed to create project: {str(e)}")

        return None

    def _validate_project_name(self, name: str) -> None:
        """Validate that the project name follows naming conventions."""
        if not name:
            raise CommandError("Project name cannot be empty")

        # Check for valid characters (alphanumeric, hyphens, underscores)
        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            raise CommandError(
                f"Invalid project name '{name}'. "
                "Use only letters, numbers, hyphens, and underscores."
            )

        if not name[0].isalpha():
            raise CommandError("Project name must start with a letter")

        if len(name) > 100:
            raise CommandError("Project name is too long (max 100 characters)")

    def _check_project_existence(
        self, project_path: Path, project_name: str, force: bool
    ) -> None:
        """Check if project already exists and handle accordingly."""
        if project_path.exists():
            if not force:
                raise CommandError(
                    f"Project '{project_name}' already exists at {project_path}. "
                    "Use --force to overwrite."
                )
            else:
                self.warning(f"Overwriting existing project at {project_path}")
                shutil.rmtree(project_path)

    def _create_directory_structure(self, project_path: Path) -> None:
        """Create the project directory structure."""
        dirs = [
            project_path,
            project_path / "workflows",
            project_path / "commands",
            project_path / "logs",
            project_path / "tests",
        ]

        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
            if dir_path.name in ["workflows", "commands", "tests"]:
                (dir_path / "__init__.py").touch()

    def _create_config_file(self, project_path: Path, project_name: str) -> None:
        """Create the project configuration file."""
        config_content = self._get_rendered_template(
            "project_config.txt",
            params={"version": version, "project_name": project_name},
        )

        config_file = project_path / "config.py"
        config_file.write_text(config_content, encoding="utf-8")

    def _create_readme(self, project_path: Path, project_name: str) -> None:
        """Create the project README file."""
        readme_content = self._get_rendered_template(
            "readme_template.txt", params={"project_name": project_name}
        )

        readme_file = project_path / "README.md"
        readme_file.write_text(readme_content, encoding="utf-8")

    def _create_gitignore(self, project_path: Path) -> None:
        """Create a .gitignore file for the project."""
        gitignore_content = self._get_rendered_template("gitignore_template.txt", {})
        gitignore_file = project_path / ".gitignore"
        gitignore_file.write_text(gitignore_content, encoding="utf-8")

    def _cleanup_on_failure(self, project_path: Path) -> None:
        """Remove the partially created project directory on failure."""
        if project_path.exists():
            try:
                shutil.rmtree(project_path)
                self.warning(f"Cleaned up partially created project at {project_path}")
            except Exception as cleanup_error:
                self.error(f"Failed to cleanup: {cleanup_error}")

    def _display_success_message(self, project_name: str, project_path: Path) -> None:
        """Display a success message and next steps."""
        self.success(
            f"\n✓ Project '{project_name}' created successfully at {project_path}!"
        )
        self.stdout.write("\nNext steps:\n")
        self.stdout.write(f"  1. cd {project_name}\n")
        self.stdout.write(f"  2. volnux list\n")
        self.stdout.write(f"  3. volnux run sample_workflow\n")
