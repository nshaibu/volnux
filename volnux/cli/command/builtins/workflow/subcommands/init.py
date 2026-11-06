import re
import keyword
import argparse
import shutil
from pathlib import Path
from typing import Optional

from volnux import __version__ as version
from volnux.engine.workflows.loaders.utils import (
    get_workflow_config_name,
    get_workflow_class_name,
)
from volnux.cli.command.base import SubCommand, CommandCategory, CommandError


class InitWorkflowCommand(SubCommand):
    """Command to scaffold a new workflow with configuration, pipeline, and event files."""

    help = "Create a new workflow with all necessary scaffolding"
    name = "init"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Add command-line arguments for workflow creation."""
        parser.add_argument(
            "name", help="Name of the workflow (e.g., 'data_processing')"
        )
        parser.add_argument(
            "--mode",
            default="cfg",
            choices=["cfg", "dag"],
            help="Pointy script parser mode (default: cfg)",
        )
        parser.add_argument(
            "--event-template",
            dest="event_template",
            default="class",
            choices=["function", "class"],
            help="Event template type to use (default: class)",
        )
        parser.add_argument(
            "--create-batch-pipeline",
            dest="create_batch_pipeline",
            action="store_true",
            help="Create batch pipeline in addition to standard pipeline",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Overwrite existing workflow if it exists",
        )

    def handle(self, *args, **options) -> Optional[str]:
        """Execute the workflow creation command."""
        workflow_name = options["name"]
        mode = options["mode"].upper()
        event_template = options["event_template"]
        should_create_batch_pipeline = options["create_batch_pipeline"]
        force = options.get("force", False)

        # Validate workflow name
        self._validate_workflow_name(workflow_name)

        # Load and validate project configuration
        project_dir = self._get_project_directory()
        workflows_dir = project_dir / "workflows"

        workflow_dir = workflows_dir / workflow_name
        if workflow_dir.exists():
            if not force:
                raise CommandError(
                    f"Workflow '{workflow_name}' already exists. "
                    f"Use --force to overwrite."
                )
            self.stdout.write(
                self.style.WARNING(f"Overwriting existing workflow '{workflow_name}'")
            )

        # Create workflow directory
        workflow_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._create_workflow_config(workflow_dir, workflow_name, mode)
            self._create_pipeline_file(workflow_dir, workflow_name)

            if should_create_batch_pipeline:
                self._create_batch_pipeline_file(workflow_dir, workflow_name)

            self._create_events_file(workflow_dir, event_template)
            self._create_pointy_script(workflow_dir, workflow_name, mode)
            self._create_init_file(workflow_dir, workflow_name)

        except Exception as e:
            # Clean up on failure
            if workflow_dir.exists() and not force:
                shutil.rmtree(workflow_dir)
            raise CommandError(f"Failed to create workflow: {str(e)}")

        try:
            self._register_workflow_in_config(project_dir, workflow_name)
        except Exception as e:
            self.stdout.write(
                self.style.WARNING(
                    f"Warning: Could not automatically register workflow in config.py: {str(e)}"
                )
            )
            self.stdout.write(
                self.style.WARNING(
                    f"Please manually add 'workflows.{workflow_name}.workflow.{get_workflow_config_name(workflow_name)}' "
                    f"to WORKFLOWS list in config.py"
                )
            )

        # Success message
        self.success(f"Workflow '{workflow_name}' created successfully!")
        self.stdout.write(f"\nWorkflow structure:\n")
        self.stdout.write(f"  workflows/{workflow_name}/\n")
        self.stdout.write(f"    ├── __init__.py\n")
        self.stdout.write(f"    ├── workflow.py\n")
        self.stdout.write(f"    ├── pipeline.py\n")
        if should_create_batch_pipeline:
            self.stdout.write(f"    ├── batch_pipeline.py\n")
        self.stdout.write(f"    ├── events.py\n")
        self.stdout.write(
            f"    └── {get_workflow_class_name(workflow_name).lower()}.pty\n"
        )
        self.stdout.write(f"\nNext steps:\n")
        self.stdout.write(
            f"  1. Edit workflow configuration: workflows/{workflow_name}/workflow.py\n"
        )
        self.stdout.write(
            f"  2. Define pipeline logic: workflows/{workflow_name}/pipeline.py\n"
        )
        self.stdout.write(
            f"  3. Configure events: workflows/{workflow_name}/events.py\n"
        )
        self.stdout.write(
            f"  4. Define structure of your workflow: workflows/{workflow_name}/{get_workflow_class_name(workflow_name).lower()}.pty\n"
        )

        return None

    def _validate_workflow_name(self, name: str) -> None:
        """Validate workflow name follows Python naming conventions."""
        if not name:
            raise CommandError("Workflow name cannot be empty")

        if not name.replace("_", "").isalnum():
            raise CommandError(
                f"Invalid workflow name '{name}'. "
                f"Use only letters, numbers, and underscores."
            )

        if name[0].isdigit():
            raise CommandError(f"Workflow name '{name}' cannot start with a digit")

        # Check for reserved Python keywords
        if keyword.iskeyword(name):
            raise CommandError(f"Workflow name '{name}' is a reserved Python keyword")

    def _get_project_directory(self) -> Path:
        """Load project configuration and return project directory."""
        project_config = self.load_project_config()
        if not project_config:
            raise CommandError(
                "Project configuration not found. " "Run 'volnux startproject' first."
            )

        project_dir = getattr(project_config, "PROJECT_DIR", None)
        if not project_dir:
            raise CommandError(
                "Project directory not found in configuration. "
                "Run 'volnux startproject' first."
            )

        workflows_dir = project_dir / "workflows"
        if not workflows_dir.exists():
            raise CommandError(
                f"Workflows directory not found at {workflows_dir}. "
                f"Not in a Volnux project. Run 'volnux startproject' first."
            )

        return project_dir

    def _create_workflow_config(
        self, workflow_dir: Path, workflow_name: str, mode: str
    ) -> None:
        """Create workflow configuration file."""
        workflow_config_file = workflow_dir / "workflow.py"

        workflow_script_content = self._get_rendered_template(
            "workflow_config_template.txt",
            params={
                "workflow_config_class_name": get_workflow_config_name(workflow_name),
                "workflow_name": workflow_name,
                "workflow_version": version,
                "mode": mode,
            },
        )
        workflow_config_file.write_text(workflow_script_content, encoding="utf-8")
        self.stdout.write(f"  Created: workflow.py")

    def _create_pipeline_file(self, workflow_dir: Path, workflow_name: str) -> None:
        """Create pipeline file."""
        pipeline_file = workflow_dir / "pipeline.py"

        pipeline_script_content = self._get_rendered_template(
            "pipeline_template.txt",
            params={"template_pipeline_name": get_workflow_class_name(workflow_name)},
        )
        pipeline_file.write_text(pipeline_script_content, encoding="utf-8")
        self.stdout.write(f"  Created: pipeline.py")

    def _create_batch_pipeline_file(
        self, workflow_dir: Path, workflow_name: str
    ) -> None:
        """Create a batch pipeline file."""
        batch_pipeline_file = workflow_dir / "batch_pipeline.py"

        batch_pipeline_script_content = self._get_rendered_template(
            "batch_pipeline_template.txt",
            params={
                "template_pipeline_name": get_workflow_class_name(workflow_name),
                "template_batch_pipeline": (
                    get_workflow_class_name(workflow_name) + "Batch"
                ),
            },
        )
        batch_pipeline_file.write_text(batch_pipeline_script_content, encoding="utf-8")
        self.stdout.write(f"  Created: batch_pipeline.py")

    def _create_events_file(self, workflow_dir: Path, event_template: str) -> None:
        """Create an events file based on a template type."""
        events_file = workflow_dir / "events.py"

        template_name = (
            "function_based_events_template.txt"
            if event_template == "function"
            else "class_based_events_template.txt"
        )

        events_script_content = self._get_rendered_template(template_name, params={})
        events_file.write_text(events_script_content, encoding="utf-8")
        self.stdout.write(f"  Created: events.py ({event_template}-based)")

    def _create_pointy_script(
        self, workflow_dir: Path, workflow_name: str, mode: str
    ) -> None:
        """Create a pointy script file."""
        pointy_script_file = (
            workflow_dir / f"{get_workflow_class_name(workflow_name).lower()}.pty"
        )

        pointy_script_content = self._get_rendered_template(
            "pointy_template.txt", params={"mode": mode}
        )
        pointy_script_file.write_text(pointy_script_content, encoding="utf-8")
        self.stdout.write(
            f"  Created: {get_workflow_class_name(workflow_name).lower()}.py"
        )

    def _create_init_file(self, workflow_dir: Path, workflow_name: str) -> None:
        """Create __init__.py to make the workflow directory a Python package."""
        init_file = workflow_dir / "__init__.py"
        pipeline_class_name = get_workflow_class_name(workflow_name)

        init_content = f'''"""Workflow package initialization."""
# This was autogenerated by volnux {version}

from .pipeline import {pipeline_class_name}

__all__ = ["{pipeline_class_name}"]
        '''
        init_file.write_text(init_content, encoding="utf-8")

    def _register_workflow_in_config(
        self, project_dir: Path, workflow_name: str
    ) -> None:
        """Register the workflow in config.py WORKFLOWS list."""
        config_file = project_dir / "config.py"

        if not config_file.exists():
            raise FileNotFoundError(f"config.py not found at {config_file}")

        # Read the config file
        config_content = config_file.read_text(encoding="utf-8")

        # Generate the dotted path notation
        workflow_config_class = get_workflow_config_name(workflow_name)
        workflow_path = f"workflows.{workflow_name}.workflow.{workflow_config_class}"

        # Check if the workflow is already registered
        if workflow_path in config_content:
            self.stdout.write(
                self.style.WARNING(
                    f"Workflow '{workflow_path}' already registered in config.py"
                )
            )
            return

        # Pattern to match WORKFLOWS = [...] with various formatting
        workflows_pattern = r"(WORKFLOWS\s*=\s*\[)(.*?)(\])"

        match = re.search(workflows_pattern, config_content, re.DOTALL)

        if not match:
            raise ValueError("WORKFLOWS list not found in config.py")

        before, workflows_content, after = match.groups()

        # Check if the list is empty or has items
        workflows_content = workflows_content.strip()

        if workflows_content:
            # List has items, add comma and new workflow
            # Remove trailing comma if it exists
            workflows_content = workflows_content.rstrip(",").strip()
            new_workflows_content = f'{workflows_content},\n    "{workflow_path}",\n'
        else:
            # Empty list, just add the workflow
            new_workflows_content = f'\n    "{workflow_path}",\n'

        # Reconstruct the file content
        new_config_content = config_content.replace(
            match.group(0), f"{before}{new_workflows_content}{after}"
        )

        # Write back to config.py
        config_file.write_text(new_config_content, encoding="utf-8")

        self.stdout.write(
            self.style.SUCCESS(f"✓ Registered '{workflow_path}' in config.py")
        )
