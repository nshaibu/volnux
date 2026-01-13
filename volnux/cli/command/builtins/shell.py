import sys
import asyncio
import code
from typing import Optional

from ..base import BaseCommand, CommandCategory
from volnux import __version__ as version


class ShellCommand(BaseCommand):
    help = "Start an interactive Python shell with Volnux context"
    name = "shell"
    category = CommandCategory.DEVELOPMENT

    def handle(self, *args, **options) -> Optional[str]:
        self.stdout.write("Starting Volnux interactive shell...\n")

        project_dir, project_config = self.get_project_root_and_config_module()

        local_vars = {
            project_config.__name__: project_config,
        }

        engine = self.initialise_workflows(project_dir)

        workflows_registry = engine.get_workflows_registry()

        for workflow in workflows_registry.get_workflow_configs():
            local_vars[workflow.name] = workflow

        shell_version = f"Python {sys.version} on {sys.platform}"

        code.interact(
            local=local_vars, banner=f"Volnux Shell {version} ({shell_version})"
        )

        return None
