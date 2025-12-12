from asgiref import sync
from typing import Optional

from ..base import BaseCommand, CommandCategory, CommandError


class ListWorkflowsCommand(BaseCommand):
    help = "List all available workflows"
    name = "list"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def handle(self, *args, **options) -> Optional[str]:
        config_module = self.load_project_config()
        if not config_module:
            raise CommandError(
                "You are not in any active project. Run 'volnux startproject' first."
            )

        self.stdout.write(self.style.BOLD("\nAvailable Workflows:\n"))

        project_dir, _ = self.get_project_root_and_config_module()

        engine = sync.async_to_sync(self._initialise_workflows)(project_dir, None)

        workflows_registry = engine.get_workflows_registry()

        num_of_workflows = 0
        for workflow in workflows_registry.get_workflow_configs():
            num_of_workflows += 1
            if workflow.is_executable:
                self.stdout.write(f"  ✓ {workflow.name}\n")
            else:
                self.stdout.write(f"  ✗ {workflow.name}\n")

        self.stdout.write(f"\nTotal: {num_of_workflows} workflow(s)\n")

        return None
