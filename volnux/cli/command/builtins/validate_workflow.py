import argparse
from typing import Optional
from pathlib import Path

from ..base import BaseCommand, CommandCategory, CommandError


class ValidateWorkflowCommand(BaseCommand):
    help = "Validate workflow definitions"
    name = "validate"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "workflow", nargs="?", help="Specific workflow to validate (optional)"
        )

    def handle(self, *args, **options) -> Optional[str]:
        workflow_name = options.get("workflow")

        project_dir, _ = self.get_project_root_and_config_module()

        engine = self.initialise_workflows(project_dir, workflow_name)
        workflows_registry = engine.get_workflows_registry()

        if workflow_name:
            self.success(f"Validating workflow: {workflow_name}\n")
            workflow = workflows_registry.get_workflow_config(workflow_name)
            if not workflow:
                raise CommandError(
                    f"Workflow {workflow_name} not found in project {project_dir}"
                )
            for index, issue in enumerate(workflow.check()):
                self.warning(f"Workflow {workflow_name}/{index}: {issue}\n")
        else:
            self.success("\nValidating all workflows...")
            for workflow in workflows_registry.get_workflow_configs():
                self.success(f"\nValidating workflow {workflow.name}...\n")
                for index, issue in enumerate(workflow.check()):
                    self.warning(f"Workflow {workflow.name}/{index}: {issue}\n")

        return None
