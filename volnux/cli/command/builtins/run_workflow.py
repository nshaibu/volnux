import argparse
import json
from typing import Optional
from pathlib import Path

from ..base import BaseCommand, CommandCategory, CommandError


class RunWorkflowCommand(BaseCommand):
    help = "Run a workflow"
    name = "run"
    category = CommandCategory.EXECUTION

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("workflow", help="Name of the workflow to run")
        parser.add_argument(
            "--params",
            help="JSON string of parameters to pass to workflow",
            required=False,
        )
        parser.add_argument(
            "--type",
            help="Type of the workflow to run",
            default="single",
            choices=["single", "batch"],
        )

    def handle(self, *args, **options) -> Optional[str]:
        workflow_name = options["workflow"]
        params = json.loads(options["params"]) if options.get("params") else {}
        w_type = options["type"]

        project_dir, _ = self.get_project_root_and_config_module()

        self.success(f"\nRunning workflow: {workflow_name}\n")
        self.stdout.write(f"\nParameters: {params}\n")

        engine = self.initialise_workflows(project_dir, workflow_name)
        workflow_registry = engine.get_workflows_registry()

        workflow = workflow_registry.get_workflow_config(workflow_name)
        if not workflow:
            raise CommandError(
                f"Workflow {workflow_name} not found in project {project_dir}"
            )

        if not workflow.is_executable:
            self.error(f"Workflow {workflow_name} not executable")
            return None

        workflow.run_workflow(params=params, run_type=w_type)

        return None
