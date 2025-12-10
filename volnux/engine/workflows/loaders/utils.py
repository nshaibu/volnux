import typing
from pathlib import Path

from volnux.exceptions import PointyNotExecutable

if typing.TYPE_CHECKING:
    from ..workflow import WorkflowConfig
    from ..registry import WorkflowRegistry


def get_workflow_class_name(workflow_name: str) -> str:
    """Convert workflow name to workflow class name"""
    return "".join(word.capitalize() for word in workflow_name.split("_"))


def get_workflow_config_name(workflow_name: str) -> str:
    """Convert a workflow name to its corresponding WorkflowConfig class name."""
    return get_workflow_class_name(workflow_name) + "Config"


def initialize_and_register_workflow(
    workflow_class: typing.Type["WorkflowConfig"],
    workflow_dir: Path,
    registry: "WorkflowRegistry",
) -> "WorkflowConfig":
    """
    Register and initialize a workflow.
    :param workflow_class: The workflow class to initialize.
    :param workflow_dir: working directory to initialize workflow from.
    :param registry: workflow registry
    :return:  initialized workflow config
    """
    workflow_config = workflow_class(workflow_path=workflow_dir)
    try:
        workflow_config.discover_workflow_submodules()
        workflow_config.is_executable = True
    except PointyNotExecutable:
        workflow_config.is_executable = False
    registry.register(workflow_config)
    return workflow_config
