import typing
import warnings
import logging.config
from pathlib import Path

from volnux.engine.workflows.trigger.engine import WorkflowConfigExecutor

if typing.TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

__all__ = ["initialise_workflows"]


def initialise_workflows(
    project_path: Path, workflow_name: typing.Optional[str] = None
) -> "WorkflowRegistry":
    """
    Initialise workflow registry.
    :param project_path: Project root directory.
    :param workflow_name: The workflow name. If None, load all workflows.
    :return: Registry.
    """
    from volnux.conf import ConfigLoader
    from volnux.engine.workflows import get_workflow_registry
    from volnux.engine.workflows.trigger import get_trigger_engine

    system_configuration = ConfigLoader.get_lazily_loaded_config()

    logging.config.dictConfig(system_configuration.LOGGING_CONFIG)

    trigger_engine = get_trigger_engine()
    if trigger_engine.is_running():
        logging.getLogger(__name__).warning(
            "Trigger engine is already running. Shutting it down."
        )

        try:
            trigger_engine.stop()
            logging.getLogger(__name__).info("Trigger engine stopped.")
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Failed to stop trigger engine due to: %s", e
            )
            warnings.warn(
                f"Failed to stop trigger engine due to: {str(e)}", RuntimeWarning
            )

    workflow_registry = get_workflow_registry()
    if not workflow_registry.is_ready():
        workflow_registry.populate_local_workflow_configs(project_path, workflow_name)
        workflow_registry.load_workflow_configs()

    if not trigger_engine.has_workflow_executor():
        trigger_engine.set_workflow_executor(WorkflowConfigExecutor(workflow_registry))
    return workflow_registry
