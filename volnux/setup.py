import typing
import warnings
import logging.config
from pathlib import Path
from asgiref import sync
from volnux.engine.workflows.trigger.engine import WorkflowConfigExecutor

if typing.TYPE_CHECKING:
    from volnux.engine.workflows.trigger import TriggerEngine

__all__ = ["initialise_workflows"]


async def initialise_workflows(
    project_path: Path, workflow_name: typing.Optional[str] = None
) -> "TriggerEngine":
    """
    Initialise workflow registry.
    :param project_path: Project root directory.
    :param workflow_name: The workflow name. If None, load all workflows.
    :return: Trigger engine.
    """
    from volnux.conf import ConfigLoader
    from volnux.engine.workflows import get_workflow_registry
    from volnux.engine.workflows.trigger import get_trigger_engine

    system_configuration = await sync.sync_to_async(
        ConfigLoader.get_lazily_loaded_config
    )(None)

    logging.config.dictConfig(system_configuration.LOGGING_CONFIG)

    trigger_engine = get_trigger_engine()
    if trigger_engine.is_running():
        logging.getLogger(__name__).warning(
            "Trigger engine is already running. Shutting it down."
        )

        try:
            await trigger_engine.stop()
            logging.getLogger(__name__).info("Trigger engine stopped.")
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Failed to stop trigger engine due to: %s", e
            )
            warnings.warn(
                f"Failed to stop trigger engine due to: {str(e)}", RuntimeWarning
            )

    workflow_registry = get_workflow_registry()

    # configure trigger engine
    if not trigger_engine.has_workflow_executor():
        trigger_engine.set_workflow_executor(WorkflowConfigExecutor(workflow_registry))

    if not workflow_registry.is_ready():
        await workflow_registry.populate_local_workflow_configs(
            project_path, workflow_name
        )
        await workflow_registry.load_workflow_configs()

    return trigger_engine
