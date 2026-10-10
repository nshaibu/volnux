import typing
import warnings
import logging.config
from pathlib import Path

if typing.TYPE_CHECKING:
    from volnux.engine.workflows.trigger import TriggerEngine

__all__ = ["initialise_workflows"]

logger = logging.getLogger(__name__)


async def initialise_workflows(
    project_path: Path, workflow_name: typing.Optional[str] = None
) -> "TriggerEngine":
    """
    Initialise the workflow registry and trigger the engine.

    Loads project configuration, sets up logging, discovers workflows,
    registers triggers and sources, and returns a configured trigger engine
    ready to execute workflows.

    Args:
        project_path: Project root directory containing settings.py and workflows/.
        workflow_name: Specific workflow to load. If None, loads all workflows.

    Returns:
        Configured TriggerEngine with all workflows registered and ready.

    Raises:
        FileNotFoundError: If project_path does not exist.
        RuntimeError: If the trigger engine cannot be cleanly restarted.
    """
    if not project_path.exists():
        raise FileNotFoundError(f"Project path does not exist: {project_path}")

    from volnux.config import VolnuxConfig
    from volnux.import_utils import import_string as import_class
    from volnux.engine.workflows import get_workflow_registry
    from volnux.engine.workflows.trigger import get_trigger_engine
    from volnux.engine.workflows.trigger.engine import (
        WorkflowConfigExecutor,
        BaseWorkflowConfigExecutor,
    )

    # Load project configuration
    volnux_config = VolnuxConfig.get_instance()
    init_file = project_path / "init.py"

    if init_file.exists():
        await volnux_config.load_from_file_async(init_file)
        logger.info("Loaded project configuration from %s", init_file)
    else:
        logger.debug(
            "No init.py found at %s — using default configuration", project_path
        )

    # Configure logging from project settings
    logging.config.dictConfig(volnux_config.LOGGING_CONFIG)

    # Handle the existing trigger engine (e.g., hot reload)
    trigger_engine = get_trigger_engine()

    if trigger_engine.is_running():
        logger.warning(
            "Trigger engine is already running — stopping before reinitializing"
        )

        try:
            await trigger_engine.stop()
            logger.info("Trigger engine stopped successfully")
        except Exception as e:
            logger.error("Failed to stop trigger engine: %s", e)
            raise RuntimeError(
                f"Failed to stop running trigger engine. "
                f"Cannot reinitialize safely. Error: {e}"
            ) from e

    workflow_registry = get_workflow_registry()

    if not workflow_registry.is_ready():
        await workflow_registry.populate_local_workflow_configs(
            project_path, workflow_name
        )
        logger.info(
            "Discovered %d workflow(s)%s",
            workflow_registry.workflow_count,
            f" (filtered by '{workflow_name}')" if workflow_name else "",
        )

        await workflow_registry.load_workflow_configs()
        logger.info("Workflow configurations loaded and ready")
    else:
        logger.debug("Workflow registry already populated — skipping discovery")

    # Configure a workflow executor if not already set by init.py
    if not trigger_engine.has_workflow_executor():
        executor: typing.Union[str, typing.Type["BaseWorkflowConfigExecutor"]] = (
            await volnux_config.get_async(
                "DEFAULT_WORKFLOW_EXECUTOR", WorkflowConfigExecutor
            )
        )

        if isinstance(executor, str):
            executor: typing.Type[BaseWorkflowConfigExecutor] = import_class(executor)  # type: ignore[assignment]

        trigger_engine.set_workflow_executor(executor(workflow_registry))
        logger.debug("Set default workflow executor: WorkflowConfigExecutor")

    logger.info(
        "Initialization complete — trigger engine ready with %d workflow(s)",
        workflow_registry.workflow_count,
    )

    return trigger_engine
