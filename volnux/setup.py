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

    from volnux.config import VolnuxConfig, setting
    from volnux.import_utils import import_string as import_class
    from volnux.engine.workflows import get_workflow_registry
    from volnux.engine.workflows.trigger import get_trigger_engine
    from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor
    from volnux.reporting import ensure_pipeline_reporters_installed

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
    logging.config.dictConfig(volnux_config.LOGGING)

    # Install the configured pipeline reporters (``PIPELINE_REPORTERS``) now
    # that the project's own configuration — init.py — has been loaded, so the
    # list is read after the deployment has had a chance to set it. At most
    # once per process; a no-op when nothing is configured.
    ensure_pipeline_reporters_installed()

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
        executor_path = setting("DEFAULT_WORKFLOW_EXECUTOR", None)
        if executor_path is None:
            from volnux.conf import global_settings

            executor_path = global_settings.DEFAULT_WORKFLOW_EXECUTOR

        executor_class = (
            import_class(executor_path)
            if isinstance(executor_path, str)
            else executor_path
        )
        if not (
            isinstance(executor_class, type)
            and issubclass(executor_class, WorkflowExecutor)
        ):
            raise TypeError(
                f"DEFAULT_WORKFLOW_EXECUTOR must name a WorkflowExecutor subclass, "
                f"got {executor_path!r}"
            )

        trigger_engine.set_workflow_executor(executor_class(workflow_registry))
        logger.debug("Set default workflow executor: %s", executor_class.__name__)

    logger.info(
        "Initialization complete — trigger engine ready with %d workflow(s)",
        workflow_registry.workflow_count,
    )

    return trigger_engine
