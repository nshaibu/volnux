import threading
import time
import typing
import warnings
import logging.config
from pathlib import Path

if typing.TYPE_CHECKING:
    from volnux.engine.workflows.trigger import TriggerEngine

__all__ = [
    "initialise_workflows",
    "get_project_init_module",
    "shutdown_observability",
]

logger = logging.getLogger(__name__)

# init.py files that an initialise_workflows is loading right now (see there)
_loading_init_files: typing.Set[str] = set()
_loading_lock = threading.Lock()
# The module of each project's init.py as the last initialise_workflows ran it
_init_modules: typing.Dict[str, typing.Any] = {}


def get_project_init_module(project_path: Path) -> typing.Optional[typing.Any]:
    """The ``init.py`` module of ``project_path`` as the last
    ``initialise_workflows`` for it ran it (None if it has none or has not run).

    The CLI reads the project's ``engine`` from here instead of running
    ``init.py`` a second time.
    """
    return _init_modules.get(str((Path(project_path) / "init.py").resolve()))


def shutdown_observability() -> None:
    """Shut down the telemetry ``initialise_workflows`` started (a process that owns
    its lifetime calls this before exiting; nothing to do if it is not running)."""
    from volnux.observability import shutdown_observability as shutdown

    shutdown()


async def initialise_workflows(
    project_path: Path, workflow_name: typing.Optional[str] = None
) -> "TriggerEngine":
    """
    Initialise the workflow registry and trigger the engine.

    Loads project configuration, sets up logging, starts telemetry, discovers
    workflows, registers triggers and sources, and returns a configured trigger
    engine ready to execute workflows.

    The project's ``init.py`` is run once, to read its configuration. If it calls
    this function itself (``engine = initialise_workflows(project_dir)``, as the one
    ``volnux init`` writes does), that nested call returns the engine un-wired and
    this call wires it after the settings are loaded.

    Telemetry (``OTEL_ENABLED``) starts as soon as the project's configuration is
    loaded (the ``OTEL_*`` settings may come from its ``init.py``), so everything
    after that is traced: a ``setup.initialise_workflows`` span with one child per
    phase (``setup.discover_workflows``, ``setup.load_workflows``, ...), and the
    ``setup.duration`` metric. The time the configuration took is recorded on that
    span as ``setup.config_load_ms``. Telemetry never makes this function fail.

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
    from volnux.engine.workflows.trigger import get_trigger_engine
    from volnux.observability import (
        record_setup_duration,
        setup_phase,
        start_tracing_for_setup,
    )

    started = time.perf_counter()

    # Load project configuration
    volnux_config = VolnuxConfig.get_instance()
    init_file = project_path / "init.py"

    if init_file.exists():
        # init.py is run once, here, to read its configuration. The init.py that
        # `volnux init` writes calls initialise_workflows itself (``engine = ...``):
        # that nested call, made while this one is loading init.py, gets the engine
        # back without any wiring, and this call wires it below once the settings
        # init.py defines are in place. (Without this the nested call loaded
        # init.py again, which called it again, until the process ran out of
        # threads; with only a recursion guard init.py still ran twice, and the
        # wiring ran before its own settings were applied.)
        key = str(init_file.resolve())
        with _loading_lock:
            nested = key in _loading_init_files
            if not nested:
                _loading_init_files.add(key)
        if nested:
            logger.debug(
                "%s is being loaded by an enclosing initialise_workflows; "
                "returning the engine to be wired by it",
                init_file,
            )
            return get_trigger_engine()
        try:
            module = await volnux_config.load_from_file_async(init_file)
        finally:
            with _loading_lock:
                _loading_init_files.discard(key)
        _init_modules[key] = module
        logger.info("Loaded project configuration from %s", init_file)
    else:
        logger.debug(
            "No init.py found at %s — using default configuration", project_path
        )

    # Configure logging from project settings
    logging.config.dictConfig(volnux_config.LOGGING)
    config_load_ms = (time.perf_counter() - started) * 1000.0

    # Start telemetry now that the configuration (init.py included) is loaded.
    # Off by default; never raises.
    role = str(setting("ROLE", "api"))
    start_tracing_for_setup(
        project_name=str(setting("PROJECT_NAME", project_path.name)),
        version=str(setting("VERSION", "unknown")),
        role=role,
    )

    outcome = "error"
    try:
        with setup_phase(
            "initialise_workflows",
            **{
                "project.path": str(project_path),
                "workflow.filter": workflow_name,
                "setup.config_load_ms": config_load_ms,
                "role": role,
            },
        ) as root_span:
            engine = await _wire_engine(project_path, workflow_name, root_span)
            outcome = "success"
            return engine
    finally:
        record_setup_duration(time.perf_counter() - started, outcome)


def _annotate(span: typing.Any, **attributes: typing.Any) -> None:
    """Put attributes on a setup span (None when telemetry is off)."""
    if span is None:
        return
    from volnux.otel.instrumentations._common import set_attributes

    set_attributes(span, attributes)


async def _wire_engine(
    project_path: Path, workflow_name: typing.Optional[str], root_span: typing.Any
) -> "TriggerEngine":
    """Everything after the configuration: reporters, the registry, the executor."""
    from volnux.config import setting
    from volnux.import_utils import import_string as import_class
    from volnux.observability import setup_phase
    from volnux.engine.workflows import get_workflow_registry
    from volnux.engine.workflows.trigger import get_trigger_engine
    from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor
    from volnux.reporting import ensure_pipeline_reporters_installed

    # Install the configured pipeline reporters (``PIPELINE_REPORTERS``) now
    # that the project's own configuration — init.py — has been loaded, so the
    # list is read after the deployment has had a chance to set it. At most
    # once per process; a no-op when nothing is configured.
    with setup_phase("configure_reporters"):
        ensure_pipeline_reporters_installed()

    # Handle the existing trigger engine (e.g., hot reload)
    trigger_engine = get_trigger_engine()

    if trigger_engine.is_running():
        logger.warning(
            "Trigger engine is already running — stopping before reinitializing"
        )

        with setup_phase("stop_running_engine"):
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
        with setup_phase("discover_workflows") as span:
            await workflow_registry.populate_local_workflow_configs(
                project_path, workflow_name
            )
            _annotate(
                span,
                **{"setup.sources": len(workflow_registry.combined_workflow_sources)},
            )
        logger.info(
            "Discovered %d source(s)%s",
            len(workflow_registry.combined_workflow_sources),
            f" (filtered by '{workflow_name}')" if workflow_name else "",
        )

        with setup_phase("load_workflows") as span:
            await workflow_registry.load_workflow_configs()
            _annotate(
                span,
                **{
                    "setup.workflows": workflow_registry.workflow_count,
                    "setup.failed_sources": len(workflow_registry.load_errors),
                },
            )
        logger.info("Workflow configurations loaded and ready")
    else:
        logger.debug("Workflow registry already populated — skipping discovery")

    # Configure a workflow executor if not already set by init.py
    if not trigger_engine.has_workflow_executor():
        with setup_phase("configure_executor") as span:
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
            _annotate(span, **{"executor.class": executor_class.__name__})
            logger.debug("Set default workflow executor: %s", executor_class.__name__)

    _annotate(root_span, **{"setup.workflows": workflow_registry.workflow_count})
    logger.info(
        "Initialization complete — trigger engine ready with %d workflow(s)",
        workflow_registry.workflow_count,
    )

    return trigger_engine
