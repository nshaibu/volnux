import sys
import logging
import typing
from pathlib import Path

from volnux import Event
from volnux.event.base import EventType
from volnux.import_utils import load_module_from_path
from .utils import initialize_and_register_workflow

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ..registry import WorkflowRegistry


class LoadFromLocal(Event):
    """
    Represents a specialized event class responsible for loading and processing workflow
    configurations from local directories.

    This class implements functionality to discover, validate, and load workflow definitions
    from a local directory containing a valid workflow module. The primary purpose is to
    assist in the management and initialization of workflows that are locally stored.

    :ivar name: Identifies the name of the event. Defaults to "local".
    :type name: str
    :ivar event_type: Specifies the type of event. Defaults to EventType.SYSTEM.
    :type event_type: EventType
    """

    name = "local"

    event_type = EventType.SYSTEM
    checkpointing_enabled = False

    def _load_local_workflow(
        self, workflow_file: Path, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        """Load a workflow configuration from a local directory."""
        from ..workflow import WorkflowConfig

        loading_status = False
        workflow_dir = workflow_file.parent

        try:
            module = load_module_from_path("workflow", workflow_file)
            workflow_config = None
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, WorkflowConfig)
                    and attr != WorkflowConfig
                ):
                    initialize_and_register_workflow(attr, workflow_dir, registry)
                    logger.info(f"  ✓ Loaded local workflow: {workflow_file.name}")
                    loading_status = True
                    break

            return loading_status, workflow_config
        except ImportError as e:
            logger.error(
                f"  ✗ Error loading workflow '{workflow_file}': {e}", exc_info=e
            )
            loading_status = False

        return loading_status, None

    async def process(
        self, workflow_dir: Path, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        if not workflow_dir.exists():
            return False, f"Workflows directory not found: {workflow_dir}"

        logger.info(f"Discovering local workflows in: {workflow_dir}")

        workflow_dir_str = str(workflow_dir.parent)
        if workflow_dir_str not in sys.path:
            sys.path.insert(0, workflow_dir_str)

        # Check if workflow.py exists
        workflow_file = workflow_dir / "workflow.py"
        if not workflow_file.exists():
            logger.debug(f"  <UNK> No workflow found at {workflow_file}")
            return False, f"Workflow file not found: {workflow_file}"

        # Load the workflow configuration
        status, _ = self._load_local_workflow(workflow_file, registry)
        if not status:
            logger.error(f"Failed to load workflow from: {workflow_dir}")

        return True, None
