import hashlib
import importlib.util
import logging
import subprocess
import sys
import typing
from pathlib import Path

from volnux import Event
from volnux.base import EventType
from volnux.exceptions import PointyNotExecutable

from .utils import initialize_and_register_workflow

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ..registry import WorkflowRegistry


class LoadFromGit(Event):
    name = "git"

    event_type = EventType.SYSTEM

    def _load_github_workflow(
        self,
        workflow_dir: Path,
        repo: str,
        workflow_name: str,
        registry: "WorkflowRegistry",
    ) -> typing.Tuple[bool, typing.Any]:
        """Load a workflow from a GitHub-cloned directory."""
        from ..workflow import WorkflowConfig

        workflow_file = workflow_dir / "workflow.py"
        if not workflow_file.exists():
            logger.error(f"  ✗ No workflow.py found in {workflow_dir}")
            return False, None

        # Add parent directory to sys.path temporarily
        parent_dir = workflow_dir.parent
        if str(parent_dir) not in sys.path:
            sys.path.insert(0, str(parent_dir))

        loading_status = False

        try:
            # Load module from file
            spec = importlib.util.spec_from_file_location(
                f"github_{repo.replace('/', '_')}_{workflow_name}", workflow_file
            )
            if spec.loader is None:
                raise ImportError(f"Cannot find a loader for {workflow_name}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find WorkflowConfig subclass
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, WorkflowConfig)
                    and attr != WorkflowConfig
                ):
                    # Instantiate the workflow config
                    initialize_and_register_workflow(attr, workflow_dir, registry)
                    logger.info(f"  ✓ Loaded workflow from GitHub: {workflow_name}")
                    loading_status = True
                    break
        except Exception as e:
            loading_status = False
            logger.error(f"  ✗ Error loading workflow: {e}")
        finally:
            # Clean up sys.path
            if str(parent_dir) in sys.path:
                sys.path.remove(str(parent_dir))

        return loading_status, workflow_name

    def process(
        self, location: str, workflow_name: str, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        """
        Install a workflow from any Git repository.

        Example:
            registry.process(
                'https://gitlab.com/myorg/workflows.git',
                'docker_registry',
                branch='develop'
            )
        """

        branch = self.options.extras.get("branch", "main")

        cache_dir = self.options.extras.get("cache_dir")
        if cache_dir is None:
            logger.error("  <UNK> No cache directory configured")
            return False, None

        logger.info(f"Installing workflow from Git: {location}")

        # Create cache directory using hash of git URL
        url_hash = hashlib.md5(location.encode()).hexdigest()[:8]
        repo_cache = cache_dir / f"git_{url_hash}"

        try:
            # Clone or update repository
            if repo_cache.exists():
                print(f"  Updating existing clone...")
                subprocess.check_call(["git", "pull"], cwd=repo_cache)
            else:
                print(f"  Cloning repository...")
                subprocess.check_call(
                    ["git", "clone", "-b", branch, location, str(repo_cache)]
                )

            # Find workflow directory
            workflow_dir = repo_cache / workflow_name
            if not workflow_dir.exists():
                logger.error(f"  ✗ Workflow '{workflow_name}' not found in repository")
                return False, None

            # Load the workflow (reuse GitHub loader)
            return self._load_github_workflow(
                workflow_dir, f"git_{url_hash}", workflow_name, registry
            )

        except subprocess.CalledProcessError as e:
            logger.error(f"  ✗ Failed to clone/update repository: {e}")
            return False, None
