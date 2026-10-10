import contextlib
import hashlib
import importlib.util
import logging
import subprocess
import sys
import typing
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from volnux import Event
from volnux.event.base import EventType
from volnux.manifest.utils import build_authenticated_url, redact_credentials
from volnux.exceptions import SubprocessTimeoutError
from .utils import initialize_and_register_workflow
from volnux.utils import run_command

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ..source import SourceCredentials
    from ..registry import WorkflowRegistry


@contextlib.contextmanager
def _temporary_sys_path(directory: Path) -> typing.Iterator[None]:
    """Temporarily prepend *directory* to ``sys.path``, then restore it."""
    dir_str = str(directory)
    inserted = dir_str not in sys.path
    if inserted:
        sys.path.insert(0, dir_str)
    try:
        yield
    finally:
        if inserted and dir_str in sys.path:
            sys.path.remove(dir_str)


class LoadFromGit(Event):
    name = "git"
    event_type = EventType.SYSTEM

    checkpointing_enabled = False

    @staticmethod
    def _load_workflow_from_directory(
        workflow_dir: Path,
        module_name: str,
        workflow_name: str,
        registry: "WorkflowRegistry",
    ) -> typing.Tuple[bool, typing.Optional[str]]:
        """
        Load a WorkflowConfig subclass from *workflow_dir/workflow.py*
        and register it with *registry*.
        """
        from ..workflow import WorkflowConfig

        workflow_file = workflow_dir / "workflow.py"
        if not workflow_file.exists():
            logger.error("✗ No workflow.py found in %s", workflow_dir)
            return False, None

        with _temporary_sys_path(workflow_dir.parent):
            try:
                spec = importlib.util.spec_from_file_location(
                    module_name, workflow_file
                )
                if spec is None or spec.loader is None:
                    raise ImportError(f"Cannot find a loader for {workflow_name!r}")

                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, WorkflowConfig)
                        and attr is not WorkflowConfig
                    ):
                        initialize_and_register_workflow(attr, workflow_dir, registry)
                        logger.info("✓ Loaded workflow from git: %s", workflow_name)
                        return True, workflow_name

                logger.error("✗ No WorkflowConfig subclass found in %s", workflow_file)
                return False, None

            except Exception as exc:
                logger.error(
                    "✗ Error loading workflow %r: %s", workflow_name, exc, exc_info=True
                )
                return False, None

    @staticmethod
    def _run_git(
        *args: str,
        cwd: typing.Optional[Path] = None,
        timeout: typing.Optional[int] = None,
    ) -> None:
        """
        Run a git command, surfacing stderr on failure.

        Args:
            *args:   git sub-command and arguments (without the "git" prefix).
            cwd:     Working directory for the command.
            timeout: Optional timeout in seconds, applied

        Raises:
            subprocess.CalledProcessError: with stderr attached.
        """
        result = run_command(["git", *args], cwd=cwd, timeout_s=timeout)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode,
                ["git", *args],
                output=result.stdout,
                stderr=result.stderr,
            )

    def process(
        self,
        location: str,
        workflow_name: str,
        registry: "WorkflowRegistry",
        *,
        credentials: typing.Optional["SourceCredentials"] = None,
        branch: str = "main",
        timeout: typing.Optional[int] = None,
    ) -> typing.Tuple[bool, typing.Optional[str]]:
        """
        Install a workflow from any Git repository.

        Args:
            location: Git remote URL (HTTPS or SSH).
            workflow_name: Name of the workflow directory inside the repo.
            registry:      Target workflow registry.
            credentials:   Optional ``SourceCredentials`` for private repositories.
                           Must satisfy ``is_valid()`` when provided.
                           Credentials are embedded in the clone URL for HTTPS
                           remotes; SSH remotes use key-based auth and ignore this.
            branch:        Branch to clone/update. Defaults to ``"main"``.
            timeout:       HTTP operation timeout in seconds passed to git via
                           ``http.timeout``. Has no effect on SSH remotes.

        Returns:
            ``(True, workflow_name)`` on success, ``(False, None)`` on failure.

        Raises:
            ValueError: If ``credentials`` is provided but fails ``is_valid()``.

        Example::

            loader.process(
                "https://gitlab.com/myorg/workflows.git",
                "docker_registry",
                registry=registry,
                credentials=SourceCredentials(token="glpat-xxxx"),
                branch="develop",
                timeout=30,
            )
        """
        if credentials is not None and not credentials.is_valid():
            raise ValueError(
                f"Invalid credentials for {redact_credentials(location)!r}: "
                "provide either a token or both username and password."
            )

        cache_dir: typing.Optional[Path] = self.options.extras.get("cache_dir")
        if cache_dir is None:
            logger.error("✗ No cache_dir configured for git loader")
            return False, None

        safe_location = redact_credentials(location)
        logger.info(
            "Installing workflow from git: %s (branch=%s)", safe_location, branch
        )

        url_hash = hashlib.md5(location.encode(), usedforsecurity=False).hexdigest()[:8]
        repo_cache = cache_dir / f"git_{url_hash}"

        # Resolve the URL to use for network operations (may contain credentials)
        clone_url = (
            build_authenticated_url(credentials, location, "general")
            if credentials is not None
            else location
        )

        try:
            if repo_cache.exists():
                logger.info("Updating existing clone in %s", repo_cache)
                self._run_git("fetch", "origin", cwd=repo_cache, timeout=timeout)
                self._run_git("checkout", branch, cwd=repo_cache, timeout=timeout)
                self._run_git("pull", "origin", branch, cwd=repo_cache, timeout=timeout)
            else:
                logger.info("Cloning %s (branch=%s)", safe_location, branch)
                self._run_git(
                    "clone", "-b", branch, clone_url, str(repo_cache), timeout=timeout
                )

        except SubprocessTimeoutError as exc:
            logger.error(
                "✗ Git timed out after %.1fs for %s", exc.timeout_seconds, safe_location
            )
            return False, None

        except subprocess.CalledProcessError as exc:
            # Redact the command list before logging — it may contain the clone URL
            # with embedded credentials.
            logger.error(
                "✗ Git operation failed (exit %d) for %s:\n%s",
                exc.returncode,
                safe_location,
                exc.stderr or exc.output or "(no output)",
            )
            return False, None

        workflow_dir = repo_cache / workflow_name
        if not workflow_dir.exists():
            logger.error(
                "✗ Workflow %r not found in repository %s", workflow_name, safe_location
            )
            return False, None

        module_name = f"git_{url_hash}_{workflow_name}"
        return self._load_workflow_from_directory(
            workflow_dir, module_name, workflow_name, registry
        )
