import contextlib
import hashlib
import importlib.util
import logging
import re
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

# A branch, tag or commit: no leading '-' (it would be read as an option), no spaces.
_SAFE_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@+-]*$")

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

    @staticmethod
    def _is_safe_subpath(name: str) -> bool:
        """A relative path that stays inside the repository."""
        path = Path(name)
        return bool(name) and not path.is_absolute() and ".." not in path.parts

    @staticmethod
    def _is_remote_branch(repo: Path, ref: str, timeout: typing.Optional[int]) -> bool:
        """True when ``ref`` names a branch of ``origin`` (not a tag or a commit)."""
        result = run_command(
            ["git", "show-ref", "--verify", "--quiet", f"refs/remotes/origin/{ref}"],
            cwd=repo,
            timeout_s=timeout,
        )
        return result.returncode == 0

    def _sync_repository(
        self,
        repo_cache: Path,
        clone_url: str,
        safe_location: str,
        ref: str,
        timeout: typing.Optional[int],
    ) -> None:
        """Bring ``repo_cache`` to ``ref``: a branch (then kept up to date), a tag or a
        commit (then pinned: it does not move).

        The old code cloned with ``-b <ref>`` and pulled ``origin <ref>``, which only
        works for branches and tags - the manifest allows a commit SHA.
        """
        if repo_cache.exists():
            logger.info("Updating existing clone in %s", repo_cache)
            self._run_git(
                "fetch", "--tags", "--force", "origin", cwd=repo_cache, timeout=timeout
            )
        else:
            logger.info("Cloning %s", safe_location)
            self._run_git("clone", clone_url, str(repo_cache), timeout=timeout)

        self._run_git("checkout", ref, cwd=repo_cache, timeout=timeout)
        if self._is_remote_branch(repo_cache, ref, timeout):
            self._run_git(
                "merge", "--ff-only", f"origin/{ref}", cwd=repo_cache, timeout=timeout
            )

    @staticmethod
    def _pip_install_directory(
        directory: Path, timeout: typing.Optional[int]
    ) -> typing.Tuple[bool, str]:
        """``pip install`` a source tree (an event package kept in a Git repository)."""
        result = run_command(
            [sys.executable, "-m", "pip", "install", "--no-input", str(directory)],
            timeout_s=timeout,
        )
        return result.returncode == 0, redact_credentials(
            result.stderr or result.stdout or "(no output)"
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
        as_package: bool = False,
    ) -> typing.Tuple[bool, typing.Optional[str]]:
        """
        Install a package from any Git repository.

        Two kinds of repository are supported:

        * a **workflow** package (default): ``workflow_name`` is the directory inside
          the repository that holds ``workflow.py``; its ``WorkflowConfig`` is
          registered with ``registry``.
        * an **event** package (``as_package=True``): the repository is a Python
          package; it is installed with pip. Its events are registered afterwards
          from the manifest (``registry`` is not used here).

        Args:
            location: Git remote URL (HTTPS or SSH).
            workflow_name: Name of the workflow directory inside the repo (the package
                           name, for an event package).
            registry:      Target workflow registry.
            credentials:   Optional ``SourceCredentials`` for private repositories.
                           Must satisfy ``is_valid()`` when provided.
                           Credentials are embedded in the clone URL for HTTPS
                           remotes; SSH remotes use key-based auth and ignore this.
            branch:        Branch, tag or commit SHA to check out. Defaults to
                           ``"main"``. A branch is kept up to date, a tag or SHA stays
                           where it is.
            timeout:       Timeout in seconds for each git (and pip) command.
            as_package:    Install the repository as an event package.

        Returns:
            ``(True, workflow_name)`` on success, ``(False, None)`` on failure.

        Raises:
            ValueError: If ``credentials`` is provided but fails ``is_valid()``.
        """
        if credentials is not None and not credentials.is_valid():
            raise ValueError(
                f"Invalid credentials for {redact_credentials(location)!r}: "
                "provide either a token or both username and password."
            )

        if not _SAFE_REF_RE.match(branch or ""):
            logger.error("✗ %r is not a valid git branch, tag or commit", branch)
            return False, None
        if not as_package and not self._is_safe_subpath(workflow_name):
            logger.error("✗ %r is not a valid workflow directory", workflow_name)
            return False, None

        cache_dir: typing.Optional[Path] = self.options.extras.get("cache_dir")
        if cache_dir is None:
            logger.error("✗ No cache_dir configured for git loader")
            return False, None

        safe_location = redact_credentials(location)
        logger.info("Installing from git: %s (ref=%s)", safe_location, branch)

        url_hash = hashlib.md5(location.encode(), usedforsecurity=False).hexdigest()[:8]
        repo_cache = Path(cache_dir) / f"git_{url_hash}"

        # Resolve the URL to use for network operations (may contain credentials)
        clone_url = (
            build_authenticated_url(credentials, location, "general")
            if credentials is not None
            else location
        )

        try:
            self._sync_repository(repo_cache, clone_url, safe_location, branch, timeout)

        except SubprocessTimeoutError as exc:
            logger.error(
                "✗ Git timed out after %.1fs for %s", exc.timeout_seconds, safe_location
            )
            return False, None

        except subprocess.CalledProcessError as exc:
            # The command list may contain the clone URL with embedded credentials,
            # so only the redacted output is logged.
            logger.error(
                "✗ Git operation failed (exit %d) for %s:\n%s",
                exc.returncode,
                safe_location,
                redact_credentials(str(exc.stderr or exc.output or "(no output)")),
            )
            return False, None

        if as_package:
            try:
                installed, output = self._pip_install_directory(repo_cache, timeout)
            except SubprocessTimeoutError as exc:
                logger.error(
                    "✗ pip timed out after %.1fs installing %s",
                    exc.timeout_seconds,
                    safe_location,
                )
                return False, None
            if not installed:
                logger.error("✗ pip could not install %s:\n%s", safe_location, output)
                return False, None
            logger.info("✓ Installed %s from git: %s", workflow_name, safe_location)
            return True, workflow_name

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
