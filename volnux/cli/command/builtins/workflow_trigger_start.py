import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

from ..base import BaseCommand, CommandCategory, CommandError


class StartTriggersCommand(BaseCommand):
    """Start all workflow triggers in the project"""

    help = "Start all workflow triggers to begin monitoring and execution"
    name = "start_triggers"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def add_arguments(self, parser) -> None:
        """Add command arguments"""
        parser.add_argument(
            "--daemon",
            "-d",
            action="store_true",
            help="Run trigger engine as a background daemon process",
        )
        parser.add_argument(
            "--pid-file",
            type=str,
            default=".volnux_triggers.pid",
            help="Path to PID file for daemon mode (default: .volnux_triggers.pid)",
        )

    def handle(self, *args, **options) -> Optional[str]:
        daemon_mode = options.get("daemon", False)
        pid_file = options.get("pid_file", ".volnux_triggers.pid")

        project_dir, _ = self.get_project_root_and_config_module()

        if daemon_mode:
            return self._handle_daemon_mode(pid_file, project_dir)
        else:
            return self._handle_foreground_mode(project_dir)

    def _handle_daemon_mode(self, pid_file: str, project_dir: Path) -> Optional[str]:
        """Handle daemon mode startup"""
        pid_path = project_dir / pid_file

        # Check if already running
        if pid_path.exists():
            try:
                with open(pid_path, "r") as f:
                    pid = int(f.read().strip())

                # Check if process is actually running
                try:
                    os.kill(pid, 0)  # Signal 0 just checks if process exists
                    raise CommandError(
                        f"Trigger engine is already running (PID: {pid}). "
                        f"Stop it first or remove {pid_file} if it's stale."
                    )
                except OSError:
                    pid_path.unlink()
                    self.warning(f"Removed stale PID file: {pid_file}")
            except (ValueError, IOError):
                pid_path.unlink()
                self.warning(f"Removed invalid PID file: {pid_file}")

        self.stdout.write("\n")
        self.stdout.write(self.style.BOLD("Starting Trigger Engine in Daemon Mode:"))
        self.stdout.write("\n")

        # Fork to background
        try:
            pid = os.fork()
            if pid > 0:
                # Parent process
                self.success(f"✓ Trigger engine started in background (PID: {pid})")
                self.stdout.write("\n")
                self.stdout.write(f"  PID file: {pid_path}\n")
                self.stdout.write(f"  Stop with: volnux stop-triggers\n\n")
                return None
        except OSError as e:
            raise CommandError(f"Failed to fork daemon process: {e}")

        # Child process continues here
        # Decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # Second fork to prevent zombie process
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            sys.exit(1)

        with open(pid_path, "w") as f:
            f.write(str(os.getpid()))

        # Redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()

        # Redirect to log files or /dev/null
        log_dir = project_dir / "logs"
        log_dir.mkdir(mode=0o755, exist_ok=True)

        stdin = open(os.devnull, "r")
        stdout = open(log_dir / "triggers.log", "a")
        stderr = open(log_dir / "triggers.error.log", "a")

        os.dup2(stdin.fileno(), sys.stdin.fileno())
        os.dup2(stdout.fileno(), sys.stdout.fileno())
        os.dup2(stderr.fileno(), sys.stderr.fileno())

        # Run the engine
        try:
            self._run_engine()
        finally:
            if pid_path.exists():
                pid_path.unlink()

        return None

    def _handle_foreground_mode(self, project_dir: Path) -> Optional[str]:
        """Handle foreground mode startup"""

        try:
            self._display_workflows_and_triggers(project_dir)

            try:
                self._run_engine()
            except KeyboardInterrupt:
                self.stdout.write("\n")
                self.warning("Stopping trigger engine...")
                project_dir, _ = self.get_project_root_and_config_module()
                engine = self.initialise_workflows(project_dir)
                asyncio.run(engine.stop())
                self.success("✓ Trigger engine stopped successfully")

        except Exception as e:
            raise CommandError(f"Failed to start triggers: {e}")

        return None

    def _display_workflows_and_triggers(self, project_dir: Path) -> int:
        """Display workflows and their triggers, return total trigger count"""
        engine = self.initialise_workflows(project_dir)
        workflows_registry = engine.get_workflows_registry()

        # Get all workflows and their triggers
        workflows = list(workflows_registry.get_workflow_configs())

        if not workflows:
            self.warning("No workflows found to start.")
            return 0

        self.stdout.write("\n")
        self.stdout.write(self.style.BOLD("Starting Workflow Triggers:"))
        self.stdout.write("\n")

        # Display workflows and their triggers
        total_triggers = 0
        for workflow in workflows:
            if workflow.is_executable and hasattr(workflow, "triggers"):
                triggers = (
                    workflow.triggers.get_all()
                    if hasattr(workflow.triggers, "get_all")
                    else []
                )
                trigger_count = len(triggers)

                if trigger_count > 0:
                    total_triggers += trigger_count
                    self.stdout.write(f"\n  {workflow.name}")
                    for trigger in triggers:
                        trigger_type = (
                            trigger.trigger_type.value
                            if hasattr(trigger.trigger_type, "value")
                            else str(trigger.trigger_type)
                        )
                        self.stdout.write(
                            f"    └─ {trigger_type} trigger (ID: {trigger.trigger_id[:8]}...)"
                        )

        if total_triggers == 0:
            self.warning("\nNo triggers found in any workflow.")
            self.stdout.write("\n")
            return 0

        self.stdout.write("\n")
        self.stdout.write(
            self.style.SUCCESS(f"Starting {total_triggers} trigger(s)...")
        )
        self.stdout.write("\n")

        return total_triggers

    def _run_engine(self):
        """Initialize and run the engine"""
        project_dir, _ = self.get_project_root_and_config_module()
        engine = self.initialise_workflows(project_dir)

        asyncio.run(engine.start())

        self.success(f"✓ Trigger engine started successfully")
        self.stdout.write("\n")
        self.stdout.write("Trigger engine is now running. Press Ctrl+C to stop.\n")

        asyncio.run(self._keep_running(engine))

        return None

    async def _keep_running(self, engine):
        """Keep the engine running until interrupted"""
        try:
            while engine.is_running():
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
