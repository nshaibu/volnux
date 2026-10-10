import asyncio
import os
import signal
import sys
from pathlib import Path

from volnux.cli.command.base import SubCommand, CommandCategory, CommandError

__all__ = ["StartTriggerEngineSubCommand"]


class StartTriggerEngineSubCommand(SubCommand):
    """Start all workflow triggers in the project"""

    help = "Start all workflow triggers to begin monitoring and execution"
    name = "start_triggers"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def add_arguments(self, parser) -> None:
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

    def handle(self, *args, **options) -> None:
        daemon_mode = options.get("daemon", False)
        pid_file = options.get("pid_file", ".volnux_triggers.pid")

        project_dir, _ = self.get_project_root_and_config_module()

        if daemon_mode:
            self._handle_daemon_mode(pid_file, project_dir)
        else:
            self._handle_foreground_mode(project_dir)

    def _handle_daemon_mode(self, pid_file: str, project_dir: Path) -> None:
        """Double-fork daemon.

        C3: a pipe carries the grandchild's own PID back to the parent so the
        user is shown the PID of the process that is actually running.
        """
        pid_path = project_dir / pid_file

        if pid_path.exists():
            try:
                with open(pid_path, "r") as f:
                    pid = int(f.read().strip())
                try:
                    os.kill(pid, 0)
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

        # C3: pipe lets the grandchild send its own PID to the parent
        read_fd, write_fd = os.pipe()

        try:
            pid = os.fork()  # first fork
        except OSError as e:
            os.close(read_fd)
            os.close(write_fd)
            raise CommandError(f"Failed to fork daemon process: {e}")

        if pid > 0:

            os.close(write_fd)
            raw = b""
            while True:
                chunk = os.read(read_fd, 64)
                if not chunk:
                    break
                raw += chunk
            os.close(read_fd)
            os.waitpid(pid, 0)  # reap intermediate child; prevents zombie
            try:
                daemon_pid = int(raw.strip())
            except ValueError:
                raise CommandError("Daemon failed to start — no PID received.")
            # C3: printing grandchild PID — the real daemon
            self.success(f"✓ Trigger engine started in background (PID: {daemon_pid})")
            self.stdout.write("\n")
            self.stdout.write(f"  PID file: {pid_path}\n")
            self.stdout.write(f"  Stop with: volnux stop-triggers\n\n")
            return

        os.close(read_fd)

        try:
            os.chdir("/")  # m3: non-fatal if restricted
        except OSError:
            pass
        os.setsid()
        os.umask(0)

        try:
            pid = os.fork()  # second fork
        except OSError as e:
            # M5: log before exiting so the failure is not silent
            sys.stderr.write(f"Second fork failed: {e}\n")
            os.close(write_fd)
            sys.exit(1)

        if pid > 0:
            os.close(write_fd)
            sys.exit(0)  # intermediate child exits cleanly

        # ── GRANDCHILD (actual daemon) ─────────────────────────────────────────
        # C3: send our own PID to the parent through the pipe
        os.write(write_fd, str(os.getpid()).encode())
        os.close(write_fd)

        with open(pid_path, "w") as f:
            f.write(str(os.getpid()))

        sys.stdout.flush()
        sys.stderr.flush()

        log_dir = project_dir / "logs"
        log_dir.mkdir(mode=0o755, exist_ok=True)

        stdin = open(os.devnull, "r")
        stdout = open(log_dir / "triggers.log", "a")
        stderr = open(log_dir / "triggers.error.log", "a")

        os.dup2(stdin.fileno(), sys.stdin.fileno())
        os.dup2(stdout.fileno(), sys.stdout.fileno())
        os.dup2(stderr.fileno(), sys.stderr.fileno())

        # M2: close Python-level wrappers now that dup2 is done;
        #     the underlying FDs (0/1/2) remain open via the dup2 copies
        stdin.close()
        stdout.close()
        stderr.close()

        # M1: initialise once and pass the engine through
        try:
            engine = self.initialise_workflows(project_dir)
            self._run_engine(engine)
        finally:
            if pid_path.exists():
                pid_path.unlink()

    def _handle_foreground_mode(self, project_dir: Path) -> None:
        try:
            # M1: initialise once; share the engine between display and run
            engine = self.initialise_workflows(project_dir)
            self._display_workflows_and_triggers(engine)

            # _run_engine owns the single asyncio.run() call and
            # handles SIGINT / SIGTERM internally — no bare KeyboardInterrupt
            # handler needed here
            self._run_engine(engine)

        except KeyboardInterrupt:
            # Can only fire if the interrupt arrives before _run_engine takes
            # over (i.e. during display). Nothing is running yet.
            self.stdout.write("\n")
            self.warning("Interrupted before engine started.")
        except CommandError:
            raise
        except Exception as e:
            raise CommandError(f"Failed to start triggers: {e}")

    def _display_workflows_and_triggers(self, engine) -> int:
        """Display workflows and their triggers; return total trigger count.

        M1: accepts the already-initialised engine — no second initialise call.
        """
        workflows_registry = engine.get_workflow_registry()
        workflows = list(workflows_registry.get_workflow_configs())

        if not workflows:
            self.warning("No workflows found to start.")
            return 0

        self.stdout.write("\n")
        self.stdout.write(self.style.BOLD("Starting Workflow Triggers:"))
        self.stdout.write("\n")

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
                            f"    └─ {trigger_type} trigger"
                            f" (ID: {trigger.trigger_id[:8]}...)"
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

    def _run_engine(self, engine) -> None:
        """Run the engine inside a single asyncio event loop.

        C1: engine.start(), the keep-alive loop, and engine.stop() all run
        inside ONE asyncio.run() call so background tasks created by start()
        (e.g. _state_sync_loop) remain alive for the full lifetime of the run.

        M1: engine is passed in, not re-created here.
        M4: get_project_root_and_config_module() is not called here.
        """
        asyncio.run(self._run_engine_async(engine))

    async def _run_engine_async(self, engine) -> None:
        """Async entry point: start → keep-alive → stop, all in one event loop.

        C2: engine.stop() is called on the same engine that was started.
        C4: SIGTERM and SIGINT handled inside the loop via
            loop.add_signal_handler so the daemon responds to kill <pid>.
        """
        loop = asyncio.get_running_loop()
        shutdown_event = asyncio.Event()

        def _request_shutdown(sig_name: str) -> None:
            if not shutdown_event.is_set():
                sys.stderr.write(f"\nReceived {sig_name}, shutting down...\n")
                shutdown_event.set()

        # C4: SIGTERM — daemon responds to `kill <pid>` / service managers
        # C4: SIGINT  — foreground Ctrl-C uses the same graceful path as daemon
        loop.add_signal_handler(signal.SIGTERM, _request_shutdown, "SIGTERM")
        loop.add_signal_handler(signal.SIGINT, _request_shutdown, "SIGINT")

        try:
            # C1: start() runs here — its background tasks live in this loop
            await engine.start()

            # m5: no interpolation, so no f-prefix
            self.success("✓ Trigger engine started successfully")
            self.stdout.write("\n")
            self.stdout.write("Trigger engine is now running. Press Ctrl+C to stop.\n")

            # C1: keep-alive runs in the SAME event loop as engine.start()
            await self._keep_running(engine, shutdown_event)

        finally:
            # C2: always stop the engine that was actually started
            try:
                await engine.stop()
                self.success("✓ Trigger engine stopped successfully")
            except Exception as e:
                sys.stderr.write(f"Error during engine stop: {e}\n")

            try:
                loop.remove_signal_handler(signal.SIGTERM)
                loop.remove_signal_handler(signal.SIGINT)
            except (NotImplementedError, RuntimeError):
                pass  # Windows or already-closed loop — safe to ignore

    async def _keep_running(self, engine, shutdown_event: asyncio.Event) -> None:
        """Keep the engine running until interrupted or shutdown is requested.

        M3: CancelledError is re-raised so cancellation propagates correctly.
        C2 / C4: watches shutdown_event set by signal handlers.
        """
        try:
            while engine.is_running():
                try:
                    # Wait up to 1 s or until a shutdown signal arrives
                    await asyncio.wait_for(
                        asyncio.shield(shutdown_event.wait()), timeout=1.0
                    )
                    break  # shutdown_event set — exit the loop
                except asyncio.TimeoutError:
                    continue  # normal 1-second tick; re-check is_running()
        except asyncio.CancelledError:
            # M3: re-raise so the caller's cancellation propagates
            raise
