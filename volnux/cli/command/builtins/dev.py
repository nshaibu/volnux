from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from ..base import BaseCommand, CommandCategory
from volnux.exceptions import CommandError
from ..mixins import ProjectMixin, CheckMixin

logger = logging.getLogger(__name__)


class DevCommand(ProjectMixin, CheckMixin, BaseCommand):
    """
    Start the Volnux development server.

    Loads the project from the current directory, performs infrastructure
    checks, and starts the engine with hot reload and verbose output.

    Usage:
        volnux dev
        volnux dev --host 127.0.0.1 --port 8080
        volnux dev --log-level DEBUG
        volnux dev --no-reload

    What it starts:
        - REST API on --host:--port
        - TriggerEngine (all registered triggers active)
        - RehydrationManager
        - Prometheus metrics on --metrics-port
        - File watcher (hot reload on .py and .pty changes)
    """

    help = "Start the development server with hot reload"
    name = "dev"
    category = CommandCategory.DEVELOPMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--host",
            default=os.environ.get("VOLNUX_API_HOST", "127.0.0.1"),
            help="Bind address for the REST API (default: 127.0.0.1)",
        )
        parser.add_argument(
            "--port",
            type=int,
            default=int(os.environ.get("VOLNUX_API_PORT", "8080")),
            help="Port for the REST API (default: 8080)",
        )
        parser.add_argument(
            "--metrics-port",
            type=int,
            default=int(os.environ.get("VOLNUX_PROMETHEUS_PORT", "9464")),
            dest="metrics_port",
            help="Port for Prometheus metrics (default: 9464)",
        )
        parser.add_argument(
            "--log-level",
            default=os.environ.get("VOLNUX_LOG_LEVEL", "DEBUG"),
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            dest="log_level",
            help="Log level (default: DEBUG)",
        )
        parser.add_argument(
            "--no-reload",
            action="store_true",
            default=False,
            help="Disable hot reload on file changes",
        )
        parser.add_argument(
            "--no-checks",
            action="store_true",
            default=False,
            help="Skip backend connectivity checks on startup",
        )
        parser.add_argument(
            "--project-dir",
            default=None,
            dest="project_dir",
            help="Path to the Volnux project directory (default: auto-detect)",
        )

    def handle(self, *args, **options) -> Optional[str]:  # noqa: C901
        host = options["host"]
        port = options["port"]
        metrics_port = options["metrics_port"]
        log_level = options["log_level"]
        no_reload = options["no_reload"]
        no_checks = options["no_checks"]
        project_dir = (
            Path(options["project_dir"]) if options.get("project_dir") else None
        )

        # Set environment for development mode
        os.environ.setdefault("VOLNUX_ENV", "development")
        os.environ.setdefault("VOLNUX_LOG_LEVEL", log_level)
        os.environ.setdefault("VOLNUX_LOG_FORMAT", "text")
        os.environ.setdefault("VOLNUX_API_DOCS", "true")

        # Resolve and load a project
        if project_dir is None:
            try:
                project_dir = self.resolve_project_dir()
            except CommandError as exc:
                self.error(str(exc))
                return None

        self._print_banner(project_dir)

        # Load project configuration
        try:
            engine = self.initialise_workflows(project_dir)
        except CommandError as exc:
            self.error(str(exc))
            return None

        project_name = getattr(engine, "project_name", project_dir.name)
        project_version = getattr(engine, "version", "dev")

        self.stdout.write(
            f"  {self.style.NOTICE('Project:')}  "
            f"{self.style.BOLD(project_name)} {project_version}"
        )
        self.stdout.write(f"  {self.style.NOTICE('Directory:')} {project_dir}")
        self.stdout.write("")

        # Pre-flight infrastructure checks
        if not no_checks:
            asyncio.run(self.run_checks(engine))

        # Print service URLs
        self.stdout.write(self.style.NOTICE("\n  Starting services:"))
        self.stdout.write(
            f"   {self.style.SUCCESS('●')} REST API   →  "
            f"{self.style.BOLD(f'http://{host}:{port}')}"
        )
        self.stdout.write(
            f"   {self.style.SUCCESS('●')} Swagger UI →  "
            f"{self.style.BOLD(f'http://{host}:{port}/api/v1/docs')}"
        )
        self.stdout.write(
            f"   {self.style.SUCCESS('●')} Metrics    →  "
            f"http://{host}:{metrics_port}/metrics"
        )

        reload_status = (
            self.style.WARNING("disabled (--no-reload)")
            if no_reload
            else self.style.SUCCESS("enabled  (.py  .pty)")
        )
        self.stdout.write(f"   {self.style.SUCCESS('●')} Hot reload →  {reload_status}")
        self.stdout.write(f"\n  {self.style.WARNING('Press Ctrl+C to stop.')}\n")
        self.stdout.write("─" * 60 + "\n")

        # Start the engine
        try:
            asyncio.run(
                self._run_dev_server(
                    engine=engine,
                    host=host,
                    port=port,
                    metrics_port=metrics_port,
                    log_level=log_level,
                    reload=not no_reload,
                    project_dir=project_dir,
                )
            )
        except KeyboardInterrupt:
            self.stdout.write(
                f"\n{self.style.WARNING('  Shutting down development server...')}"
            )
            self.stdout.write(self.style.SUCCESS("  Goodbye.\n"))
        except Exception as exc:
            self.error(f"Server error: {exc}")
            logger.exception("Development server error")

        return None

    def _print_banner(self, project_dir: Path) -> None:
        self.stdout.write("\n" + "─" * 60)
        self.stdout.write(self.style.BOLD("\n  Volnux Development Server\n"))
        self.stdout.write("─" * 60)

    async def _run_dev_server(
        self,
        engine,
        host: str,
        port: int,
        metrics_port: int,
        log_level: str,
        reload: bool,
        project_dir: Path,
    ) -> None:
        """
        Start the Volnux API server in development mode.
        Sets up a graceful shutdown on SIGTERM/SIGINT.
        When reload=True, watches .py and .pty files for changes.
        """
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_level),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )

        # Graceful shutdown event
        stop_event = asyncio.Event()

        def _handle_signal(sig, _frame):
            self.stdout.write(
                f"\n{self.style.WARNING(f'  Received {signal.Signals(sig).name} — stopping...')}"
            )
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

        try:
            from volnux.server import create_app
            from volnux.server import start_api_server

            await start_api_server(
                engine=engine,
                host=host,
                port=port,
                metrics_port=metrics_port,
                log_level=log_level.lower(),
                reload=reload,
                project_dir=project_dir,
                stop_event=stop_event,
                dev_mode=True,
            )

        except ImportError:
            # Engine server module not yet implemented — fallback stub
            logger.debug("volnux.server not found — running stub dev loop")
            self.stdout.write(
                self.style.WARNING(
                    "  [stub] volnux.server not found. "
                    "Engine will run when implemented."
                )
            )
            # Keep alive until signal
            while not stop_event.is_set():
                await asyncio.sleep(0.5)

        # Checkpoint drain on shutdown
        drain_timeout = float(os.environ.get("VOLNUX_CHECKPOINT_DRAIN_TIMEOUT", "30"))
        self.stdout.write(
            f"  Draining checkpoint queue (timeout: {drain_timeout:.0f}s)..."
        )
        try:
            checkpoint_manager = getattr(engine, "checkpoint_manager", None)
            if checkpoint_manager and hasattr(checkpoint_manager, "drain"):
                await asyncio.wait_for(
                    checkpoint_manager.drain(), timeout=drain_timeout
                )
        except asyncio.TimeoutError:
            self.warning(f"  Checkpoint drain timed out after {drain_timeout:.0f}s.")
        except Exception as exc:
            logger.warning("Checkpoint drain error: %s", exc)

        self.stdout.write(self.style.SUCCESS("  Checkpoint drain complete."))
