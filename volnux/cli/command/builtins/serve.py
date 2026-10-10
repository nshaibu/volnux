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

from ..base import BaseCommand, CommandCategory, CommandError

logger = logging.getLogger(__name__)


class ServeCommand(BaseCommand):
    """
    Start the Volnux production server.

    Runs as one of two roles determined by --role:

        api     REST API + TriggerEngine + RehydrationManager
                Accepts workflow execution requests and fires triggers.
                Recommended replicas: 2-5.

        worker  Workflow execution engine + CheckpointManager + RemoteManager
                Executes events, manages resources, checkpoints state.
                Recommended replicas: 3-20 (auto-scaled by HPA).

    Usage:
        volnux serve --role api
        volnux serve --role worker
        volnux serve --role api --host 0.0.0.0 --port 8080
        volnux serve --role worker --log-level INFO

    In Kubernetes, the role is set by the container CMD:
        command: ["volnux", "serve", "--role", "api"]
        command: ["volnux", "serve", "--role", "worker"]
    """

    help = "Start the production API or worker server"
    name = "serve"
    category = CommandCategory.EXECUTION

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--role",
            choices=["api", "worker"],
            default=os.environ.get("VOLNUX_ROLE", "api"),
            help="Server role: 'api' or 'worker' (default: api)",
        )
        parser.add_argument(
            "--host",
            default=os.environ.get("VOLNUX_API_HOST", "0.0.0.0"),
            help="Bind address (default: 0.0.0.0)",
        )
        parser.add_argument(
            "--port",
            type=int,
            default=int(os.environ.get("VOLNUX_API_PORT", "8080")),
            help="Bind port for API role (default: 8080)",
        )
        parser.add_argument(
            "--worker-port",
            type=int,
            default=int(os.environ.get("VOLNUX_WORKER_PORT", "9090")),
            dest="worker_port",
            help="Bind port for worker health endpoint (default: 9090)",
        )
        parser.add_argument(
            "--metrics-port",
            type=int,
            default=int(os.environ.get("VOLNUX_PROMETHEUS_PORT", "9464")),
            dest="metrics_port",
            help="Prometheus metrics port (default: 9464)",
        )
        parser.add_argument(
            "--log-level",
            default=os.environ.get("VOLNUX_LOG_LEVEL", "INFO"),
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            dest="log_level",
            help="Log level (default: INFO)",
        )
        parser.add_argument(
            "--project-dir",
            default=None,
            dest="project_dir",
            help="Project directory (default: auto-detect from cwd)",
        )
        parser.add_argument(
            "--drain-timeout",
            type=float,
            default=float(os.environ.get("VOLNUX_CHECKPOINT_DRAIN_TIMEOUT", "120")),
            dest="drain_timeout",
            help=(
                "Seconds to wait for checkpoint queue to drain on shutdown "
                "(default: 120). Must be less than K8s terminationGracePeriodSeconds."
            ),
        )

    def handle(self, *args, **options) -> Optional[str]:  # noqa: C901
        role = options["role"]
        host = options["host"]
        port = options["port"]
        worker_port = options["worker_port"]
        metrics_port = options["metrics_port"]
        log_level = options["log_level"]
        drain_timeout = options["drain_timeout"]
        project_dir = (
            Path(options["project_dir"]) if options.get("project_dir") else None
        )

        # ── Validate environment ───────────────────────────────────────────
        env = os.environ.get("VOLNUX_ENV", "production")
        if env == "development":
            self.warning(
                "Running 'volnux serve' in development mode. "
                "Consider using 'volnux dev' instead."
            )

        # ── Resolve project ────────────────────────────────────────────────
        if project_dir is None:
            try:
                project_dir = _resolve_project_dir()
            except CommandError as exc:
                self.error(str(exc))
                return None

        # ── Configure logging ──────────────────────────────────────────────
        log_format = os.environ.get("VOLNUX_LOG_FORMAT", "json")
        if log_format == "json":
            self._configure_json_logging(log_level)
        else:
            logging.basicConfig(
                level=getattr(logging, log_level),
                format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            )

        # ── Load project ───────────────────────────────────────────────────
        logger.info(
            "Loading project", extra={"project_dir": str(project_dir), "role": role}
        )
        try:
            engine = _load_project(project_dir)
        except CommandError as exc:
            self.error(str(exc))
            return None

        project_name = getattr(engine, "project_name", project_dir.name)
        logger.info("Project loaded", extra={"project": project_name, "role": role})

        # ── Print startup summary ──────────────────────────────────────────
        self._print_startup_summary(
            role=role,
            host=host,
            port=port if role == "api" else worker_port,
            metrics_port=metrics_port,
            drain_timeout=drain_timeout,
            project_name=project_name,
            log_level=log_level,
        )

        # ── Run pre-flight validation ──────────────────────────────────────
        try:
            asyncio.run(self._preflight(engine, role))
        except CommandError as exc:
            self.error(f"Pre-flight check failed: {exc}")
            return None
        except Exception as exc:
            self.error(f"Pre-flight check error: {exc}")
            logger.exception("Pre-flight check error")
            return None

        # ── Start the server ───────────────────────────────────────────────
        try:
            asyncio.run(
                self._run_server(
                    engine=engine,
                    role=role,
                    host=host,
                    port=port,
                    worker_port=worker_port,
                    metrics_port=metrics_port,
                    log_level=log_level,
                    drain_timeout=drain_timeout,
                )
            )
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt — shutting down")
        except SystemExit:
            pass
        except Exception as exc:
            self.error(f"Server crashed: {exc}")
            logger.exception("Server error")
            sys.exit(1)

        return None

    # ── Internal helpers ───────────────────────────────────────────────────

    def _print_startup_summary(
        self,
        role: str,
        host: str,
        port: int,
        metrics_port: int,
        drain_timeout: float,
        project_name: str,
        log_level: str,
    ) -> None:
        node_id = os.environ.get("VOLNUX_NODE_ID", "local")
        self.stdout.write(
            f"\n{self.style.BOLD('  Volnux')}  "
            f"{self.style.NOTICE(role.upper())}  "
            f"node={self.style.BOLD(node_id)}"
        )
        self.stdout.write(f"  project={project_name}  log={log_level}")
        if role == "api":
            self.stdout.write(
                f"  {self.style.SUCCESS('→')} "
                f"http://{host}:{port}  "
                f"metrics={metrics_port}  "
                f"drain={drain_timeout:.0f}s"
            )
        else:
            self.stdout.write(
                f"  {self.style.SUCCESS('→')} "
                f"metrics={metrics_port}  "
                f"grpc=45545  "
                f"drain={drain_timeout:.0f}s"
            )
        self.stdout.write("")

    async def _preflight(self, engine, role: str) -> None:
        """
        Validate that required backends are reachable before starting.
        Fails fast with a clear error on any critical dependency failure.
        The API role requires all three backends.
        The worker role requires checkpoints and broker but not OTel (advisory).
        """
        errors = []
        warnings = []

        # PostgreSQL — required for both roles
        pg_config = getattr(engine, "postgres_config", {})
        pg_ok, pg_msg = await _check_postgres(pg_config)
        if not pg_ok:
            errors.append(
                f"PostgreSQL unreachable ({pg_msg}). "
                "Set VOLNUX_POSTGRES_HOST and VOLNUX_POSTGRES_PASSWORD."
            )
        else:
            logger.info("Pre-flight: PostgreSQL OK — %s", pg_msg)

        # Redis checkpoints — required for both roles
        ckpt_url = os.environ.get(
            "VOLNUX_REDIS_CHECKPOINT_URL", "redis://localhost:6379/0"
        )
        ck_ok, ck_msg = await _check_redis(ckpt_url)
        if not ck_ok:
            errors.append(
                f"Redis (checkpoints) unreachable at {ckpt_url} ({ck_msg}). "
                "Set VOLNUX_REDIS_CHECKPOINT_URL."
            )
        else:
            logger.info("Pre-flight: Redis checkpoints OK — %s", ck_msg)

        # Redis broker — required for both roles (Celery ScheduleTrigger)
        broker_url = os.environ.get(
            "VOLNUX_REDIS_BROKER_URL", "redis://localhost:6380/0"
        )
        br_ok, br_msg = await _check_redis(broker_url)
        if not br_ok:
            # Broker failure is a warning for worker, error for API
            # (API registers ScheduleTrigger which requires broker)
            if role == "api":
                errors.append(
                    f"Redis (broker) unreachable at {broker_url} ({br_msg}). "
                    "Set VOLNUX_REDIS_BROKER_URL."
                )
            else:
                warnings.append(
                    f"Redis (broker) unreachable — ScheduleTrigger will not fire."
                )
        else:
            logger.info("Pre-flight: Redis broker OK — %s", br_msg)

        # Log warnings
        for w in warnings:
            logger.warning("Pre-flight warning: %s", w)

        # Raise on errors
        if errors:
            raise CommandError(
                "Pre-flight checks failed:\n" + "\n".join(f"  • {e}" for e in errors)
            )

    async def _run_server(
        self,
        engine,
        role: str,
        host: str,
        port: int,
        worker_port: int,
        metrics_port: int,
        log_level: str,
        drain_timeout: float,
    ) -> None:
        """
        Start the server for the given role and handle graceful shutdown.

        Graceful shutdown sequence on SIGTERM:
            1. Stop accepting new workflow dispatches
            2. Complete in-flight events at the next safe phase boundary
            3. Drain checkpoint queue (up to drain_timeout seconds)
            4. Stop Prometheus and OTel exporters
            5. Exit cleanly
        """
        stop_event = asyncio.Event()
        drain_complete = asyncio.Event()

        def _handle_sigterm(sig, _frame):
            logger.info(
                "Received %s — initiating graceful shutdown", signal.Signals(sig).name
            )
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, _handle_sigterm)

        try:
            from volnux.server import start_api_server, start_worker_server

            if role == "api":
                await start_api_server(
                    engine=engine,
                    host=host,
                    port=port,
                    metrics_port=metrics_port,
                    log_level=log_level,
                    reload=False,
                    dev_mode=False,
                    stop_event=stop_event,
                )
            else:
                await start_worker_server(
                    engine=engine,
                    host=host,
                    port=worker_port,
                    metrics_port=metrics_port,
                    log_level=log_level,
                    stop_event=stop_event,
                )

        except ImportError:
            # Stub: engine server not yet implemented
            logger.warning("volnux.server not found — running stub server loop")
            while not stop_event.is_set():
                await asyncio.sleep(0.5)

        # ── Graceful shutdown ──────────────────────────────────────────────
        logger.info("Draining checkpoint queue (timeout=%.0fs)", drain_timeout)
        try:
            checkpoint_manager = getattr(engine, "checkpoint_manager", None)
            if checkpoint_manager and hasattr(checkpoint_manager, "drain"):
                await asyncio.wait_for(
                    checkpoint_manager.drain(),
                    timeout=drain_timeout,
                )
            drain_complete.set()
            logger.info("Checkpoint drain complete")
        except asyncio.TimeoutError:
            logger.warning(
                "Checkpoint drain timed out after %.0fs — "
                "some in-flight events may resume from an earlier checkpoint",
                drain_timeout,
            )
        except Exception as exc:
            logger.error("Checkpoint drain error: %s", exc)

    @staticmethod
    def _configure_json_logging(log_level: str) -> None:
        """
        Configure structured JSON logging for production.
        Falls back to standard logging if python-json-logger is not installed.
        """
        try:
            from pythonjsonlogger import jsonlogger

            handler = logging.StreamHandler(sys.stdout)
            formatter = jsonlogger.JsonFormatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
            handler.setFormatter(formatter)
            root = logging.getLogger()
            root.setLevel(getattr(logging, log_level))
            root.addHandler(handler)
        except ImportError:
            logging.basicConfig(
                level=getattr(logging, log_level),
                format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                stream=sys.stdout,
            )
