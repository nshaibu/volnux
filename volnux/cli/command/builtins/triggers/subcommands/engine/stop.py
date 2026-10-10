from __future__ import annotations

import os
import signal
import time
from pathlib import Path

from volnux.cli.command.base import SubCommand, CommandCategory, CommandError

__all__ = ["StopTriggerEngineSubCommand"]


class StopTriggerEngineSubCommand(SubCommand):
    """Stop the running trigger engine daemon."""

    name = "stop_triggers"
    category = CommandCategory.WORKFLOW_MANAGEMENT
    help = "Stop the running trigger engine"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--pid-file",
            type=str,
            default=".volnux_triggers.pid",
            help=(
                "Path to the PID file written by the trigger engine daemon "
                "(default: .volnux_triggers.pid)"
            ),
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=30,
            help=(
                "Seconds to wait for graceful shutdown before escalating to "
                "SIGKILL (default: 30)"
            ),
        )
        parser.add_argument(
            "--force",
            "-f",
            action="store_true",
            default=False,
            help="Skip SIGTERM and send SIGKILL immediately",
        )

    def handle(self, *args, **options) -> None:
        try:
            self._stop_daemon(options)
        except CommandError:
            raise
        except Exception as exc:
            raise CommandError(
                f"Unexpected error while stopping trigger engine: {exc}"
            ) from exc

    def _stop_daemon(self, options: dict) -> None:
        """Read PID file → signal process → poll for exit."""

        pid_file: str = options["pid_file"]
        timeout: int = options["timeout"]
        force: bool = options["force"]

        project_dir, _ = self.get_project_root_and_config_module()
        pid_path: Path = project_dir / pid_file

        self.stdout.write(self.style.BOLD("Stopping Trigger Engine:"))

        if not pid_path.exists():
            self.warning(
                "Trigger engine does not appear to be running (no PID file found)."
            )
            return

        pid: int = self._read_pid(pid_path)

        if not self._process_is_running(pid):
            self.warning(f"Process {pid} is not running. Removing stale PID file.")
            self._safe_unlink(pid_path)
            return

        if force:
            # --force: skip SIGTERM, kill immediately
            self._send_signal(pid, signal.SIGKILL, label="SIGKILL")
            self._wait_for_exit(pid, timeout=5, after_sigkill=True)
        else:
            # Normal path: SIGTERM first, escalate if needed
            self._send_signal(pid, signal.SIGTERM, label="SIGTERM")
            exited = self._poll_until_gone(pid, timeout=timeout)

            if not exited:
                self.warning(
                    f"Graceful shutdown timed out after {timeout}s. "
                    "Sending SIGKILL..."
                )
                self._send_signal(pid, signal.SIGKILL, label="SIGKILL")
                self._wait_for_exit(pid, timeout=5, after_sigkill=True)

        self._safe_unlink(pid_path)

        self.success(f"✓ Trigger engine stopped (PID: {pid})")

    def _read_pid(self, pid_path: Path) -> int:
        """Read and return the integer PID from pid_path.
        Removes the file and raises CommandError on any read/parse failure.
        """
        try:
            return int(pid_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError) as exc:
            self.warning(
                f"Could not read PID file '{pid_path}': {exc}. Removing and aborting."
            )
            self._safe_unlink(pid_path)
            raise CommandError(f"Invalid or unreadable PID file: {pid_path}") from exc

    def _process_is_running(self, pid: int) -> bool:
        """Return True if pid refers to a live process.
        Uses os.kill(pid, 0) — the null signal checks existence without
        delivering a signal.
        """
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _send_signal(self, pid: int, sig: signal.Signals, label: str) -> None:
        """Send sig to pid and print a progress line."""
        self.stdout.write(f"  Sending {label} to process {pid}...")
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            # Process vanished between liveness check and kill — already gone
            pass
        except OSError as exc:
            raise CommandError(
                f"Failed to send {label} to process {pid}: {exc}"
            ) from exc

    def _poll_until_gone(self, pid: int, timeout: int) -> bool:
        """Poll every 0.5 s until pid disappears or timeout elapses.
        Writes a progress dot to stdout on every tick (no newline, flushed
        immediately). Returns True if the process exited within the timeout.
        """
        elapsed: float = 0.0
        interval: float = 0.5

        while elapsed < timeout:
            time.sleep(interval)
            elapsed += interval

            self.stdout.write(".", ending="")
            self.stdout.flush()

            if not self._process_is_running(pid):
                self.stdout.write("")  # newline after dots
                return True

        self.stdout.write("")  # newline before timeout warning
        return False

    def _wait_for_exit(
        self, pid: int, timeout: int, *, after_sigkill: bool = False
    ) -> None:
        """Poll for up to timeout seconds after a SIGKILL.
        Raises CommandError if the process is still alive — SIGKILL should be
        unblockable, so something is seriously wrong.
        """
        elapsed: float = 0.0
        interval: float = 0.5

        while elapsed < timeout:
            time.sleep(interval)
            elapsed += interval

            self.stdout.write(".", ending="")
            self.stdout.flush()

            if not self._process_is_running(pid):
                self.stdout.write("")  # newline after dots
                return

        self.stdout.write("")  # newline after dots
        if after_sigkill:
            raise CommandError(
                f"Process {pid} did not exit even after SIGKILL. "
                "Manual intervention may be required."
            )

    def _safe_unlink(self, path: Path) -> None:
        """Remove path if it exists, silently ignoring missing-file errors."""
        try:
            path.unlink(missing_ok=True)
        except OSError:
            self.warning(f"Could not remove PID file '{path}'.")
