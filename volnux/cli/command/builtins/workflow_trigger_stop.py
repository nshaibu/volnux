import asyncio
import os
import time
import sys
from pathlib import Path
from typing import Optional

from ..base import BaseCommand, CommandCategory, CommandError


class StopTriggersCommand(BaseCommand):
    """Stop all running workflow triggers"""

    help = "Stop all running workflow triggers"
    name = "stop_triggers"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def add_arguments(self, parser) -> None:
        """Add command arguments"""
        parser.add_argument(
            "--pid-file",
            type=str,
            default=".volnux_triggers.pid",
            help="Path to PID file for daemon mode (default: .volnux_triggers.pid)",
        )

    def handle(self, *args, **options) -> Optional[str]:
        pid_file = options.get("pid_file", ".volnux_triggers.pid")

        project_dir, _ = self.get_project_root_and_config_module()

        pid_path = project_dir / pid_file

        if pid_path.exists():
            return self._stop_daemon(pid_path)
        else:
            return self._stop_via_engine()

    def _stop_daemon(self, pid_path: Path) -> Optional[str]:
        """Stop daemon process using PID file"""
        try:
            with open(pid_path, "r") as f:
                pid = int(f.read().strip())

            self.stdout.write("\n")
            self.stdout.write(self.style.BOLD("Stopping Trigger Engine Daemon:"))
            self.stdout.write("\n")
            self.stdout.write(f"  Found daemon process (PID: {pid})\n")

            # Try to terminate gracefully
            try:
                os.kill(pid, 15)  # SIGTERM
                self.stdout.write("  Sending termination signal...\n")

                # Wait for process to stop (with timeout)
                timeout = 10
                for _ in range(timeout * 10):
                    try:
                        os.kill(pid, 0)  # Check if still running
                        time.sleep(0.1)
                    except OSError:
                        # Process no longer exists
                        break
                else:
                    # Timeout reached, force kill
                    self.warning("  Process did not stop gracefully, forcing...")
                    os.kill(pid, 9)  # SIGKILL

                pid_path.unlink()

                self.success("✓ Trigger engine daemon stopped successfully")
                self.stdout.write("\n")

            except OSError as e:
                if e.errno == 3:  # No such process
                    self.warning(f"Process {pid} not found (already stopped)")
                    pid_path.unlink()
                else:
                    raise CommandError(f"Failed to stop process {pid}: {e}")

        except (ValueError, IOError) as e:
            raise CommandError(f"Invalid PID file: {e}")

        return None

    def _stop_via_engine(self) -> Optional[str]:
        """Stop engine via API (non-daemon mode)"""
        self.warning("No daemon process found.")
        self.stdout.write("If the trigger engine is running in foreground mode, ")
        self.stdout.write("press Ctrl+C in that terminal to stop it.\n\n")
        return None
