# management/commands/status_triggers.py

from __future__ import annotations

__all__ = ["StatusTriggerEngineSubCommand"]

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from volnux.cli.command.base import SubCommand, CommandCategory, CommandError

_COL_NAME = 22
_COL_STATE = 10
_COL_LAST_FIRED = 21
_COL_ERRORS = 7
_COL_LAST_ERROR = 42


class StatusTriggerEngineSubCommand(SubCommand):
    """Show the status of the trigger engine and all registered triggers."""

    name = "status_triggers"
    category = CommandCategory.WORKFLOW_MANAGEMENT
    help = "Show the status of the trigger engine and all registered triggers"

    # ------------------------------------------------------------------
    # Arguments
    # ------------------------------------------------------------------

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--pid-file",
            dest="pid_file",
            type=str,
            default=".volnux_triggers.pid",
            help="Path to the PID file written by the trigger engine daemon.",
        )
        parser.add_argument(
            "--format",
            dest="format",
            choices=["table", "json"],
            default="table",
            help="Output format: 'table' (default) or 'json'.",
        )
        parser.add_argument(
            "--verbose",
            "-v",
            dest="verbose",
            action="store_true",
            default=False,
            help="Include extra columns: last_fired_at, error_count, last_error.",
        )

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def handle(self, *args, **options) -> None:
        try:
            self._run(options)
        except CommandError:
            raise
        except Exception as exc:
            raise CommandError(
                f"Unexpected error while running status_triggers: {exc}"
            ) from exc

    def _run(self, options: dict) -> None:
        pid_file: str = options["pid_file"]
        fmt: str = options["format"]
        verbose: bool = options["verbose"]

        # ── Step 1: Daemon process status ─────────────────────────────────────
        project_dir, _ = self.get_project_root_and_config_module()
        pid_path: Path = project_dir / pid_file

        daemon_status: str
        pid: Optional[int]
        uptime: Optional[str]

        if not pid_path.exists():
            daemon_status, pid, uptime = "stopped", None, None
        else:
            try:
                pid = int(pid_path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                daemon_status, pid, uptime = "unknown (corrupt PID file)", None, None
            else:
                try:
                    os.kill(pid, 0)  # null signal — liveness probe only
                except OSError:
                    daemon_status, uptime = "stopped (stale PID file)", None
                else:
                    daemon_status = "running"
                    uptime = self._get_process_start_time(pid)

        # ── Step 2: Trigger state from database ───────────────────────────────
        triggers: list = []
        db_error: Optional[str] = None

        try:
            from ..state import TriggerStateRecord  # noqa: PLC0415

            triggers = TriggerStateRecord.all()
        except ImportError as exc:
            db_error = f"Cannot import TriggerStateRecord: {exc}"
        except Exception as exc:  # noqa: BLE001
            db_error = f"Error querying trigger state: {exc}"

        # ── Step 3: Render ────────────────────────────────────────────────────
        if fmt == "json":
            self._render_json(daemon_status, pid, uptime, triggers, db_error, verbose)
        else:
            self._render_table(daemon_status, pid, uptime, triggers, db_error, verbose)

    # ------------------------------------------------------------------
    # Renderers
    # ------------------------------------------------------------------

    def _render_json(
        self,
        daemon_status: str,
        pid: Optional[int],
        uptime: Optional[str],
        triggers: list,
        db_error: Optional[str],
        verbose: bool,
    ) -> None:
        trigger_list = [
            {
                "trigger_id": r.trigger_id,
                "trigger_name": r.trigger_name,
                "state": r.state,
                "last_fired_at": r.last_fired_at,
                "error_count": r.error_count,
                "last_error": r.last_error,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            }
            for r in triggers
        ]

        payload: dict = {
            "daemon": {"status": daemon_status, "pid": pid, "uptime": uptime},
            "triggers": trigger_list,
            "summary": _build_summary(triggers),
        }
        if db_error:
            payload["db_error"] = db_error

        # default=str serialises datetime objects as ISO 8601 strings
        self.stdout.write(json.dumps(payload, indent=2, default=str))
        self.stdout.flush()

    def _render_table(
        self,
        daemon_status: str,
        pid: Optional[int],
        uptime: Optional[str],
        triggers: list,
        db_error: Optional[str],
        verbose: bool,
    ) -> None:
        out = self.stdout.write

        # ── Header ────────────────────────────────────────────────────────────
        out(self.style.BOLD("Trigger Engine Status"))
        out(self.style.BOLD("====================="))
        out("")

        # ── Daemon line ───────────────────────────────────────────────────────
        daemon_label = _colorise_daemon_status(daemon_status, self.style)
        pid_info = ""
        if pid is not None:
            pid_info = f"  (PID: {pid}"
            if uptime:
                pid_info += f", up {uptime}"
            pid_info += ")"
        out(f"  Daemon:   {daemon_label}{pid_info}")

        # ── Database line ─────────────────────────────────────────────────────
        if db_error:
            out(f"  Database: {self.style.ERROR('✗ error')}  ({db_error})")
        else:
            noun = "trigger" if len(triggers) == 1 else "triggers"
            out(
                f"  Database: {self.style.SUCCESS('✓ connected')}"
                f"  ({len(triggers)} {noun} registered)"
            )

        out("")

        # ── Triggers table ────────────────────────────────────────────────────
        out(self.style.BOLD("Triggers"))
        out(self.style.BOLD("--------"))

        if not triggers:
            out("  (no triggers registered)")
        else:
            header = (
                f"  {'NAME':<{_COL_NAME}}"
                f"{'STATE':<{_COL_STATE}}"
                f"{'LAST FIRED':<{_COL_LAST_FIRED}}"
                f"{'ERRORS':<{_COL_ERRORS}}"
            )
            if verbose:
                header += f"  {'LAST ERROR'}"
            out(header)

            sep_width = _COL_NAME + _COL_STATE + _COL_LAST_FIRED + _COL_ERRORS
            if verbose:
                sep_width += 2 + _COL_LAST_ERROR
            out("  " + "─" * sep_width)

            for rec in triggers:
                row = self._format_table_row(rec, verbose)

                # Pad state column by printable width (ANSI codes are invisible)
                state_pad = " " * max(0, _COL_STATE - len(_strip_ansi(row[1])))
                line = (
                    f"  {row[0]:<{_COL_NAME}}"
                    f"{row[1]}{state_pad}"
                    f"{row[2]:<{_COL_LAST_FIRED}}"
                    f"{row[3]:<{_COL_ERRORS}}"
                )
                if verbose and len(row) > 4:
                    line += f"  {row[4]}"
                out(line)

        out("")

        # ── Summary line ──────────────────────────────────────────────────────
        s = _build_summary(triggers)
        error_str = self.style.ERROR(str(s["error"])) if s["error"] else str(s["error"])
        out(
            f"Summary: {s['total']} total  |  "
            f"{self.style.SUCCESS(str(s['active']))} active  |  "
            f"{self.style.WARNING(str(s['paused']))} paused  |  "
            f"{s['stopped']} stopped  |  "
            f"{error_str} error"
        )

    # ------------------------------------------------------------------
    # Row formatter
    # ------------------------------------------------------------------

    def _format_table_row(self, record, verbose: bool) -> tuple[str, ...]:
        """Return display strings for one table row."""
        name = record.trigger_name or record.trigger_id
        state_str = record.state or "unknown"
        last_fired = self._format_relative_time(record.last_fired_at)
        errors = str(record.error_count)
        coloured = _colorise_trigger_state(state_str, self.style)

        if verbose:
            raw = record.last_error or ""
            trunc = raw[:40] + ("…" if len(raw) > 40 else "")
            return (name, coloured, last_fired, errors, trunc)

        return (name, coloured, last_fired, errors)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_process_start_time(self, pid: int) -> Optional[str]:
        """
        Return how long ago the process started (e.g. "2h 03m ago"), or None.

        Tries /proc/{pid}/stat on Linux first, then psutil, then gives up.
        Never raises.
        """
        elapsed: Optional[float] = None

        # Linux /proc approach
        try:
            stat_path = Path(f"/proc/{pid}/stat")
            uptime_path = Path("/proc/uptime")
            if stat_path.exists() and uptime_path.exists():
                fields = stat_path.read_text(encoding="utf-8").split()
                starttime_ticks = int(fields[21])  # field index 21
                clk_tck: int = os.sysconf("SC_CLK_TCK")  # usually 100 Hz
                uptime_secs = float(uptime_path.read_text(encoding="utf-8").split()[0])
                elapsed = uptime_secs - (starttime_ticks / clk_tck)
        except Exception:  # noqa: BLE001
            pass

        # psutil fallback (optional dependency)
        if elapsed is None:
            try:
                import psutil  # noqa: PLC0415

                create_time = psutil.Process(pid).create_time()
                elapsed = datetime.now(tz=timezone.utc).timestamp() - create_time
            except Exception:  # noqa: BLE001
                pass

        return (_format_elapsed(elapsed) + " ago") if elapsed is not None else None

    @staticmethod
    def _format_relative_time(dt: Optional[datetime]) -> str:
        """Return a human-readable relative time string, or 'never' for None."""
        if dt is None:
            return "never"
        now = datetime.now(tz=timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)  # assume UTC for naive datetimes
        delta = max(0.0, (now - dt).total_seconds())
        return _format_elapsed(delta) + " ago"


# ---------------------------------------------------------------------------
# Module-level pure helpers
# ---------------------------------------------------------------------------


def _build_summary(triggers: list) -> dict:
    """Return total and per-state counts."""
    s = {"total": len(triggers), "active": 0, "paused": 0, "stopped": 0, "error": 0}
    for rec in triggers:
        key = (rec.state or "").lower()
        if key in s:
            s[key] += 1
    return s


def _format_elapsed(seconds: float) -> str:
    """Convert raw seconds to a compact human-readable string."""
    total = int(math.floor(max(0.0, seconds)))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)

    if days:
        return f"{days}d"
    if hours:
        return f"{hours}h {mins:02d}m"
    if mins:
        return f"{mins}m {secs:02d}s"
    return f"{total}s"


def _colorise_trigger_state(state: str, style) -> str:
    lower = state.lower()
    if lower == "active":
        return style.SUCCESS(state)
    if lower == "paused":
        return style.WARNING(state)
    if lower == "error":
        return style.ERROR(state)
    return state  # "stopped" and unknowns — plain


def _colorise_daemon_status(status: str, style) -> str:
    lower = status.lower()
    if lower == "running":
        return style.SUCCESS("● running")
    if lower.startswith("unknown"):
        return style.WARNING("● unknown")
    return style.ERROR("● stopped")


def _strip_ansi(text: str) -> str:
    """Strip ANSI escape codes to measure printable column width."""
    import re

    return re.sub(r"\x1b\[[0-9;]*m", "", text)
