import logging
import asyncio
from datetime import datetime
from typing import List, Optional

try:
    from croniter import croniter
except ImportError:
    raise RuntimeError("croniter not installed. Run: pip install croniter")

from .base import TriggerBase, TriggerLifecycle, TriggerType


logger = logging.getLogger(__name__)


class ScheduleTrigger(TriggerBase):
    """
    Trigger that activates on a schedule.

    Activation mechanism: asyncio timer or cron-like scheduler.
    No event bus required!
    """

    def __init__(
        self,
        workflow_name: str,
        schedule: str,  # cron expression or interval
        schedule_type: str = "interval",  # "interval" or "cron"
        **kwargs,
    ):
        super().__init__(workflow_name, **kwargs)
        self.schedule = schedule
        self.schedule_type = schedule_type
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the scheduler."""
        logger.info(f"ScheduleTrigger {self.trigger_id} starting: {self.schedule}")

        if self.schedule_type == "interval":
            self._task = asyncio.create_task(self._interval_loop())
        elif self.schedule_type == "cron":
            self._task = asyncio.create_task(self._cron_loop())
        else:
            raise ValueError(f"Unknown schedule type: {self.schedule_type}")

        self.lifecycle = TriggerLifecycle.ACTIVE

    async def stop(self):
        """Stop the scheduler."""
        logger.info(f"ScheduleTrigger {self.trigger_id} stopping")

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        self.lifecycle = TriggerLifecycle.STOPPED

    async def _interval_loop(self):
        """Simple interval-based scheduling."""
        interval_seconds = self._parse_interval(self.schedule)

        while True:
            try:
                await asyncio.sleep(interval_seconds)

                if self.enabled:
                    await self.activate(
                        scheduled_time=datetime.now().isoformat(),
                        schedule_type="interval",
                        interval=self.schedule,
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Schedule trigger error: {e}")
                self.error_count += 1

    async def _cron_loop(self):
        """Cron-style scheduling."""
        cron = croniter(self.schedule, datetime.now())

        while True:
            try:
                # Get next scheduled time
                next_run = cron.get_next(datetime)
                now = datetime.now()

                # Sleep until next run
                wait_seconds = (next_run - now).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                if self.enabled:
                    await self.activate(
                        scheduled_time=next_run.isoformat(),
                        schedule_type="cron",
                        cron_expression=self.schedule,
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cron trigger error: {e}")
                self.error_count += 1

    def _parse_interval(self, schedule: str) -> float:
        """Parse interval string like '5m', '1h', '30s'."""
        unit = schedule[-1]
        value = float(schedule[:-1])

        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}

        if unit not in multipliers:
            raise ValueError(f"Invalid interval unit: {unit}")

        return value * multipliers[unit]

    def get_activation_source(self) -> TriggerType:
        return TriggerType.SCHEDULE
