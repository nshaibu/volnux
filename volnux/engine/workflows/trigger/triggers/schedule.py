import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from celery import Celery
from celery.schedules import crontab

from .base import TriggerBase, TriggerType

logger = logging.getLogger(__name__)


class SchedulerTrigger(TriggerBase):
    """
    Trigger that activates on a Celery cron schedule.

    Activation mechanism
    --------------------
    On ``start()``, registers a named Celery task and injects a corresponding
    entry into ``celery_app.conf.beat_schedule``. Celery Beat fires the task
    according to the supplied ``crontab``; the task bridges back into async
    by running ``activate()`` on a fresh event loop inside the worker process.

    On ``stop()``, the beat entry is removed and the task is deregistered from
    the Celery task registry so no further executions are scheduled or dispatched.

    Crontab fields
    --------------
    Accepts either a pre-built ``celery.schedules.crontab`` instance via
    ``schedule`` or individual cron field strings (``minute``, ``hour``, etc.).
    Providing both raises ``ValueError``.

    Examples
    --------
    Using individual fields::

        trigger = SchedulerTrigger(
            workflow_name="generate_report",
            celery_app=app,
            hour="9",
            minute="30",
            day_of_week="mon-fri",
        )

    Using a pre-built crontab::

        trigger = SchedulerTrigger(
            workflow_name="generate_report",
            celery_app=app,
            schedule=crontab(hour="9", minute="30", day_of_week="mon-fri"),
        )
    """

    trigger_type = TriggerType.SCHEDULE

    def __init__(
        self,
        workflow_name: str,
        celery_app: Celery,
        *,
        schedule: Optional[crontab] = None,
        minute: str = "*",
        hour: str = "*",
        day_of_week: str = "*",
        day_of_month: str = "*",
        month_of_year: str = "*",
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        _using_schedule = schedule is not None
        _using_fields = any(
            v != "*" for v in (minute, hour, day_of_week, day_of_month, month_of_year)
        )

        if _using_schedule and _using_fields:
            raise ValueError(
                "Provide either a 'schedule' crontab instance or individual cron "
                "field arguments (minute, hour, …), not both."
            )

        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )

        self.celery_app = celery_app
        self._crontab: crontab = schedule or crontab(
            minute=minute,
            hour=hour,
            day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
        )

        # Stable, unique names derived from trigger_id (set after state.save()).
        # Both the Celery task name and the beat-schedule key use the same value
        # so they are trivially correlated in logs and monitoring.
        self._task_name: Optional[str] = None

    def _make_task_name(self) -> str:
        """Return the Celery task name for this trigger instance."""
        return f"volnux.schedule_trigger.{self.trigger_id}"

    def _register_celery_task(self) -> None:
        """
        Dynamically create and register a Celery task that fires ``activate()``.

        The task is bound to ``self`` via closure so each ``ScheduleTrigger``
        instance gets its own isolated task. A fresh event loop is used per
        invocation because Celery worker processes do not share the engine's
        async event loop.
        """
        trigger = self  # explicit closure capture; avoids accidental rebinding
        task_name = self._task_name

        @self.celery_app.task(name=task_name)
        def _fire_trigger() -> None:
            logger.debug("Celery task '%s' executing schedule trigger", task_name)
            fired_at = datetime.now(timezone.utc).isoformat()
            try:
                asyncio.run(
                    trigger.activate(
                        scheduled_at=fired_at,
                        crontab=str(trigger._crontab),
                    )
                )
            except Exception as exc:
                # Error is already recorded inside activate(); re-raise so
                # Celery can apply its own retry / failure machinery.
                logger.error(
                    "ScheduleTrigger '%s' raised during Celery task execution: %s",
                    trigger.trigger_id,
                    exc,
                )
                raise

    def _deregister_celery_task(self) -> None:
        """Remove the task from Celery's registry if it is present."""
        if self._task_name and self._task_name in self.celery_app.tasks:
            del self.celery_app.tasks[self._task_name]
            logger.debug(
                "ScheduleTrigger '%s': deregistered Celery task '%s'",
                self.trigger_id,
                self._task_name,
            )

    async def start(self) -> None:
        """
        Register the Celery task and inject the beat-schedule entry.

        Calling ``start()`` on an already-active trigger is idempotent: a
        warning is logged and the method returns without modifying existing
        state, preventing duplicate beat entries.
        """
        task_name = self._make_task_name()

        if task_name in self.celery_app.conf.beat_schedule:
            logger.warning(
                "ScheduleTrigger '%s': beat entry '%s' already exists; "
                "skipping start to avoid duplicate registration.",
                self.trigger_id,
                task_name,
            )
            return

        self._task_name = task_name
        self._register_celery_task()

        self.celery_app.conf.beat_schedule[self._task_name] = {
            "task": self._task_name,
            "schedule": self._crontab,
            # Pass nothing via Celery args; all context lives in the closure.
            "args": (),
            "kwargs": {},
            "options": {
                # Prevent stale tasks from piling up if a worker is slow.
                "expires": self._crontab_period_seconds(),
            },
        }

        logger.info(
            "ScheduleTrigger '%s' started — task: '%s', crontab: '%s'",
            self.trigger_id,
            self._task_name,
            self._crontab,
        )

    async def stop(self) -> None:
        """
        Remove the beat-schedule entry and deregister the Celery task.

        Safe to call on a trigger that was never started or was already stopped.
        """
        if self._task_name is None:
            logger.debug(
                "ScheduleTrigger '%s': stop() called before start(); nothing to do.",
                self.trigger_id,
            )
            return

        removed = self.celery_app.conf.beat_schedule.pop(self._task_name, None)
        if removed:
            logger.info(
                "ScheduleTrigger '%s': removed beat entry '%s'.",
                self.trigger_id,
                self._task_name,
            )
        else:
            logger.warning(
                "ScheduleTrigger '%s': beat entry '%s' was not found during stop().",
                self.trigger_id,
                self._task_name,
            )

        self._deregister_celery_task()
        self._task_name = None

    def _crontab_period_seconds(self) -> int:
        """
        Return a conservative upper-bound expiry for the task in seconds.

        Used to set ``expires`` on dispatched tasks so stale executions from
        a paused worker do not fire in a burst when the worker recovers.
        A full minute (60 s) is used as the floor to cover per-minute crontabs.
        """
        # crontab does not expose its period directly, so we derive an upper
        # bound from the human-readable string that Celery itself builds.
        period_map = {
            "day_of_week": 7 * 24 * 3600,
            "day_of_month": 31 * 24 * 3600,
            "hour": 3600,
        }
        for field, seconds in period_map.items():
            if getattr(self._crontab, field, "*") not in ("*", None):
                return seconds
        return 60  # per-minute floor
