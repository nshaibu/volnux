import asyncio
import logging
import threading
from functools import partial
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from celery import Celery
from celery.schedules import crontab

from ..event import TriggerEvent
from .base import TriggerBase, TriggerType
from .event import EventSubscriptionMixin

logger = logging.getLogger(__name__)

#: Name of the one Celery task every schedule fires. It is the same for all
#: triggers, so a worker that never saw a trigger can still run it.
FIRE_TASK_NAME = "volnux.schedule_trigger.fire"

#: ``TriggerEvent.event_type`` published by the task when a schedule is due.
SCHEDULE_FIRED_EVENT = "schedule.fired"


def _run_sync(coro) -> None:
    """Run ``coro`` to completion from synchronous (Celery task) code.

    ``asyncio.run`` refuses to start inside a thread that already runs an event
    loop (an eager task called from async code, a gevent/eventlet worker), so in
    that case the coroutine runs on a short-lived thread of its own.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
        return

    outcome: Dict[str, BaseException] = {}

    def runner() -> None:
        try:
            asyncio.run(coro)
        except BaseException as exc:  # handed back to the calling thread
            outcome["error"] = exc

    thread = threading.Thread(target=runner, name="schedule-trigger-fire")
    thread.start()
    thread.join()
    if "error" in outcome:
        raise outcome["error"]


def fire_schedule(trigger_id: str, crontab_repr: str = "") -> None:
    """What a worker does when beat dispatches a schedule: say that it is due.

    The worker holds no trigger and no engine. It publishes a ``TriggerEvent``
    over the messaging backend; the ``SchedulerTrigger`` with this id, running
    wherever the engine is, receives it and activates its workflow.
    """
    event = TriggerEvent(
        event_type=SCHEDULE_FIRED_EVENT,
        source=FIRE_TASK_NAME,
        data={
            "trigger_id": trigger_id,
            "scheduled_at": datetime.now(timezone.utc).isoformat(),
            "crontab": crontab_repr,
        },
    )
    _run_sync(TriggerEvent.publish(event))


def register_schedule_task(celery_app: Celery) -> None:
    """Register the shared fire task on ``celery_app`` (idempotent).

    Call this wherever the Celery app is configured, so the worker process
    knows the task; constructing a ``SchedulerTrigger`` against the app also
    does it, which covers the engine side.
    """
    if FIRE_TASK_NAME not in celery_app.tasks:
        celery_app.task(name=FIRE_TASK_NAME, shared=False)(fire_schedule)


class SchedulerTrigger(EventSubscriptionMixin, TriggerBase):
    """
    Trigger that activates on a Celery cron schedule.

    Activation mechanism
    --------------------
    ``start()`` adds an entry to ``celery_app.conf.beat_schedule`` that runs one
    shared task, ``volnux.schedule_trigger.fire``, with this trigger's id.
    When Celery Beat dispatches it, the worker publishes a ``schedule.fired``
    ``TriggerEvent`` over the messaging backend; the trigger, listening in the
    engine process, receives it and calls ``activate()``.

    Beat, the worker and the engine therefore need not share a process: the
    worker only has to know the shared task (``register_schedule_task(app)``,
    done for you when a ``SchedulerTrigger`` is built against the app) and reach
    the same messaging backend. Delivery is pub/sub: a run that comes due while
    the engine is not listening is missed, not queued.

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
        timezone: Optional[str] = None,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if celery_app is None:
            raise ValueError("SchedulerTrigger requires a celery_app.")
        if schedule is not None and not isinstance(schedule, crontab):
            raise TypeError(
                f"'schedule' must be a celery.schedules.crontab, got "
                f"{type(schedule).__name__}."
            )

        if timezone is not None and schedule is not None:
            raise ValueError(
                "'timezone' applies to the cron field arguments; build the "
                "'schedule' crontab with its own 'nowfun' instead."
            )
        tzinfo = None
        if timezone is not None:
            try:
                tzinfo = ZoneInfo(timezone)
            except Exception as exc:  # ZoneInfoNotFoundError is a KeyError
                raise ValueError(f"Unknown time zone {timezone!r}.") from exc

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
            # evaluated in that zone (default: the Celery app's timezone)
            **({"nowfun": partial(datetime.now, tzinfo)} if tzinfo else {}),
        )
        self.timezone = timezone
        register_schedule_task(celery_app)

        # Key of this trigger's beat entry (set by start(), cleared by stop()).
        self._beat_key: Optional[str] = None

    def _make_beat_key(self) -> str:
        """The beat-schedule key of this trigger instance."""
        return f"volnux.schedule_trigger.{self.trigger_id}"

    async def start(self) -> None:
        """
        Add the beat entry and start listening for the schedule's events.

        Calling ``start()`` on an already-active trigger is idempotent: no
        second beat entry is created.
        """
        key = self._make_beat_key()
        register_schedule_task(self.celery_app)

        if key in self.celery_app.conf.beat_schedule:
            logger.warning(
                "SchedulerTrigger '%s': beat entry '%s' already exists; "
                "not adding a duplicate.",
                self.trigger_id,
                key,
            )
        else:
            self.celery_app.conf.beat_schedule[key] = {
                "task": FIRE_TASK_NAME,
                "schedule": self._crontab,
                "args": (self.trigger_id, str(self._crontab)),
                "kwargs": {},
                "options": {
                    # Prevent stale tasks from piling up if a worker is slow.
                    "expires": self._crontab_period_seconds(),
                },
            }
        self._beat_key = key

        # Listen before the first run can come due.
        await self._open_subscription()

        logger.info(
            "SchedulerTrigger '%s' started — beat entry '%s', crontab: '%s'",
            self.trigger_id,
            key,
            self._crontab,
        )

    async def stop(self) -> None:
        """
        Remove the beat entry and stop listening.

        Safe to call on a trigger that was never started or was already stopped.
        """
        await self._close_subscription()

        if self._beat_key is None:
            logger.debug(
                "SchedulerTrigger '%s': stop() called before start(); nothing to do.",
                self.trigger_id,
            )
            return

        removed = self.celery_app.conf.beat_schedule.pop(self._beat_key, None)
        if removed:
            logger.info(
                "SchedulerTrigger '%s': removed beat entry '%s'.",
                self.trigger_id,
                self._beat_key,
            )
        else:
            logger.warning(
                "SchedulerTrigger '%s': beat entry '%s' was not found during stop().",
                self.trigger_id,
                self._beat_key,
            )
        self._beat_key = None

    async def _handle_event(self, event: TriggerEvent) -> None:
        """Activate when the worker reports that *this* trigger's schedule is due."""
        if event.event_type != SCHEDULE_FIRED_EVENT:
            return
        data = event.data or {}
        if data.get("trigger_id") != self.trigger_id:
            return

        await self.activate(
            scheduled_at=data.get("scheduled_at"),
            crontab=data.get("crontab"),
            correlation_id=self.generate_correlation_id(),
        )

    #: How many upcoming gaps between runs are looked at to find the longest.
    _EXPIRY_LOOKAHEAD = 12

    def _crontab_period_seconds(self) -> int:
        """
        Return an upper bound, in seconds, on the time between two runs.

        Used as ``expires`` on dispatched tasks so executions that sat in a queue
        past the next scheduled run (a paused worker) are dropped instead of
        firing in a burst on recovery. The gaps between the next several runs are
        measured, because they are not always equal (``mon-fri`` leaves a
        three-day gap over the weekend); the longest wins, with a 60 s floor.
        """
        longest = 60.0
        try:
            moment = self._crontab.now()
            # remaining_delta(last_run) is the step from ``last_run`` to the next
            # run, whatever the current time is (remaining_estimate is not).
            previous = moment
            for _ in range(self._EXPIRY_LOOKAHEAD + 1):
                last_run, delta, _now = self._crontab.remaining_delta(previous)
                following = last_run + delta
                if following <= previous:
                    break
                if previous is not moment:
                    longest = max(longest, (following - previous).total_seconds())
                previous = following
        except Exception:  # an exotic schedule must not stop the trigger starting
            logger.debug("Could not measure the crontab period", exc_info=True)
        return int(longest)
