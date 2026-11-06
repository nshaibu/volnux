"""Turning a stored ``TriggerConfig`` into a live trigger of the TriggerEngine."""

import logging
from typing import Any, Dict, Optional

from volnux.config import setting
from volnux.models import TriggerConfig, Workflow
from volnux.models.enums import TriggerType

logger = logging.getLogger(__name__)

HOOK_PATH = "/api/v1/hooks"
MIN_WEBHOOK_SECRET = 16


class TriggerConfigError(ValueError):
    """The configuration cannot be turned into a trigger (reported as a 422)."""


def _celery_app() -> Any:
    path = setting("CELERY_APP", None)
    if not path:
        raise TriggerConfigError(
            "Schedule triggers need the CELERY_APP setting (the dotted path of a "
            "Celery application)"
        )
    from volnux.import_utils import import_string

    try:
        return import_string(path)
    except Exception as exc:
        raise TriggerConfigError(f"CELERY_APP {path!r} cannot be imported: {exc}")


def _cron_fields(cron: Any) -> Dict[str, str]:
    parts = str(cron).split()
    if len(parts) != 5:
        raise TriggerConfigError(
            "'cron' must have five fields: minute hour day-of-month month day-of-week"
        )
    minute, hour, day_of_month, month_of_year, day_of_week = parts
    return {
        "minute": minute,
        "hour": hour,
        "day_of_month": day_of_month,
        "month_of_year": month_of_year,
        "day_of_week": day_of_week,
    }


def build_trigger(
    trigger_type: TriggerType,
    workflow_name: str,
    config: Dict[str, Any],
    enabled: bool = True,
):
    """The engine trigger for a configuration.

    :raises TriggerConfigError: unsupported type, or a missing / invalid setting.
    """
    from volnux.engine.workflows.trigger.triggers import (
        ManualTrigger,
        SchedulerTrigger,
        WebhookTrigger,
    )

    params = config.get("workflow_params") or {}
    try:
        if trigger_type == TriggerType.MANUAL:
            return ManualTrigger(
                workflow_name,
                require_confirmation=bool(config.get("require_confirmation", False)),
                workflow_params=params,
                enabled=enabled,
            )
        if trigger_type == TriggerType.WEBHOOK:
            secret = config.get("secret_token")
            if not isinstance(secret, str) or len(secret) < MIN_WEBHOOK_SECRET:
                raise TriggerConfigError(
                    f"A webhook needs a 'secret_token' of at least "
                    f"{MIN_WEBHOOK_SECRET} characters"
                )
            return WebhookTrigger(
                workflow_name,
                endpoint_path=f"{HOOK_PATH}/pending",  # (set to the id's path below)
                secret_token=secret,
                token_header=config.get("token_header", "x-webhook-token"),
                timestamp_header=config.get("timestamp_header"),
                workflow_params=params,
                enabled=enabled,
            )
        if trigger_type == TriggerType.SCHEDULE:
            if "cron" not in config:
                raise TriggerConfigError("A schedule needs a 'cron' expression")
            return SchedulerTrigger(
                workflow_name,
                _celery_app(),
                workflow_params=params,
                enabled=enabled,
                **_cron_fields(config["cron"]),
            )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, TriggerConfigError):
            raise
        raise TriggerConfigError(str(exc)) from exc
    raise TriggerConfigError(
        f"Triggers of type {trigger_type.value!r} cannot be created through the API yet"
    )


async def register_trigger(
    trigger_type: TriggerType,
    workflow_name: str,
    config: Dict[str, Any],
    enabled: bool = True,
):
    """Build a trigger, register it with the engine and start it when the engine runs.

    :raises TriggerConfigError: the configuration is not valid.
    :raises WorkflowNotFound: the workflow is not loaded in the engine.
    """
    from volnux.engine.workflows.trigger import get_trigger_engine
    from volnux.engine.workflows.trigger.triggers import WebhookTrigger

    engine = get_trigger_engine()
    trigger = build_trigger(trigger_type, workflow_name, config, enabled)
    if isinstance(trigger, WebhookTrigger):
        trigger.endpoint_path = f"{HOOK_PATH}/{trigger.trigger_id}"
    engine.register(trigger)
    try:
        await engine.start_trigger(trigger.trigger_id)
    except Exception:
        await engine.unregister(trigger.trigger_id)
        raise
    return trigger


async def unregister_trigger(engine_trigger_id: Optional[str]) -> None:
    """Remove a live trigger; one the engine does not know is not an error."""
    if not engine_trigger_id:
        return
    from volnux.engine.workflows.trigger import get_trigger_engine

    try:
        await get_trigger_engine().unregister(engine_trigger_id)
    except ValueError:
        pass


async def restore_triggers() -> int:
    """Register every active ``TriggerConfig`` with the engine (at API startup).

    One that cannot be registered is logged and skipped. Returns how many were.
    """
    restored = 0
    for config in await TriggerConfig.filter_async(is_active=True):
        try:
            workflow = await Workflow.get_async(config.workflow.id)
            trigger = await register_trigger(
                config.trigger_type, workflow.name, config.config
            )
            config.engine_trigger_id = trigger.trigger_id
            await config.save_async()
            restored += 1
        except Exception as exc:
            logger.warning("Trigger %s was not restored: %s", config.id, exc)
    return restored
