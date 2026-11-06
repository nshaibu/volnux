"""The trigger CLI commands read and write TriggerStateRecord through the async API."""

import asyncio

import pytest

from volnux.cli.command.builtins.triggers.subcommands.trigger.inspect import (
    InspectTriggerSubCommand,
)
from volnux.cli.command.builtins.triggers.subcommands.trigger.list import (
    ListTriggerSubCommand,
)
from volnux.cli.command.builtins.triggers.subcommands.trigger.pause import (
    PauseTriggerSubCommand,
)
from volnux.cli.command.builtins.triggers.subcommands.trigger.resume import (
    ResumeTriggerSubCommand,
)
from volnux.cli.command.builtins.triggers.subcommands.trigger.stop import (
    StopTriggerSubCommand,
)
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import TriggerLifecycle


def seeded(lifecycle=TriggerLifecycle.ACTIVE):
    record = TriggerStateRecord(
        workflow_name="wf", lifecycle=lifecycle, enabled=True, last_fired=None
    )
    asyncio.run(record.save_async())
    return record


def load(record):
    return asyncio.run(TriggerStateRecord.get_async(record.id))


@pytest.mark.parametrize(
    "command, lifecycle",
    [
        (PauseTriggerSubCommand, TriggerLifecycle.PAUSED),
        (StopTriggerSubCommand, TriggerLifecycle.STOPPED),
        (ResumeTriggerSubCommand, TriggerLifecycle.ACTIVE),
    ],
)
def test_command_requests_the_change_and_marks_dirty(store, command, lifecycle):
    record = seeded(TriggerLifecycle.PAUSED)
    command().handle(trigger_id=record.id)
    reloaded = load(record)
    assert reloaded.lifecycle is lifecycle and reloaded.dirty is True


@pytest.mark.parametrize(
    "command",
    [
        PauseTriggerSubCommand,
        StopTriggerSubCommand,
        ResumeTriggerSubCommand,
        InspectTriggerSubCommand,
    ],
)
def test_unknown_trigger_is_reported_not_raised(store, command, capsys):
    command().handle(trigger_id="missing")
    captured = capsys.readouterr()
    assert "not found" in captured.out + captured.err


def test_list_shows_record_ids(store, capsys):
    record = seeded()
    ListTriggerSubCommand().handle()
    assert record.id in capsys.readouterr().out


def test_list_with_nothing_stored(store, capsys):
    ListTriggerSubCommand().handle()
    assert "No trigger states found" in capsys.readouterr().out


def test_inspect_shows_the_record(store, capsys):
    record = seeded()
    InspectTriggerSubCommand().handle(trigger_id=record.id)
    out = capsys.readouterr().out
    assert record.id in out and "wf" in out
