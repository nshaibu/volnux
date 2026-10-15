"""Tests for the signal -> governance-event reporter.

Most wired signals are registered from a table and share two generic
translators, so the mapping itself (which signal produces which event type, and
with what fixed payload) is the thing worth asserting; it is driven through the
handlers the reporter actually connects, rather than by calling translators
directly, so the table and the closures are covered too.

The reporter's wiring (connect/disconnect, exception isolation) is tested
against fake signals, so no live engine is needed.
"""

import pytest

from volnux.governance.events import EventType, GovernanceEvent
from volnux.governance.reporter import (
    SignalGovernanceReporter,
    translate_execution_failed,
    translate_hitl_requested,
    translate_task_retried,
)
from volnux.governance.sampling import EventSampler

from .conftest import (
    ExplodingPublisher,
    FakeContext,
    FakePipeline,
    FakeSignal,
    FakeSuspensionRequest,
    FakeTask,
    RecordingPublisher,
)


def handlers_for(reporter):
    """Map each wired signal's attribute name to the handler it is given."""
    from volnux.signal import signals as sig

    by_id = {id(getattr(sig, name)): name for name in dir(sig)}
    return {
        by_id.get(id(signal), "?"): handler
        for signal, handler in reporter._build_registrations()
    }


@pytest.fixture
def wired(publisher):
    """A reporter plus its handlers, keyed by signal name."""
    reporter = SignalGovernanceReporter(publisher)
    return reporter, publisher, handlers_for(reporter)


# --- The signal -> event-type table -----------------------------------------


def test_every_wired_signal_is_registered_once(wired):
    _reporter, _publisher, handlers = wired

    assert set(handlers) == {
        "pipeline_execution_start",
        "pipeline_execution_end",
        "pipeline_stop",
        "pipeline_shutdown",
        "event_execution_failed",
        "event_execution_paused",
        "event_execution_resumed",
        "event_execution_start",
        "event_execution_end",
        "event_execution_retry",
        "hitl_requested",
    }


@pytest.mark.parametrize(
    "signal_name, expected_type, expected_payload",
    [
        ("pipeline_execution_end", EventType.EXECUTION_COMPLETED, {}),
        ("pipeline_stop", EventType.EXECUTION_STOPPED, {"reason": "cancelled"}),
        # A shutdown ended the run ABORTED; the platform records that as a
        # failed execution but keeps the reason so the two remain tellable apart.
        ("pipeline_shutdown", EventType.EXECUTION_FAILED, {"reason": "aborted"}),
        ("event_execution_paused", EventType.EXECUTION_PAUSED, {}),
        ("event_execution_resumed", EventType.EXECUTION_RESUMED, {}),
    ],
)
def test_execution_signals_map_to_their_event_type(
    wired, context, signal_name, expected_type, expected_payload
):
    _reporter, publisher, handlers = wired

    handlers[signal_name](execution_context=context)

    event = publisher.sent[0]
    assert isinstance(event, GovernanceEvent)
    assert event.event_type == expected_type
    assert event.payload == expected_payload
    assert event.execution_id == "run-1"
    assert event.workflow_id == "wf-1"
    assert event.workflow_name == "Customer ETL"
    assert event.task_id is None


def test_execution_start_reads_the_pipeline_it_is_handed_directly(wired, pipeline):
    # pipeline_execution_start sends the run as `pipeline`; every other signal
    # sends an ExecutionContext. Both must correlate to the same id.
    _reporter, publisher, handlers = wired

    handlers["pipeline_execution_start"](pipeline=pipeline)

    event = publisher.sent[0]
    assert event.event_type == EventType.EXECUTION_STARTED
    assert event.execution_id == "run-1"


def test_execution_id_is_the_pipeline_id_through_the_context(wired, context):
    _reporter, publisher, handlers = wired
    context.pipeline = FakePipeline(pid="run-99")

    handlers["pipeline_execution_end"](execution_context=context)

    assert publisher.sent[0].execution_id == "run-99"


def test_a_context_that_lost_its_pipeline_reports_unknown_not_its_own_id(wired):
    # ``ExecutionContext.get_state()`` excludes the pipeline — it is transient,
    # non-serializable — and ``set_state`` never restores it, so a rehydrated
    # context (HITL resume, cold reload) has none. The run id must then be
    # ``None``: reporting the *context's* id instead would put the event in a
    # different id space and silently mis-correlate it.
    _reporter, publisher, handlers = wired
    context = FakeContext()
    context.pipeline = None
    context.id = "context-42"

    handlers["pipeline_execution_end"](execution_context=context)

    event = publisher.sent[0]
    assert event.execution_id is None
    assert event.workflow_id == "wf-1"  # the workflow fields are unaffected


@pytest.mark.parametrize(
    "signal_name, expected_type",
    [
        ("event_execution_start", EventType.TASK_STARTED),
        ("event_execution_end", EventType.TASK_COMPLETED),
    ],
)
def test_task_signals_carry_the_task_name(
    wired, context, task, signal_name, expected_type
):
    _reporter, publisher, handlers = wired

    handlers[signal_name](execution_context=context, event=task)

    event = publisher.sent[0]
    assert event.event_type == expected_type
    assert event.task_id == "ExtractCustomerData"
    assert event.execution_id == "run-1"


def test_task_id_falls_back_to_the_event_id_when_unnamed(wired, context):
    _reporter, publisher, handlers = wired

    handlers["event_execution_start"](
        execution_context=context, event=FakeTask(name=None, id="task-42")
    )

    assert publisher.sent[0].task_id == "task-42"


def test_fixed_payloads_are_not_shared_between_events(wired, context):
    # The table's payload literals are reused on every fire; mutating one
    # event's payload must not leak into the next.
    _reporter, publisher, handlers = wired

    handlers["pipeline_stop"](execution_context=context)
    publisher.sent[0].payload["reason"] = "tampered"
    handlers["pipeline_stop"](execution_context=context)

    assert publisher.sent[1].payload == {"reason": "cancelled"}


# --- The translators that carry real logic ----------------------------------


def test_execution_failed_records_the_state(context):
    # event_execution_failed fires mid-run, before the unconditional
    # pipeline_execution_end (which maps to COMPLETED); the projector's terminal
    # freeze is what keeps the run FAILED, so this must report FAILED.
    event = translate_execution_failed(execution_context=context, state="ERRORED")

    assert event.event_type == EventType.EXECUTION_FAILED
    assert event.payload == {"state": "ERRORED"}


def test_retry_prefers_the_explicit_task_id_and_records_attempt_data(context):
    event = translate_task_retried(
        event=FakeTask(name="fallback"),
        execution_context=context,
        task_id="Enrich",
        retry_count=2,
        max_attempts=3,
        backoff=1.5,
    )

    assert event.event_type == EventType.TASK_RETRIED
    assert event.task_id == "Enrich"
    assert event.payload == {"retry_count": 2, "max_attempts": 3, "backoff": 1.5}


def test_retry_falls_back_to_the_event_name(context):
    event = translate_task_retried(
        event=FakeTask(name="Enrich"), execution_context=context, retry_count=1
    )

    assert event.task_id == "Enrich"


def test_hitl_carries_the_prompt_and_the_engine_request_id(context):
    # request_id is the key the engine resumes on, so it must survive intact.
    event = translate_hitl_requested(
        execution_context=context, request=FakeSuspensionRequest()
    )

    assert event.event_type == EventType.HITL_REQUESTED
    assert event.task_id == "ApproveTransfer"
    assert event.payload == {
        "request_id": "req-77",
        "title": "Approve $47,500 transfer?",
        "description": "Needs a human",
        "options": ["approve", "reject"],
        "timeout_hours": 24,
    }


def test_hitl_tolerates_a_request_missing_optional_attributes(context):
    event = translate_hitl_requested(execution_context=context, request=object())

    assert event.payload["request_id"] is None
    assert event.payload["options"] == []


# --- Degradation and isolation ----------------------------------------------


def test_missing_attributes_degrade_to_none_rather_than_raising(wired):
    # A renamed or absent engine attribute must not blow up a signal handler,
    # and must not leave a stringified placeholder behind.
    _reporter, publisher, handlers = wired

    handlers["pipeline_execution_start"](pipeline=object())

    event = publisher.sent[0]
    assert event.execution_id is None
    assert event.workflow_id is None
    assert event.workflow_name is None


def test_a_raising_translator_never_reaches_the_engine(wired, context):
    _reporter, publisher, handlers = wired

    class ExplodingRequest:
        task_id = "x"

        @property
        def options(self):
            raise RuntimeError("attribute blew up")

    # Must not propagate: the signal handler runs on the execution path.
    handlers["hitl_requested"](execution_context=context, request=ExplodingRequest())

    assert publisher.sent == []


def test_a_raising_publisher_never_reaches_the_engine(context):
    reporter = SignalGovernanceReporter(ExplodingPublisher())
    handlers = handlers_for(reporter)

    handlers["pipeline_execution_end"](execution_context=context)


def test_every_event_carries_its_provenance(wired, context):
    # Where the fact came from: the emitting mesh node and the owning project.
    from volnux.config import VolnuxConfig

    _reporter, publisher, handlers = wired
    config = VolnuxConfig.get_instance()

    handlers["pipeline_execution_end"](execution_context=context)

    event = publisher.sent[0]
    assert event.node_id == config.get_node_id()
    assert event.project_id == config.get("PROJECT_ID")


def test_each_event_gets_its_own_identity_and_timestamp(wired, context):
    _reporter, publisher, handlers = wired

    for _ in range(5):
        handlers["pipeline_execution_end"](execution_context=context)

    assert len({event.event_id for event in publisher.sent}) == 5
    assert all(event.occurred_at > 0 for event in publisher.sent)


# --- Install / uninstall ----------------------------------------------------


@pytest.fixture
def fake_wiring(publisher, monkeypatch):
    """A reporter whose signals are fakes, so install/uninstall can be driven."""
    reporter = SignalGovernanceReporter(publisher)
    # Resolve the real handlers before patching, or the replacement would
    # re-enter itself.
    by_name = handlers_for(reporter)
    signals = {
        "start": FakeSignal("pipeline_execution_start"),
        "end": FakeSignal("pipeline_execution_end"),
    }
    monkeypatch.setattr(
        reporter,
        "_build_registrations",
        lambda: [
            (signals["start"], by_name["pipeline_execution_start"]),
            (signals["end"], by_name["pipeline_execution_end"]),
        ],
    )
    return reporter, publisher, signals


def test_install_connects_and_is_idempotent(fake_wiring):
    reporter, _publisher, signals = fake_wiring

    reporter.install()
    reporter.install()  # a second call must not double-connect

    assert reporter.installed is True
    assert len(signals["start"].listeners) == 1
    assert len(signals["end"].listeners) == 1


def test_uninstall_disconnects_and_is_idempotent(fake_wiring):
    reporter, _publisher, signals = fake_wiring

    reporter.install()
    reporter.uninstall()
    reporter.uninstall()

    assert reporter.installed is False
    assert signals["start"].listeners == []
    assert signals["end"].listeners == []


def test_an_installed_reporter_is_held_by_a_strong_reference(fake_wiring):
    # SoftSignal holds listeners weakly; without this registry an installed
    # reporter could be collected and silently stop reporting.
    from volnux.governance import reporter as reporter_module

    reporter, _publisher, _signals = fake_wiring

    reporter.install()
    assert reporter in reporter_module._installed_reporters

    reporter.uninstall()
    assert reporter not in reporter_module._installed_reporters


def test_a_connected_listener_publishes_a_translated_event(fake_wiring, pipeline):
    reporter, publisher, signals = fake_wiring
    reporter.install()

    # Drive the listener the way SoftSignal.emit would.
    listener = signals["start"].listeners[0]
    listener(signal=signals["start"], sender=object(), pipeline=pipeline)

    assert len(publisher.sent) == 1
    assert publisher.sent[0].event_type == EventType.EXECUTION_STARTED
    assert publisher.sent[0].execution_id == "run-1"

    reporter.uninstall()


# --- Sampling integration ---------------------------------------------------


def test_telemetry_is_held_back_until_a_terminal_event(context, task):
    publisher = RecordingPublisher()
    reporter = SignalGovernanceReporter(publisher, sampler=EventSampler(capacity=4))
    handlers = handlers_for(reporter)

    for _ in range(20):
        handlers["event_execution_start"](execution_context=context, event=task)

    assert publisher.sent == []  # task.started is telemetry, so it is sampled

    handlers["pipeline_execution_end"](execution_context=context)

    # The run's reservoir is flushed first, then the terminal event itself.
    assert len(publisher.sent) == 5
    assert publisher.sent[-1].event_type == EventType.EXECUTION_COMPLETED
    assert all(isinstance(event, GovernanceEvent) for event in publisher.sent)
