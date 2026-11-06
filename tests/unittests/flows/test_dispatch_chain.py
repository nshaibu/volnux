"""End-to-end tests of the dispatch chain.

Pipeline.start -> Engine -> ExecutionContext -> ExecutionCoordinator -> Flow
-> executor -> EventBase.__call__ (lifecycle phases) -> EventResult.

Nothing is mocked: a real Pipeline compiled from an inline Pointy workflow is
run, so these tests fail when any link in the chain is broken.
"""

import asyncio
import uuid

import pytest

from volnux import EventBase, Pipeline
from volnux.execution.pipeline.fields import InputDataField


def unique(name: str) -> str:
    return f"{name}{uuid.uuid4().hex[:8]}"


def make_event(name, process, executor="default", **attrs):
    return type(
        unique(name), (EventBase,), {"process": process, "executor": executor, **attrs}
    )


def make_pipeline(pointy: str, **fields):
    namespace = {
        "Meta": type("Meta", (), {"pointy": pointy}),
        **{k: InputDataField(data_type=str, required=False) for k in fields},
    }
    return type(unique("Pipe"), (Pipeline,), namespace)


async def run(pipeline_class, **inputs):
    pipeline = pipeline_class(**inputs)
    await pipeline.start()
    return pipeline


def contents(pipeline):
    """Result contents of the latest context, in order."""
    context = pipeline.execution_context.get_latest_context()
    return [r.content for r in context.get_results()]


EXECUTORS = ["default", "thread"]


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_a_sync_event_runs_through_the_whole_chain(executor):
    async def process(self, *args, **kwargs):
        return True, "ran"

    event = make_event("Echo", process, executor=executor)
    pipeline = await run(make_pipeline(event.__name__))

    assert contents(pipeline) == ["ran"]


# --- helpers for richer assertions --------------------------------------


def all_results(pipeline):
    """Results of every context in the chain, in execution order."""
    out = []
    for context in pipeline.execution_context:
        out.extend(context.get_results())
    return out


def by_event(pipeline, event_class):
    return [r for r in all_results(pipeline) if r.event_name == event_class.__name__]


class Calls:
    """Counts hook invocations across the (possibly threaded) run."""

    def __init__(self):
        self.names = []

    def __call__(self, name):
        self.names.append(name)

    def count(self, name):
        return self.names.count(name)


# --- the chain runs both kinds of process() and chains events -------------


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_a_plain_def_process_also_runs(executor):
    def process(self, *args, **kwargs):
        return True, "sync-ran"

    event = make_event("SyncEcho", process, executor=executor)
    pipeline = await run(make_pipeline(event.__name__))

    assert contents(pipeline) == ["sync-ran"]


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_chained_events_all_run_in_order(executor):
    async def first(self, *a, **k):
        return True, "one"

    async def second(self, *a, **k):
        return True, "two"

    a = make_event("First", first, executor=executor)
    b = make_event("Second", second, executor=executor)
    pipeline = await run(make_pipeline(f"{a.__name__}->{b.__name__}"))

    assert [r.content for r in all_results(pipeline)] == ["one", "two"]


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_a_failing_event_yields_an_error_result(executor):
    async def process(self, *a, **k):
        return False, "nope"

    event = make_event("Fails", process, executor=executor)
    pipeline = await run(make_pipeline(event.__name__))

    (result,) = by_event(pipeline, event)
    assert result.error is True and result.content == "nope"


@pytest.mark.asyncio
async def test_an_exception_in_process_becomes_an_error_result():
    async def process(self, *a, **k):
        raise ValueError("kaboom")

    event = make_event("Raises", process)
    pipeline = await run(make_pipeline(event.__name__))

    (result,) = all_results(pipeline)
    assert result.error is True and "kaboom" in str(result.content)


# --- T0-3: result building happens once ----------------------------------


@pytest.mark.asyncio
async def test_on_success_hook_signal_fires_exactly_once_per_event():
    from volnux.signal.signals import event_called

    calls = []

    def listener(**kwargs):
        calls.append(kwargs["hook_type"])

    async def process(self, *a, **k):
        return True, "ok"

    event = make_event("Once", process)
    event_called.connect(event, listener)
    try:
        await run(make_pipeline(event.__name__))
    finally:
        event_called.disconnect(event, listener)

    assert calls == ["on_success"]


@pytest.mark.asyncio
async def test_on_failure_hook_signal_fires_exactly_once_per_event():
    from volnux.signal.signals import event_called

    calls = []

    def listener(**kwargs):
        calls.append(kwargs["hook_type"])

    async def process(self, *a, **k):
        return False, "bad"

    event = make_event("OnceFail", process)
    event_called.connect(event, listener)
    try:
        await run(make_pipeline(event.__name__))
    finally:
        event_called.disconnect(event, listener)

    assert calls == ["on_failure"]


# --- T0-4: a bypassed event is a success, not a failure -------------------


@pytest.mark.asyncio
async def test_a_bypassed_event_returns_its_bypass_data_as_a_success():
    calls = Calls()

    async def bypass(self):
        return True, "bypassed"

    async def process(self, *a, **k):
        calls("process")
        return True, "should-not-run"

    event = make_event("Bypassed", process, bypass=bypass)
    pipeline = await run(make_pipeline(event.__name__))

    (result,) = by_event(pipeline, event)
    assert result.error is False
    assert result.content == "bypassed"
    assert calls.count("process") == 0


# --- Tier 1: lifecycle ordering, communicate(), HITL guards ----------------


@pytest.mark.asyncio
@pytest.mark.parametrize("executor", EXECUTORS)
async def test_every_lifecycle_hook_runs_in_order(executor):
    order = []

    async def bypass(self):
        order.append("bypass")
        raise NotImplementedError  # default: no bypass

    async def communicate(self, *a, **k):
        order.append("communicate")

    async def process(self, *a, **k):
        order.append("process")
        return True, "ok"

    async def cleanup(self, *a, **k):
        order.append("cleanup")

    event = make_event(
        "Lifecycle",
        process,
        executor=executor,
        bypass=bypass,
        communicate=communicate,
        cleanup=cleanup,
    )
    await run(make_pipeline(event.__name__))

    # the bypass check precedes any external communication, then the work
    assert order == ["bypass", "communicate", "process", "cleanup"]


@pytest.mark.asyncio
async def test_phases_only_ever_advance():
    from volnux.signal.signals import event_phase_changed

    seen = []

    def listener(**kwargs):
        seen.append(int(kwargs["phase"]))

    async def process(self, *a, **k):
        return True, "ok"

    event = make_event("Phases", process)
    event_phase_changed.connect(event, listener)
    try:
        await run(make_pipeline(event.__name__))
    finally:
        event_phase_changed.disconnect(event, listener)

    assert seen == sorted(seen), f"phase went backwards: {seen}"
    assert len(set(seen)) == len(seen), f"a phase was reported twice: {seen}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method, kwargs",
    [
        ("request_human_input", {"title": "t", "description": "d", "payload": {}}),
        ("wait_for_event", {"event_type": "x.y", "title": "t", "description": "d"}),
        (
            "wait_for_condition",
            {"condition_fn": lambda: True, "title": "t", "description": "d"},
        ),
    ],
)
async def test_external_communication_can_be_requested_from_communicate(method, kwargs):
    """These suspend the event; they used to raise RuntimeError("...can only be
    called from communicate() hook") because the phase had not advanced yet."""
    from volnux.exceptions import ExternalCommunicationSuspensionRequest

    async def communicate(self, *a, **k):
        await getattr(self, method)(**kwargs)

    async def process(self, *a, **k):
        return True, "unreachable"

    event = make_event("Suspends", process, communicate=communicate)

    pipeline = make_pipeline(event.__name__)()
    raised = None
    try:
        await pipeline.start()
    except Exception as exc:  # noqa: BLE001 - inspected below
        raised = exc

    errors = (
        list(pipeline.execution_context.get_latest_context().errors)
        if pipeline.execution_context
        else []
    )
    if raised is not None:
        errors.append(raised)
    suspended = [
        e for e in errors if isinstance(e, ExternalCommunicationSuspensionRequest)
    ]
    runtime = [e for e in errors if isinstance(e, RuntimeError)]

    assert not runtime, f"guard rejected a legitimate call: {runtime}"
    assert suspended, f"expected a suspension request, got {errors!r}"


# --- Tier 1: goto() / control flow must not be retried ----------------------


def _branching_pipeline(retry_policy):
    calls = Calls()

    async def decide(self, *a, **k):
        calls("decide")
        self.goto(1, True, "chose-branch-1")

    async def branch_zero(self, *a, **k):
        calls("branch-0")
        return True, "zero"

    async def branch_one(self, *a, **k):
        calls("branch-1")
        return True, "one"

    attrs = {"retry_policy": retry_policy} if retry_policy else {}
    first = make_event("Decide", decide, **attrs)
    zero = make_event("BranchZero", branch_zero)
    one = make_event("BranchOne", branch_one)
    pipeline_class = make_pipeline(
        f"{first.__name__}(0->{zero.__name__},1->{one.__name__})"
    )
    return pipeline_class, calls


@pytest.mark.asyncio
async def test_goto_switches_branch_without_a_retry_policy():
    pipeline_class, calls = _branching_pipeline(None)
    await run(pipeline_class)
    assert calls.names == ["decide", "branch-1"]


@pytest.mark.asyncio
async def test_goto_is_not_retried_when_the_event_has_a_retry_policy():
    pipeline_class, calls = _branching_pipeline({"max_attempts": 3})
    await run(pipeline_class)

    # one attempt, then the requested branch (it used to run 3 times and
    # then report a failure, never switching)
    assert calls.names == ["decide", "branch-1"]


@pytest.mark.asyncio
async def test_real_errors_are_still_retried_up_to_max_attempts():
    calls = Calls()

    async def flaky(self, *a, **k):
        calls("try")
        raise ConnectionError("down")

    event = make_event(
        "Flaky", flaky, retry_policy={"max_attempts": 3, "backoff_factor": 0}
    )
    pipeline = await run(make_pipeline(event.__name__))

    assert calls.count("try") == 3
    (result,) = all_results(pipeline)
    assert result.error is True


@pytest.mark.asyncio
async def test_a_retried_event_that_eventually_succeeds_reports_success():
    calls = Calls()

    async def eventually(self, *a, **k):
        calls("try")
        if calls.count("try") < 3:
            raise ConnectionError("not yet")
        return True, "finally"

    event = make_event(
        "Eventually", eventually, retry_policy={"max_attempts": 5, "backoff_factor": 0}
    )
    pipeline = await run(make_pipeline(event.__name__))

    assert calls.count("try") == 3
    (result,) = all_results(pipeline)
    assert result.error is False and result.content == "finally"


# --- the process pool: the event is pickled into a worker process -----------


@pytest.mark.asyncio
@pytest.mark.parametrize("event_name", ["ProcessEcho", "ProcessSyncEcho"])
async def test_events_run_in_a_separate_process_on_the_process_pool(event_name):
    import os

    from tests.unittests.flows import (
        process_events,
    )  # noqa: F401 - registers the events

    pipeline = await run(make_pipeline(event_name))

    (result,) = all_results(pipeline)
    assert result.error is False
    assert result.content.endswith(str(os.getpid())) is False, "ran in the parent"
    assert "pid-" in result.content


@pytest.mark.asyncio
async def test_a_parallel_group_runs_every_event_then_the_next_step():
    """ParallelFlow used to return a list of futures, which crashed
    ResultProcessor.process_futures() with 'unhashable type: list'."""
    from tests.unittests.flows import (
        process_events,
    )  # noqa: F401 - registers the events

    pipeline = await run(make_pipeline("ParA||ParB||ParC->AfterParallel"))

    contexts = list(pipeline.execution_context)
    first = sorted(r.content for r in contexts[0].get_results())
    assert first == ["a", "b", "c"]
    assert [r.content for r in contexts[-1].get_results()] == ["after"]
    assert not any(c.errors for c in contexts)


# --- retry details ----------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_option_in_pointy_applies_to_an_event_with_a_class_policy():
    """`Event * 3` combined with a class-level retry_policy used to crash:
    _configure_event called config_retry_policy(policy=...), which has no such
    parameter."""
    calls = Calls()

    async def always_fails(self, *a, **k):
        calls("try")
        raise ConnectionError("down")

    event = make_event(
        "PolicyPlusOption",
        always_fails,
        retry_policy={"max_attempts": 2, "backoff_factor": 0},
    )
    pipeline = await run(make_pipeline(f"{event.__name__} * 4"))

    assert calls.count("try") == 4  # the pointy option wins over the class policy
    (result,) = all_results(pipeline)
    assert result.error is True


@pytest.mark.asyncio
async def test_pointy_retry_option_works_without_a_class_policy():
    calls = Calls()

    async def always_fails(self, *a, **k):
        calls("try")
        raise ConnectionError("down")

    event = make_event("OptionOnly", always_fails)
    await run(make_pipeline(f"{event.__name__} * 3"))

    assert calls.count("try") == 3


@pytest.mark.asyncio
async def test_subclasses_of_a_configured_exception_are_retried():
    """retry_on_exceptions=[OSError] must also retry FileNotFoundError."""
    calls = Calls()

    async def flaky(self, *a, **k):
        calls("try")
        raise FileNotFoundError("missing")

    event = make_event(
        "SubclassRetry",
        flaky,
        retry_policy={
            "max_attempts": 3,
            "backoff_factor": 0,
            "retry_on_exceptions": [OSError],
        },
    )
    await run(make_pipeline(event.__name__))

    assert calls.count("try") == 3


@pytest.mark.asyncio
async def test_unrelated_exceptions_are_not_retried_when_a_filter_is_set():
    calls = Calls()

    async def wrong_kind(self, *a, **k):
        calls("try")
        raise KeyError("nope")

    event = make_event(
        "FilteredRetry",
        wrong_kind,
        retry_policy={
            "max_attempts": 5,
            "backoff_factor": 0,
            "retry_on_exceptions": [OSError],
        },
    )
    await run(make_pipeline(event.__name__))

    assert calls.count("try") == 1


@pytest.mark.asyncio
async def test_the_final_failure_reports_the_last_error_not_the_first():
    calls = Calls()

    async def changing(self, *a, **k):
        calls("try")
        raise ValueError(f"attempt {calls.count('try')}")

    event = make_event(
        "LastError", changing, retry_policy={"max_attempts": 3, "backoff_factor": 0}
    )
    pipeline = await run(make_pipeline(event.__name__))

    (result,) = all_results(pipeline)
    assert "attempt 3" in str(result.content)


# --- failures keep the identity of the event that caused them ---------------


@pytest.mark.asyncio
async def test_an_error_result_names_the_event_and_task_that_raised():
    async def boom(self, *a, **k):
        raise ValueError("kaboom")

    event = make_event("Identified", boom)
    pipeline = await run(make_pipeline(event.__name__))

    (result,) = all_results(pipeline)
    assert result.event_name == event.__name__
    assert result.task_id


@pytest.mark.asyncio
async def test_a_shared_executor_instance_survives_runs():
    """SingleFlow.run() used to shut down whatever executor it was given right
    after submitting, so an instance registered for sharing (a Celery executor,
    a thread pool) died after its first run."""
    from concurrent.futures import ThreadPoolExecutor

    pool = ThreadPoolExecutor(max_workers=2)

    async def process(self, *a, **k):
        return True, "shared"

    try:
        for _ in range(2):
            event = make_event("SharedPool", process, executor=pool)
            pipeline = await run(make_pipeline(event.__name__))
            assert contents(pipeline) == ["shared"]

        assert pool.submit(lambda: 42).result(timeout=5) == 42  # still usable
    finally:
        pool.shutdown()


@pytest.mark.asyncio
async def test_run_does_not_block_until_the_event_finishes():
    """The flow hands back a future; waiting for the result is the coordinator's
    job (run() used to sit in executor.shutdown(wait=True) until the task ended)."""
    from collections import deque
    from unittest.mock import MagicMock

    from volnux.flows import SingleFlow

    async def process(self, *a, **k):
        await asyncio.sleep(1.5)
        return True, "late"

    event_class = make_event("Gated", process, executor="thread")
    profile = MagicMock()
    profile.get_event_class.return_value = event_class
    profile.options = None
    profile.get_id.return_value = "task-1"
    context = MagicMock()
    context.task_profiles = deque([profile])
    flow = SingleFlow(context, enable_communication=False)
    flow.get_initialized_event = lambda tp: (
        event_class(context, "task-1"),
        {},
    )

    future = await asyncio.wait_for(flow.run(), 3)  # returns at once...

    assert not future.done()  # ...while the event is still running
    future.cancel()
    await flow.close()  # waits for the worker thread, then shuts the pool down


@pytest.mark.asyncio
async def test_a_dispatch_timeout_ends_a_task_that_runs_too_long(monkeypatch):
    """End to end: the coordinator's timeout used to cover only the submission,
    so a slow task ran to completion regardless."""
    import time

    from volnux.execution.context import ExecutionContext, ExecutionStatus

    original = ExecutionContext.dispatch

    async def dispatch_with_timeout(self, timeout=None):
        return await original(self, timeout=0.3)

    monkeypatch.setattr(ExecutionContext, "dispatch", dispatch_with_timeout)

    async def process(self, *a, **k):
        await asyncio.sleep(30)
        return True, "never"

    slow = make_event("TooSlow", process)
    pipeline_class = make_pipeline(f"{slow.__name__}")

    started = time.monotonic()
    pipeline = pipeline_class()
    try:
        await asyncio.wait_for(pipeline.start(), 10)
    except Exception:
        pass  # whether start() raises or reports is the engine's business
    assert time.monotonic() - started < 5

    context = pipeline.execution_context.get_latest_context()
    assert context.status == ExecutionStatus.FAILED
    assert all(r.content != "never" for r in context.get_results())
