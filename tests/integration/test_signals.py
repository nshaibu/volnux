"""The signals a run fires, in the order the stack fires them."""

import pytest

from tests.integration.support import Trace, make_event, run, step
from volnux.signal import signals


@pytest.fixture
def fired():
    log = []
    names = [
        "pipeline_execution_start",
        "event_execution_start",
        "event_execution_end",
        "pipeline_execution_end",
    ]
    keep = []
    for name in names:

        def make(name):
            def listener(sender=None, signal=None, **kwargs):
                event = kwargs.get("event")
                log.append((name, type(event).__name__ if event is not None else None))

            return listener

        fn = make(name)
        keep.append(fn)  # signals hold listeners weakly
        getattr(signals, name).connect(None, fn)
    yield log
    for name, fn in zip(names, keep):
        getattr(signals, name).disconnect(None, fn)


@pytest.mark.asyncio
async def test_a_run_fires_pipeline_and_event_signals_in_order(fired):
    trace = Trace()
    a = make_event("SigA", step(trace, "a"))
    b = make_event("SigB", step(trace, "b"))

    _, error = await run(f"{a.__name__} -> {b.__name__}")
    mine = [(n, e) for n, e in fired if e in (None, a.__name__, b.__name__)]

    assert error is None
    assert mine == [
        ("pipeline_execution_start", None),
        ("event_execution_start", a.__name__),
        ("event_execution_end", a.__name__),
        ("event_execution_start", b.__name__),
        ("event_execution_end", b.__name__),
        ("pipeline_execution_end", None),
    ]
