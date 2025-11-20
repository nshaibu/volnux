from volnux.decorators import listener
from volnux.signal.signals import *
from volnux.execution.context import ExecutionContext


@listener(
    [
        event_execution_init,
        event_execution_end,
        event_execution_retry_done,
        event_execution_retry,
        event_execution_start,
    ],
    sender=ExecutionContext,
)
def event_execution_start(*args, **kwargs):
    print(f"Event signals: {args}, {kwargs}")


@listener(event_execution_cancelled, sender=ExecutionContext)
def event_execution_cancelled(*args, **kwargs):
    print(f"Event cancelled signal: {args}, {kwargs}")
