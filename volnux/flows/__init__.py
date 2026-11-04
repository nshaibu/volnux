import typing

from .base import BaseFlow
from .parallel import ParallelFlow
from .single import SingleFlow
from .stream import StreamFlow

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


def setup_execution_flow(execution_context: "ExecutionContext") -> BaseFlow:
    if execution_context.is_stream_segment():
        return StreamFlow(execution_context)
    if execution_context.is_multitask():
        return ParallelFlow(execution_context)
    else:
        return SingleFlow(execution_context)


__all__ = ["setup_execution_flow"]
