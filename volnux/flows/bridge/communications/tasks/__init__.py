"""
Module for managing task execution and communication across different types
of executor bridges.

This module provides the functionality to create communication bridges
required for task execution and coordination between various components.
The supported executor types include local, thread, process, and celery.

Exports:
- TaskCommunicationBridge: Abstract base class for task communication bridges.
- LocalTaskCommunicationBridge: Concrete implementation for local task communication.
- ProcessTaskCommunicationBridge: Implementation for process-based task communication.
- CeleryTaskCommunicationBridge: Implementation for celery-based task communication.
- TaskExecutionHandle: Represents a handle for task execution.
- create_communication_bridge: Factory function to create a communication bridge.

"""

from typing import TYPE_CHECKING

from .base import (
    TaskExecutionHandle,
    TaskCommunicationBridge,
    TaskCommand,
    TaskMessage,
    MessageType,
    TaskState,
    TaskStatus,
    CommandType,
)

from .local import LocalTaskCommunicationBridge
from .process import ProcessTaskCommunicationBridge
from .celery import CeleryTaskCommunicationBridge
from .remote import RemoteTaskCommunicationBridge

if TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


def create_communication_bridge(
    context: "ExecutionContext",
    executor_type: str,
    **kwargs,
) -> TaskCommunicationBridge:
    """
    Factory function to create the appropriate communication bridge.

    Args:
        context: Execution context
        executor_type: Type of executor ("local", "process", "remote", "celery")
        **kwargs: Additional arguments for specific bridge types

    Returns:
        Appropriate TaskCommunicationBridge instance
    """
    bridges = {
        "local": LocalTaskCommunicationBridge,
        "thread": LocalTaskCommunicationBridge,
        "process": ProcessTaskCommunicationBridge,
        "remote": RemoteTaskCommunicationBridge,
        "celery": CeleryTaskCommunicationBridge,
    }

    bridge_class = bridges.get(executor_type.lower())
    if not bridge_class:
        raise ValueError(f"Unknown executor type: {executor_type}")

    return bridge_class(context, **kwargs)


__all__ = [
    "TaskCommunicationBridge",
    "LocalTaskCommunicationBridge",
    "ProcessTaskCommunicationBridge",
    "TaskExecutionHandle",
    "create_communication_bridge",
    "TaskCommand",
    "CommandType",
    "MessageType",
    "TaskMessage",
]
