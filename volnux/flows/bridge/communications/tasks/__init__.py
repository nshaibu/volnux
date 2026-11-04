"""
Module for managing task execution and communication across different types
of executor bridges.

This module provides the functionality to create communication bridges
required for task execution and coordination between various components.
Tasks in this process use the local bridge; tasks outside it (process pool,
Celery) use the messaging bridge over the configured Redis/Kafka backend.

Exports:
- TaskCommunicationBridge: Abstract base class for task communication bridges.
- LocalTaskCommunicationBridge: Concrete implementation for local task communication.
- MessagingTaskCommunicationBridge: Implementation over a messaging backend.
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
    ChannelUnavailable,
    TERMINAL_STATES,
    parse_task_state,
)

from .local import LocalTaskCommunicationBridge
from .messaging import MessagingTaskCommunicationBridge

if TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


def create_communication_bridge(
    context: "ExecutionContext",
    executor_type: str,
    **kwargs,
) -> TaskCommunicationBridge:
    """
    Create the bridge matching where a task runs.

    Args:
        context: Execution context
        executor_type: ``"local"``/``"thread"`` for tasks in this process,
            ``"process"``/``"celery"``/``"messaging"`` for tasks outside it.
        **kwargs: Additional arguments for specific bridge types

    Returns:
        The matching TaskCommunicationBridge instance
    """
    bridges = {
        "local": LocalTaskCommunicationBridge,
        "thread": LocalTaskCommunicationBridge,
        "process": MessagingTaskCommunicationBridge,
        "celery": MessagingTaskCommunicationBridge,
        "messaging": MessagingTaskCommunicationBridge,
    }

    bridge_class = bridges.get(executor_type.lower())
    if not bridge_class:
        raise ValueError(f"Unknown executor type: {executor_type}")

    return bridge_class(context, **kwargs)


__all__ = [
    "TaskCommunicationBridge",
    "LocalTaskCommunicationBridge",
    "MessagingTaskCommunicationBridge",
    "ChannelUnavailable",
    "TaskExecutionHandle",
    "create_communication_bridge",
    "TaskCommand",
    "CommandType",
    "MessageType",
    "TaskMessage",
    "TaskState",
    "TaskStatus",
    "TERMINAL_STATES",
    "parse_task_state",
]
