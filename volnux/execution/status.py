from enum import Enum


class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    ABORTED = "aborted"
    FAILED = "failed"
