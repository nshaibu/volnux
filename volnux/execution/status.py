from enum import Enum


class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    #: Waiting on the outside world (a human, an external event, a condition):
    #: checkpointed and released, to be resumed by the rehydration manager.
    SUSPENDED = "suspended"
    ABORTED = "aborted"
    FAILED = "failed"
