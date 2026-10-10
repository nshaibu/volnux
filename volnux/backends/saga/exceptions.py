import logging
from typing import List, Tuple


class SagaError(Exception):
    """Base exception for all Saga failures."""


class SagaStepFailureError(SagaError):
    """A saga step's action failed after all retry attempts."""

    def __init__(self, step_index: int, step_name: str, original_error: Exception):
        self.step_index = step_index
        self.step_name = step_name
        self.original_error = original_error
        super().__init__(
            "Saga step %d ('%s') failed: %s" % (step_index, step_name, original_error)
        )


class SagaCompensationFailureError(SagaError):
    """One or more compensation steps failed after a saga step failure.

    Indicates the system may be in a partially inconsistent state.
    Check ``compensation_failures`` for the specific steps that could
    not be rolled back — these likely require manual intervention.
    """

    def __init__(
        self,
        original_error: Exception,
        compensation_failures: List[Tuple[int, str, Exception]],
    ):
        self.original_error = original_error
        # List of (step_index, step_name, exception) for each failed compensation.
        self.compensation_failures = compensation_failures
        failed_indices = [i for i, _, _ in compensation_failures]
        super().__init__(
            "Saga step failed and compensation also failed for step(s) %s. "
            "Manual intervention may be required. "
            "Original error: %s" % (failed_indices, original_error)
        )


class SagaTimeoutError(SagaError):
    """A saga step exceeded its configured timeout."""

    def __init__(self, step_index: int, step_name: str, timeout: float):
        self.step_index = step_index
        self.step_name = step_name
        self.timeout = timeout
        super().__init__(
            "Saga step %d ('%s') timed out after %.1fs"
            % (step_index, step_name, timeout)
        )
