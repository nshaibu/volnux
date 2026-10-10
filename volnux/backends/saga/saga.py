import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from .exceptions import (
    SagaStepFailureError,
    SagaTimeoutError,
    SagaCompensationFailureError,
    SagaError,
)
from ._dlq import DeadLetterEntry
from .recovery import SagaRecoveryManager
from volnux.mixins import ObjectIdentityMixin
from volnux.concurrency.async_utils import as_coroutine

logger = logging.getLogger(__name__)


class StepStatus(str, Enum):
    """Execution status of a single saga step."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"
    TIMED_OUT = "timed_out"


class SagaStatus(str, Enum):
    """Overall execution status of a saga."""

    PENDING = "pending"
    RUNNING = "running"
    # All steps completed successfully.
    COMPLETED = "completed"
    # A step failed; compensation is in progress.
    COMPENSATING = "compensating"
    # A step failed; all completed steps were successfully compensated.
    COMPENSATED = "compensated"
    # A step failed AND one or more compensations also failed.
    # The system may be in an inconsistent state.
    PARTIALLY_COMPENSATED = "partially_compensated"
    # An unexpected exception occurred inside the saga orchestrator itself
    # (not inside a user-supplied action or compensation).
    ORCHESTRATION_ERROR = "orchestration_error"


@dataclass
class RetryPolicy:
    """Configuration for retrying failed saga steps.

    Attributes:
        max_attempts: Total number of attempts (including the initial try).
        backoff_base: Base delay in seconds; doubles on each retry.
        backoff_max: Upper bound on backoff delay in seconds.
        retryable_exceptions: Tuple of exception types that should trigger
            a retry. ``None`` means all exceptions are retryable; an empty
            tuple means no exceptions are retryable (effectively disables
            retries even if max_attempts > 1).
    """

    max_attempts: int = 3
    backoff_base: float = 1.0
    backoff_max: float = 60.0
    retryable_exceptions: Optional[Tuple[type, ...]] = None

    def is_retryable(self, exc: Exception) -> bool:
        if self.retryable_exceptions is None:
            return True
        if not self.retryable_exceptions:
            return False
        return isinstance(exc, self.retryable_exceptions)

    def delay_for_attempt(self, attempt: int) -> float:
        """Exponential backoff delay for a given attempt number (1-indexed)."""
        return min(self.backoff_base * (2 ** (attempt - 1)), self.backoff_max)


@dataclass
class SagaStep:
    """A single unit of work and its corresponding undo operation.

    Attributes:
        name: Human-readable label used in logs and the DLQ.
        action: Callable that performs the local transaction.
            Maybe sync or async. Must be idempotent if a retry policy
            is configured.
        compensation: Callable that undoes the local transaction.
            Maybe sync or async. Must be idempotent.
        retry_policy: Optional retry configuration applied to both
            the action and its compensation.
        timeout: Optional per-invocation timeout in seconds applied to
            both the action and its compensation.
    """

    name: str
    action: Callable[[], Any]
    compensation: Callable[[], Any]
    retry_policy: Optional[RetryPolicy] = None
    timeout: Optional[float] = None

    # Runtime state — populated during execution, not set by callers.
    status: StepStatus = field(default=StepStatus.PENDING, init=False)
    attempts: int = field(default=0, init=False)
    error: Optional[Exception] = field(default=None, init=False)
    started_at: Optional[float] = field(default=None, init=False)
    completed_at: Optional[float] = field(default=None, init=False)


@dataclass
class SagaResult:
    """Outcome of a completed saga execution.

    Attributes:
        saga_id: Unique identifier for this execution.
        saga_name: Human-readable name for this saga transaction. Used in logs,
            dead letter entries, and recovery state. If not provided,
            defaults to the saga_id.
        status: Final status of the saga.
        steps: Snapshot of all steps with their final statuses.
        total_time: Wall-clock duration in seconds.
        error: The error that caused failure, if any.
        failed_step_index: Index of the step that triggered compensation,
            if any.
    """

    saga_id: str
    saga_name: str
    status: SagaStatus
    steps: List[SagaStep]
    total_time: float
    error: Optional[Exception] = None
    failed_step_index: Optional[int] = None

    @property
    def succeeded(self) -> bool:
        """True if all steps completed without error."""
        return self.status == SagaStatus.COMPLETED

    @property
    def step_failed(self) -> bool:
        """True if a step failed (regardless of compensation outcome)."""
        return self.status in (
            SagaStatus.COMPENSATED,
            SagaStatus.PARTIALLY_COMPENSATED,
            SagaStatus.ORCHESTRATION_ERROR,
        )

    @property
    def needs_intervention(self) -> bool:
        """True if the system may be inconsistent and requires manual review."""
        return self.status == SagaStatus.PARTIALLY_COMPENSATED


@dataclass
class CompensationResult:
    """Outcome of a compensation phase.

    Attributes:
        success: True if all compensations are completed without error.
        compensated_count: Number of steps successfully compensated.
        failures: List of (step_index, step_name, exception) for each
            failed compensation.
    """

    success: bool
    compensated_count: int
    failures: List[Tuple[int, str, Exception]] = field(default_factory=list)


class Saga(ObjectIdentityMixin):
    """Orchestrates a sequence of distributed transactions with compensation.

    If compensation fails, the failure is automatically persisted to the
    underlying storage spine via ``DeadLetterEntry.enqueue()``.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        enable_recovery: bool = True,
        compensate_failed_step: bool = False,
    ):
        """
        Initialize a new Saga.

        Parameters
        ----------
        name:
            Human-readable name for this saga transaction. Used in logs,
            dead letter entries, and recovery state. If not provided,
            defaults to the saga_id.
        enable_recovery:
            If True, saves recovery state before each step execution.
        compensate_failed_step:
            If True, attempts to compensate for the failed step itself (useful if
            the step's action partially succeeded). If False (default), it only
            compensates for previously completed steps.
        """
        super().__init__()

        self._steps: List[SagaStep] = []
        self._status = SagaStatus.PENDING
        self.name = name or self._id
        self.enable_recovery = enable_recovery
        self.compensate_failed_step = compensate_failed_step
        self._created_at = time.time()  # Wall clock for timestamps
        self._started_at: Optional[float] = None  # Monotonic for duration

    @property
    def saga_id(self) -> str:
        return self._id

    def add_step(
        self,
        name: str,
        action: Callable[[], Any],
        compensation: Callable[[], Any],
        retry_policy: Optional[RetryPolicy] = None,
        timeout: Optional[float] = None,
    ) -> "Saga":
        if self._status != SagaStatus.PENDING:
            raise RuntimeError(
                "Saga '%s' (%s) has already been executed (status: %s)."
                % (self.name, self.saga_id, self._status)
            )
        if not callable(action):
            raise ValueError(
                "'action' must be callable, got %r." % type(action).__name__
            )
        if not callable(compensation):
            raise ValueError(
                "'compensation' must be callable, got %r." % type(compensation).__name__
            )

        self._steps.append(
            SagaStep(
                name=name,
                action=action,
                compensation=compensation,
                retry_policy=retry_policy,
                timeout=timeout,
            )
        )
        return self

    async def execute(self, start_at_index: int = 0) -> SagaResult:
        """Run all steps in order; compensate on failure with durable recovery."""
        if self._status not in (SagaStatus.PENDING, SagaStatus.RUNNING):
            raise RuntimeError(
                "Saga '%s' (%s) is in invalid status '%s' for execution."
                % (self.name, self.saga_id, self._status)
            )
        if not self._steps:
            raise RuntimeError(
                "Saga '%s' (%s) has no steps." % (self.name, self.saga_id)
            )

        self._status = SagaStatus.RUNNING
        self._started_at = time.monotonic()
        failed_step_index: Optional[int] = None
        error: Optional[Exception] = None

        logger.info(
            "Starting saga '%s' (%s) with %d steps",
            self.name,
            self.saga_id,
            len(self._steps),
        )

        try:
            for i in range(start_at_index, len(self._steps)):
                step = self._steps[i]

                if self.enable_recovery:
                    await SagaRecoveryManager.save_state(
                        saga_id=self.saga_id,
                        saga_name=self.name,
                        current_step_index=i,
                        status=self._status.value,
                        steps=self._steps,
                        created_at=self._created_at,
                    )

                try:
                    await self._execute_step(i, step)
                except (SagaStepFailureError, SagaTimeoutError) as exc:
                    failed_step_index = i
                    error = exc
                    break

            if failed_step_index is not None:
                await self._run_compensation(failed_step_index, error)
            else:
                self._status = SagaStatus.COMPLETED
                logger.info(
                    "Saga '%s' (%s) completed successfully in %.2fs",
                    self.name,
                    self.saga_id,
                    time.monotonic() - self._started_at,
                )

                # Clean up recovery state on successful execution
                if self.enable_recovery:
                    await SagaRecoveryManager.delete_state(self.saga_id)

        except Exception as exc:
            self._status = SagaStatus.ORCHESTRATION_ERROR
            error = exc
            logger.exception(
                "Saga '%s' (%s) orchestration error: %s",
                self.name,
                self.saga_id,
                exc,
            )

        total_time = time.monotonic() - self._started_at
        return SagaResult(
            saga_id=self.saga_id,
            saga_name=self.name,
            status=self._status,
            steps=list(self._steps),
            total_time=total_time,
            error=error,
            failed_step_index=failed_step_index,
        )

    async def _execute_step(self, index: int, step: SagaStep) -> None:
        """
        Executes a single step's action in the saga with support for retries
        and timeouts. This function ensures robust handling of failures and
        timeouts during the step execution.

        Steps are marked with appropriate statuses (`RUNNING`, `SUCCEEDED`,
        `TIMED_OUT`, or `FAILED`) based on the execution outcome. Retry policies
        are honored where applicable, including retry delays and determining whether
        an exception is retryable. Upon failure after retries are exhausted, the
        appropriate exception is raised.

        :param index: The index of the step within the saga.
        :type index: int
        :param step: The step to execute, including its action, retry policy,
            and timeout configuration.
        :type step: SagaStep
        :return: None
        :raises SagaTimeoutError: If the step execution exceeds the defined timeout.
        :raises SagaStepFailureError: If the step execution fails due to an
            unhandled exception after exhausting all retries.
        """
        step.status = StepStatus.RUNNING
        step.started_at = time.monotonic()
        max_attempts = step.retry_policy.max_attempts if step.retry_policy else 1
        last_exc: Exception = Exception(
            "Step execution failed with no exception captured"
        )

        for attempt in range(1, max_attempts + 1):
            step.attempts = attempt
            try:
                await _invoke(step.action, step.timeout)
                step.status = StepStatus.SUCCEEDED
                step.completed_at = time.monotonic()
                logger.debug(
                    "Saga '%s' step %d ('%s') succeeded (attempt %d)",
                    self.saga_id,
                    index,
                    step.name,
                    attempt,
                )
                return

            except asyncio.TimeoutError:
                last_exc = SagaTimeoutError(index, step.name, step.timeout)
                step.status = StepStatus.TIMED_OUT
                logger.warning(
                    "Saga '%s' (%s) step %d ('%s') timed out after %.1fs (attempt %d/%d)",
                    self.name,
                    self.saga_id,
                    index,
                    step.name,
                    step.timeout,
                    attempt,
                    max_attempts,
                )

            except asyncio.CancelledError:
                # Preserve cancellation - don't retry
                last_exc = Exception(f"Step '{step.name}' was cancelled")
                logger.warning(
                    "Saga '%s' (%s) step %d ('%s') was cancelled",
                    self.name,
                    self.saga_id,
                    index,
                    step.name,
                )
                break

            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Saga '%s' (%s) step %d ('%s') failed on attempt %d/%d: %s",
                    self.name,
                    self.saga_id,
                    index,
                    step.name,
                    attempt,
                    max_attempts,
                    exc,
                )

            if attempt < max_attempts and step.retry_policy is not None:
                if step.retry_policy.is_retryable(last_exc):
                    delay = step.retry_policy.delay_for_attempt(attempt)
                    logger.info(
                        "Saga '%s' (%s) step %d ('%s') retrying in %.1fs",
                        self.name,
                        self.saga_id,
                        index,
                        step.name,
                        delay,
                    )
                    try:
                        await asyncio.sleep(delay)
                    except asyncio.CancelledError:
                        logger.warning(
                            "Saga '%s' (%s) retry sleep cancelled for step %d ('%s')",
                            self.name,
                            self.saga_id,
                            index,
                            step.name,
                        )
                        break
                else:
                    logger.info(
                        "Saga '%s' (%s) step %d ('%s') — %s is not retryable, stopping",
                        self.name,
                        self.saga_id,
                        index,
                        step.name,
                        type(last_exc).__name__,
                    )
                    break

        step.status = StepStatus.FAILED
        step.completed_at = time.monotonic()
        step.error = last_exc

        # SagaTimeoutError is already the right type; wrap everything else.
        if isinstance(last_exc, SagaError):
            raise last_exc
        raise SagaStepFailureError(
            step_index=index,
            step_name=step.name,
            original_error=last_exc,
        )

    async def _run_compensation(
        self,
        failed_step_index: int,
        original_error: Optional[Exception],
    ) -> None:
        self._status = SagaStatus.COMPENSATING

        # Determine compensation range based on configuration
        if self.compensate_failed_step:
            # Include the failed step itself
            start_index = failed_step_index
        else:
            # Only compensate successfully completed steps
            start_index = failed_step_index - 1

        indices = list(range(start_index, -1, -1))

        logger.info(
            "Saga '%s' (%s) compensating steps %s (failed at index %d: %s)",
            self.name,
            self.saga_id,
            indices,
            failed_step_index,
            original_error,
        )

        # Checkpoint compensating status
        if self.enable_recovery:
            await SagaRecoveryManager.save_state(
                saga_id=self.saga_id,
                saga_name=self.name,
                current_step_index=failed_step_index,
                status=self._status.value,
                steps=self._steps,
                created_at=self._created_at,
            )

        result = await self._compensate_steps(indices)

        if result.success:
            self._status = SagaStatus.COMPENSATED
            logger.info(
                "Saga '%s' (%s) fully compensated after %d step(s)",
                self.name,
                self.saga_id,
                result.compensated_count,
            )
            if self.enable_recovery:
                await SagaRecoveryManager.delete_state(self.saga_id)
        else:
            self._status = SagaStatus.PARTIALLY_COMPENSATED
            logger.error(
                "Saga '%s' (%s) PARTIALLY compensated — %d compensation(s) failed. Manual intervention required.",
                self.name,
                self.saga_id,
                len(result.failures),
            )
            await self._enqueue_dead_letter(failed_step_index, original_error, result)
            # Retain recovery state for manual operator CLI investigation

    async def _compensate_steps(self, indices: List[int]) -> CompensationResult:
        """Execute compensation for each index in ``indices``.

        All compensations are attempted even if some fail — we never stop
        early. Each compensation respects the step's retry policy, including
        ``is_retryable`` checks (same policy as action retries).

        Returns:
            ``CompensationResult`` summarising the outcome.
        """
        failures: List[Tuple[int, str, Exception]] = []
        compensated_count = 0

        for i in indices:
            step = self._steps[i]
            step.status = StepStatus.COMPENSATING
            max_attempts = step.retry_policy.max_attempts if step.retry_policy else 1
            last_exc: Optional[Exception] = None
            succeeded = False

            for attempt in range(1, max_attempts + 1):
                try:
                    await _invoke(step.compensation, step.timeout)
                    step.status = StepStatus.COMPENSATED
                    compensated_count += 1
                    succeeded = True
                    logger.debug(
                        "Saga '%s' (%s) step %d ('%s') compensated (attempt %d)",
                        self.name,
                        self.saga_id,
                        i,
                        step.name,
                        attempt,
                    )
                    break

                except asyncio.TimeoutError:
                    last_exc = SagaTimeoutError(i, step.name, step.timeout)
                    logger.warning(
                        "Saga '%s' (%s) step %d ('%s') compensation timed out (attempt %d/%d)",
                        self.name,
                        self.saga_id,
                        i,
                        step.name,
                        attempt,
                        max_attempts,
                    )

                except Exception as exc:
                    last_exc = exc
                    logger.warning(
                        "Saga '%s' (%s) step %d ('%s') compensation failed (attempt %d/%d): %s",
                        self.name,
                        self.saga_id,
                        i,
                        step.name,
                        attempt,
                        max_attempts,
                        exc,
                    )

                if attempt < max_attempts and step.retry_policy is not None:
                    if step.retry_policy.is_retryable(last_exc):
                        delay = step.retry_policy.delay_for_attempt(attempt)
                        await asyncio.sleep(delay)
                    else:
                        break

            if not succeeded:
                step.status = StepStatus.COMPENSATION_FAILED
                step.error = last_exc
                failures.append((i, step.name, last_exc))

        return CompensationResult(
            success=not failures,
            compensated_count=compensated_count,
            failures=failures,
        )

    @classmethod
    async def resume(cls, saga_id: str, step_definitions: List[SagaStep]) -> SagaResult:
        """Rehydrates an interrupted saga from the disk and completes execution or compensation."""
        state = await SagaRecoveryManager.load_state(saga_id)
        if not state:
            raise ValueError(
                "No active recovery checkpoint found for saga '%s'" % saga_id
            )

        saga = cls(enable_recovery=True)
        saga.change_object_id(saga_id)
        saga.name = state.saga_name
        saga._steps = step_definitions
        saga._created_at = state.created_at

        # Validate step count matches
        if len(saga._steps) != len(state.steps_state):
            raise ValueError(
                "Saga '%s' (%s): Step count mismatch - recovery state has %d steps but %d definitions provided"
                % (saga.name, saga_id, len(state.steps_state), len(saga._steps))
            )

        # Rehydrate individual step runtime states
        for i, step_state in enumerate(state.steps_state):
            step = saga._steps[i]

            # Validate step names match for safety
            if step.name != step_state.get("name"):
                logger.warning(
                    "Saga '%s' (%s): Step name mismatch at index %d: expected '%s', got '%s'",
                    saga.name,
                    saga_id,
                    i,
                    step_state.get("name"),
                    step.name,
                )

            # Restore all runtime state fields
            step.status = StepStatus(step_state["status"])
            step.attempts = step_state.get("attempts", 0)

            if step_state.get("error"):
                # Reconstruct exception with original message
                step.error = Exception(step_state["error"])

            step.started_at = step_state.get("started_at")
            step.completed_at = step_state.get("completed_at")

        logger.info(
            "Resuming saga '%s' (%s) from step index %d (status: %s)",
            saga.name,
            saga_id,
            state.current_step_index,
            state.status,
        )

        if state.status == SagaStatus.COMPENSATING.value:
            # Re-trigger compensation if a node died during rollback
            await saga._run_compensation(
                state.current_step_index,
                Exception("Resumed from crash mid-compensation"),
            )
            return SagaResult(
                saga_id=saga.saga_id,
                saga_name=saga.name,
                status=saga._status,
                steps=list(saga._steps),
                total_time=0.0,
            )
        else:
            # Resume forward execution from the last uncompleted step
            return await saga.execute(start_at_index=state.current_step_index)

    async def _enqueue_dead_letter(
        self,
        failed_step_index: int,
        original_error: Optional[Exception],
        compensation_result: CompensationResult,
    ) -> None:
        failed_step = self._steps[failed_step_index]

        entry = DeadLetterEntry(
            saga_id=self.saga_id,
            saga_name=self.name,
            timestamp=time.time(),
            failed_step_index=failed_step_index,
            failed_step_name=failed_step.name,
            original_error=str(original_error or "unknown"),
            compensation_failures=[
                {"step_index": idx, "step_name": name, "error": str(exc)}
                for idx, name, exc in compensation_result.failures
            ],
            steps_state=[
                {
                    "index": i,
                    "name": s.name,
                    "status": s.status.value,
                    "attempts": s.attempts,
                    "error": str(s.error) if s.error else None,
                    "started_at": s.started_at,
                    "completed_at": s.completed_at,
                    "duration": (
                        s.completed_at - s.started_at
                        if s.started_at and s.completed_at
                        else None
                    ),
                }
                for i, s in enumerate(self._steps)
            ],
        )

        await DeadLetterEntry.enqueue(entry)


async def execute_saga(
    steps: List[SagaStep],
    name: Optional[str] = None,
    enable_recovery: bool = True,
    compensate_failed_step: bool = False,
) -> SagaResult:
    """
    Build and execute a Saga from a sequence of ``SagaStep`` instances.

    Example:
        result = await execute_saga(
            name="order_fulfillment",
            steps=[
            SagaStep(
                name="write_checkpoint",
                action=lambda: redis_backend.insert(...),
                compensation=lambda: redis_backend.delete(...),
                retry_policy=RetryPolicy(max_attempts=3),
                timeout=30.0,
            ),
            SagaStep(
                name="record_trace",
                action=lambda: pg_backend.insert(...),
                compensation=lambda: pg_backend.update(...),
            ),
        ])

    :param steps: Ordered collection of ``SagaStep`` objects containing definitions and
                  retry policies for each step of the saga's execution process.
    :type steps: List[SagaStep]

    :param enable_recovery: Flag indicating whether recovery actions should be performed in
                            case of partial execution or failure during the saga. Defaults
                            to ``True``.
    :type enable_recovery: bool, optional

    :param compensate_failed_step: Flag indicating whether compensation should be performed
                                   in case of a failed step. Defaults to ``False``.
    :type compensate_failed_step: bool, optional

    :return: Outcome of the saga's execution, encapsulated in a ``SagaResult`` object which
             details the success or failure, as well as any compensatory actions taken.
    :rtype: SagaResult
    """
    saga = Saga(
        name=name,
        enable_recovery=enable_recovery,
        compensate_failed_step=compensate_failed_step,
    )
    for step in steps:
        saga.add_step(
            name=step.name,
            action=step.action,
            compensation=step.compensation,
            retry_policy=step.retry_policy,
            timeout=step.timeout,
        )
    return await saga.execute()


async def _invoke(fn: Callable[[], Any], timeout: Optional[float] = None) -> Any:
    """Call ``fn()``; await the result if it is a coroutine.

    If ``timeout`` is provided, the entire call is wrapped in
    ``asyncio.wait_for``. The caller is responsible for catching
    ``asyncio.TimeoutError``.
    """
    if timeout is not None:
        return await asyncio.wait_for(as_coroutine(fn), timeout=timeout)
    return await as_coroutine(fn)
