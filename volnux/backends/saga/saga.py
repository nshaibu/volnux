import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, List, Optional, Tuple, Type

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
    CANCELLED = "cancelled"


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
            a retry. Step timeouts surface as ``asyncio.TimeoutError``.
            ``None`` means all exceptions are retryable; an empty
            tuple means no exceptions are retryable (effectively disables
            retries even if max_attempts > 1).
    """

    max_attempts: int = 3
    backoff_base: float = 1.0
    backoff_max: float = 60.0
    retryable_exceptions: Optional[Tuple[Type[BaseException], ...]] = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1, got %r." % self.max_attempts)
        if self.backoff_base < 0 or self.backoff_max < 0:
            raise ValueError("backoff_base and backoff_max must be >= 0.")

    def is_retryable(self, exc: BaseException) -> bool:
        """Return True if ``exc`` should trigger another attempt.

        A step timeout is matched as ``asyncio.TimeoutError`` (the raw
        exception), so include it in ``retryable_exceptions`` to retry timeouts.
        """
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
            both the action and its compensation. For sync callables the
            timeout only stops waiting; the worker thread keeps running.
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
        error: The error that caused failure, if any. When compensation also
            failed this is a ``SagaCompensationFailureError`` whose
            ``original_error`` is the triggering failure.
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

    Delivery semantics: checkpoints are written *before* each step, so after a
    crash ``resume`` re-runs the in-flight step. Actions and compensations
    should therefore be idempotent.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        enable_recovery: bool = True,
        compensate_failed_step: bool = False,
    ) -> None:
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
        self.name: str = name or self.id
        self.enable_recovery = enable_recovery
        self.compensate_failed_step = compensate_failed_step
        self._created_at = time.time()  # Wall clock for timestamps
        self._started_at: Optional[float] = None  # Monotonic for duration

    @property
    def saga_id(self) -> str:
        return self.id

    @property
    def status(self) -> SagaStatus:
        return self._status

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
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Step 'name' must be a non-empty string.")
        if any(s.name == name for s in self._steps):
            raise ValueError("Duplicate step name '%s'." % name)
        if not callable(action):
            raise ValueError(
                "'action' must be callable, got %r." % type(action).__name__
            )
        if not callable(compensation):
            raise ValueError(
                "'compensation' must be callable, got %r." % type(compensation).__name__
            )
        if timeout is not None and timeout <= 0:
            raise ValueError("'timeout' must be positive, got %r." % timeout)

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

    async def _checkpoint(
        self,
        current_step_index: int,
        failed_step_index: Optional[int] = None,
    ) -> None:
        await SagaRecoveryManager.save_state(
            saga_id=self.saga_id,
            saga_name=self.name,
            current_step_index=current_step_index,
            status=self._status.value,
            steps=self._steps,
            created_at=self._created_at,
            compensate_failed_step=self.compensate_failed_step,
            failed_step_index=failed_step_index,
        )

    async def execute(self, start_at_index: int = 0) -> SagaResult:
        """Run all steps in order; compensate on failure with durable recovery.

        If the running task is cancelled, compensation still runs (shielded)
        and the ``CancelledError`` is then re-raised.
        """
        if self._status != SagaStatus.PENDING:
            raise RuntimeError(
                "Saga '%s' (%s) is in invalid status '%s' for execution."
                % (self.name, self.saga_id, self._status)
            )
        if not self._steps:
            raise RuntimeError(
                "Saga '%s' (%s) has no steps." % (self.name, self.saga_id)
            )
        if not 0 <= start_at_index <= len(self._steps):
            raise ValueError(
                "start_at_index %d out of range for %d steps."
                % (start_at_index, len(self._steps))
            )

        self._status = SagaStatus.RUNNING
        self._started_at = time.monotonic()
        failed_step_index: Optional[int] = None
        error: Optional[Exception] = None
        include_failed = self.compensate_failed_step
        cancelled: Optional[asyncio.CancelledError] = None

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
                    try:
                        await self._checkpoint(i)
                    except Exception as exc:
                        # Without a durable checkpoint we cannot safely run
                        # the step. Roll back what already ran. The step
                        # itself never started, so it is not compensated.
                        logger.error(
                            "Saga '%s' (%s) could not checkpoint before step %d: %s",
                            self.name,
                            self.saga_id,
                            i,
                            exc,
                        )
                        failed_step_index = i
                        error = SagaStepFailureError(i, step.name, exc)
                        step.error = exc
                        include_failed = False
                        break

                try:
                    await self._execute_step(i, step)
                except (SagaStepFailureError, SagaTimeoutError) as exc:
                    failed_step_index = i
                    error = exc
                    break
                except asyncio.CancelledError as exc:
                    failed_step_index = i
                    error = SagaStepFailureError(
                        i, step.name, Exception("Step '%s' was cancelled" % step.name)
                    )
                    cancelled = exc
                    break

            if failed_step_index is not None:
                comp = asyncio.ensure_future(
                    self._run_compensation(
                        failed_step_index, error, include_failed=include_failed
                    )
                )
                try:
                    comp_result = await asyncio.shield(comp)
                except asyncio.CancelledError as exc:
                    # Cancelled while compensating: let compensation finish in
                    # the background, then propagate the cancellation.
                    cancelled = cancelled or exc
                    comp_result = None
                if (
                    comp_result is not None
                    and not comp_result.success
                    and error is not None
                ):
                    error = SagaCompensationFailureError(error, comp_result.failures)
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
                    await self._cleanup_checkpoint()

        except Exception as exc:
            self._status = SagaStatus.ORCHESTRATION_ERROR
            error = exc
            logger.exception(
                "Saga '%s' (%s) orchestration error: %s",
                self.name,
                self.saga_id,
                exc,
            )

        if cancelled is not None:
            raise cancelled

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

    async def _cleanup_checkpoint(self) -> None:
        """Delete the checkpoint; a cleanup failure must not change the outcome."""
        try:
            await SagaRecoveryManager.delete_state(self.saga_id)
        except Exception as exc:
            logger.warning(
                "Saga '%s' (%s) could not delete recovery state: %s",
                self.name,
                self.saga_id,
                exc,
            )

    async def _execute_step(self, index: int, step: SagaStep) -> None:
        """
        Executes a single step's action in the saga with support for retries
        and timeouts. This function ensures robust handling of failures and
        timeouts during the step execution.

        Steps are marked with appropriate statuses (`RUNNING`, `SUCCEEDED`,
        `TIMED_OUT`, `CANCELLED` or `FAILED`) based on the execution outcome.
        Retry policies are honored where applicable, including retry delays and
        determining whether an exception is retryable.

        :param index: The index of the step within the saga.
        :param step: The step to execute.
        :raises SagaTimeoutError: If the step execution exceeds the defined timeout.
        :raises SagaStepFailureError: If the step execution fails due to an
            unhandled exception after exhausting all retries.
        :raises asyncio.CancelledError: If the task is cancelled (never retried).
        """
        step.status = StepStatus.RUNNING
        step.started_at = time.monotonic()
        max_attempts = step.retry_policy.max_attempts if step.retry_policy else 1
        last_exc: Exception = Exception(
            "Step execution failed with no exception captured"
        )

        for attempt in range(1, max_attempts + 1):
            step.attempts = attempt
            raw_exc: Optional[Exception] = None
            try:
                await _invoke(step.action, step.timeout)
                step.status = StepStatus.SUCCEEDED
                step.completed_at = time.monotonic()
                logger.debug(
                    "Saga '%s' (%s) step %d ('%s') succeeded (attempt %d)",
                    self.name,
                    self.saga_id,
                    index,
                    step.name,
                    attempt,
                )
                return

            except asyncio.CancelledError:
                # Never swallow cancellation: record and propagate.
                step.status = StepStatus.CANCELLED
                step.completed_at = time.monotonic()
                step.error = Exception("Step '%s' was cancelled" % step.name)
                logger.warning(
                    "Saga '%s' (%s) step %d ('%s') was cancelled",
                    self.name,
                    self.saga_id,
                    index,
                    step.name,
                )
                raise

            except Exception as exc:
                raw_exc = exc
                if isinstance(exc, asyncio.TimeoutError) and step.timeout is not None:
                    last_exc = SagaTimeoutError(index, step.name, step.timeout)
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
                else:
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
                if step.retry_policy.is_retryable(raw_exc):
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
                        step.status = StepStatus.CANCELLED
                        step.error = Exception("Step '%s' was cancelled" % step.name)
                        raise
                else:
                    logger.info(
                        "Saga '%s' (%s) step %d ('%s') — %s is not retryable, stopping",
                        self.name,
                        self.saga_id,
                        index,
                        step.name,
                        type(raw_exc).__name__,
                    )
                    break

        step.completed_at = time.monotonic()
        step.error = last_exc

        # SagaTimeoutError is already the right type; wrap everything else.
        if isinstance(last_exc, SagaTimeoutError):
            step.status = StepStatus.TIMED_OUT
            raise last_exc
        step.status = StepStatus.FAILED
        raise SagaStepFailureError(
            step_index=index,
            step_name=step.name,
            original_error=last_exc,
        )

    async def _run_compensation(
        self,
        failed_step_index: int,
        original_error: Optional[Exception],
        include_failed: Optional[bool] = None,
    ) -> CompensationResult:
        self._status = SagaStatus.COMPENSATING
        if include_failed is None:
            include_failed = self.compensate_failed_step

        # Determine compensation range based on configuration
        start_index = failed_step_index if include_failed else failed_step_index - 1
        indices = list(range(start_index, -1, -1))

        logger.info(
            "Saga '%s' (%s) compensating steps %s (failed at index %d: %s)",
            self.name,
            self.saga_id,
            indices,
            failed_step_index,
            original_error,
        )

        # Checkpoint compensating status. Compensation matters more than the
        # checkpoint, so a persistence failure is logged but not fatal.
        await self._safe_checkpoint(failed_step_index)

        try:
            result = await self._compensate_steps(indices)
        except asyncio.CancelledError:
            self._status = SagaStatus.PARTIALLY_COMPENSATED
            raise

        if result.success:
            self._status = SagaStatus.COMPENSATED
            logger.info(
                "Saga '%s' (%s) fully compensated after %d step(s)",
                self.name,
                self.saga_id,
                result.compensated_count,
            )
            if self.enable_recovery:
                await self._cleanup_checkpoint()
        else:
            self._status = SagaStatus.PARTIALLY_COMPENSATED
            logger.error(
                "Saga '%s' (%s) PARTIALLY compensated — %d compensation(s) failed. Manual intervention required.",
                self.name,
                self.saga_id,
                len(result.failures),
            )
            # Persist per-step outcomes so a resume skips what already ran.
            await self._safe_checkpoint(failed_step_index)
            try:
                await self._enqueue_dead_letter(
                    failed_step_index, original_error, result
                )
            except Exception as exc:
                logger.error(
                    "Saga '%s' (%s) could not enqueue dead letter entry: %s",
                    self.name,
                    self.saga_id,
                    exc,
                )
            # Retain recovery state for manual operator CLI investigation
        return result

    async def _safe_checkpoint(self, failed_step_index: int) -> None:
        if not self.enable_recovery:
            return
        try:
            await self._checkpoint(failed_step_index, failed_step_index)
        except Exception as exc:
            logger.error(
                "Saga '%s' (%s) could not persist compensation checkpoint: %s",
                self.name,
                self.saga_id,
                exc,
            )

    async def _compensate_steps(self, indices: List[int]) -> CompensationResult:
        """Execute compensation for each index in ``indices``.

        All compensations are attempted even if some fail — we never stop
        early. Steps already COMPENSATED (e.g. restored by ``resume``) are
        skipped. Each compensation respects the step's retry policy.

        Returns:
            ``CompensationResult`` summarising the outcome.
        """
        failures: List[Tuple[int, str, Exception]] = []
        compensated_count = 0

        for i in indices:
            step = self._steps[i]
            if step.status == StepStatus.COMPENSATED:
                compensated_count += 1
                continue

            step.status = StepStatus.COMPENSATING
            max_attempts = step.retry_policy.max_attempts if step.retry_policy else 1
            last_exc: Exception = Exception(
                "Compensation failed with no exception captured"
            )
            succeeded = False

            for attempt in range(1, max_attempts + 1):
                raw_exc: Optional[Exception] = None
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

                except asyncio.CancelledError:
                    step.status = StepStatus.COMPENSATION_FAILED
                    raise

                except Exception as exc:
                    raw_exc = exc
                    if (
                        isinstance(exc, asyncio.TimeoutError)
                        and step.timeout is not None
                    ):
                        last_exc = SagaTimeoutError(i, step.name, step.timeout)
                    else:
                        last_exc = exc
                    logger.warning(
                        "Saga '%s' (%s) step %d ('%s') compensation failed (attempt %d/%d): %s",
                        self.name,
                        self.saga_id,
                        i,
                        step.name,
                        attempt,
                        max_attempts,
                        last_exc,
                    )

                if attempt < max_attempts and step.retry_policy is not None:
                    if step.retry_policy.is_retryable(raw_exc):
                        try:
                            await asyncio.sleep(
                                step.retry_policy.delay_for_attempt(attempt)
                            )
                        except asyncio.CancelledError:
                            step.status = StepStatus.COMPENSATION_FAILED
                            raise
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
        """Rehydrates an interrupted saga from its checkpoint and finishes it.

        ``step_definitions`` must be the same steps (same names, same order)
        the saga was originally built with.
        """
        state = await SagaRecoveryManager.load_state(saga_id)
        if not state:
            raise ValueError(
                "No active recovery checkpoint found for saga '%s'" % saga_id
            )

        steps = list(step_definitions)
        if len(steps) != len(state.steps_state):
            raise ValueError(
                "Saga '%s' (%s): Step count mismatch - recovery state has %d steps but %d definitions provided"
                % (state.saga_name, saga_id, len(state.steps_state), len(steps))
            )

        # Validate everything before mutating any step.
        parsed: List[Tuple[StepStatus, int, Optional[str]]] = []
        for i, step_state in enumerate(state.steps_state):
            if steps[i].name != step_state.get("name"):
                raise ValueError(
                    "Saga '%s' (%s): Step name mismatch at index %d: expected '%s', got '%s'"
                    % (
                        state.saga_name,
                        saga_id,
                        i,
                        step_state.get("name"),
                        steps[i].name,
                    )
                )
            try:
                step_status = StepStatus(step_state["status"])
            except (KeyError, ValueError) as exc:
                raise ValueError(
                    "Saga '%s' (%s): corrupt status for step %d: %r"
                    % (state.saga_name, saga_id, i, step_state.get("status"))
                ) from exc
            parsed.append(
                (step_status, step_state.get("attempts", 0), step_state.get("error"))
            )

        saga = cls(
            name=state.saga_name,
            enable_recovery=True,
            compensate_failed_step=bool(state.compensate_failed_step),
        )
        saga.change_object_id(saga_id)
        saga._steps = steps
        saga._created_at = state.created_at

        for step, (step_status, attempts, err) in zip(steps, parsed):
            step.status = step_status
            step.attempts = attempts
            step.error = Exception(err) if err else None

        logger.info(
            "Resuming saga '%s' (%s) from step index %d (status: %s)",
            saga.name,
            saga_id,
            state.current_step_index,
            state.status,
        )

        if state.status in (
            SagaStatus.COMPENSATING.value,
            SagaStatus.PARTIALLY_COMPENSATED.value,
        ):
            # Re-trigger compensation if a node died during rollback
            failed_index = (
                state.failed_step_index
                if state.failed_step_index is not None
                else state.current_step_index
            )
            started = time.monotonic()
            failed_error = steps[failed_index].error or Exception(
                "Resumed from crash mid-compensation"
            )
            comp_result = await saga._run_compensation(failed_index, failed_error)
            error: Optional[Exception] = failed_error
            if not comp_result.success:
                error = SagaCompensationFailureError(failed_error, comp_result.failures)
            return SagaResult(
                saga_id=saga.saga_id,
                saga_name=saga.name,
                status=saga._status,
                steps=list(saga._steps),
                total_time=time.monotonic() - started,
                error=error,
                failed_step_index=failed_index,
            )

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
                    "duration": (
                        s.completed_at - s.started_at
                        if s.started_at is not None and s.completed_at is not None
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

    The step definitions are *copied* into the saga: runtime state (status,
    attempts, ...) is reported on ``SagaResult.steps``, not on the objects
    passed in.

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
            ],
        )

    :param steps: Ordered step definitions.
    :param name: Optional saga name (defaults to the saga id).
    :param enable_recovery: Persist checkpoints for crash recovery. Defaults to ``True``.
    :param compensate_failed_step: Also compensate the failed step itself. Defaults to ``False``.
    :return: A ``SagaResult`` describing the outcome.
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
