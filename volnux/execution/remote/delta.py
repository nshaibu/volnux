from __future__ import annotations
import logging
import typing
from dataclasses import asdict, dataclass, field, fields

from .context import MiniExecutionContext

logger = logging.getLogger(__name__)


@dataclass
class ContextDelta:
    """
    Changes produced by a remote task, to be applied to the full
    ExecutionContext by the coordinator.

    In small mode, new_results and new_errors carry the full data.
    In batch/aggregate mode, bulk data was streamed to Redis; the
    coordinator must read from batch_key instead, and new_results /
    new_errors will be empty.

    :ivar state_id: Identifies which ExecutionContext to patch.
    :ivar mode: The MiniExecutionContext mode at finalization time.
        Tells the coordinator where the bulk data lives.
    :ivar batch_key: Redis key holding streamed batches (batch/aggregate
        modes only). None in small mode.
    :ivar new_results: Full result dicts (small mode only).
    :ivar new_errors: Full error dicts (small mode only).
    :ivar final_status: Execution status string to apply, if any.
    :ivar end_time: Unix timestamp of task completion.
    :ivar checkpoint_data: Arbitrary checkpoint payload from task metadata.
    :ivar execution_duration: Wall-clock seconds for the task.
    :ivar worker_hostname: Hostname of the worker that ran the task.
    :ivar worker_pid: PID of the worker process.
    """

    state_id: str

    # Mode metadata — tells the coordinator where bulk data lives
    mode: str = "small"
    batch_key: typing.Optional[str] = None

    # Small-mode payload (empty in batch/aggregate)
    new_results: typing.List[typing.Dict] = field(default_factory=list)
    new_errors: typing.List[typing.Dict] = field(default_factory=list)

    # Execution metadata
    final_status: typing.Optional[str] = None
    end_time: typing.Optional[float] = None
    checkpoint_data: typing.Optional[typing.Dict] = None

    # Worker provenance
    execution_duration: float = 0.0
    worker_hostname: typing.Optional[str] = None
    worker_pid: typing.Optional[int] = None

    @classmethod
    def compute(
        cls,
        mini_context: MiniExecutionContext,
        execution_duration: float = 0.0,
        worker_info: typing.Optional[typing.Dict] = None,
        batch_key: typing.Optional[str] = None,
    ) -> "ContextDelta":
        """
        Build a ContextDelta from a finalised MiniExecutionContext.

        Must be called after ``mini_context.finalize()`` so that
        end_time is set and all buffers are drained.

        :param mini_context: The finalised mini-context from the worker.
        :param execution_duration: Wall-clock seconds for the task.
        :param worker_info: Dict with optional ``hostname`` and ``pid`` keys.
        :param batch_key: Redis key where batches were streamed (required
            when mini_context.mode is "batch" or "aggregate").
        """
        worker_info = worker_info or {}

        # In batch/aggregate mode results/errors were flushed to Redis;
        # reading mini_context.results here would yield empty lists.
        if mini_context.mode == "small":
            new_results = list(mini_context.results)
            new_errors = list(mini_context.errors)
        else:
            new_results = []
            new_errors = []

        # Infer status: failed if any errors were recorded.
        if mini_context.total_failed > 0 or mini_context.errors:
            final_status = "FAILED"
        elif mini_context.total_processed > 0 or mini_context.results:
            final_status = "COMPLETED"
        else:
            final_status = None

        return cls(
            state_id=mini_context.state_id,
            mode=mini_context.mode,
            batch_key=batch_key,
            new_results=new_results,
            new_errors=new_errors,
            final_status=final_status,
            end_time=mini_context.end_time or None,
            checkpoint_data=mini_context.metadata.get("checkpoint"),
            execution_duration=execution_duration,
            worker_hostname=worker_info.get("hostname"),
            worker_pid=worker_info.get("pid"),
        )

    async def apply_to_context(self, context: "ExecutionContext") -> None:  # type: ignore[name-defined]
        """
        Patch a full ExecutionContext with this delta.

        For batch/aggregate mode contexts, the caller is responsible for
        reading the Redis stream at ``self.batch_key`` and applying those
        results separately — this method only handles the small-mode payload
        and metadata fields.

        :param context: The live ExecutionContext to update.
        """
        # Deferred to avoid circular imports — ExecutionContext lives in a
        # module that imports from this one.
        from volnux.execution.status import ExecutionStatus
        from volnux.result import EventResult

        # Apply small-mode results
        for result_dict in self.new_results:
            try:
                result = EventResult(**result_dict)
            except Exception as e:
                logger.warning(
                    "Failed to reconstruct EventResult from dict: %s",
                    result_dict,
                    exc_info=e,
                )

                if not isinstance(result_dict, dict):
                    result_dict = {"content": result_dict}

                result = EventResult(
                    error=False,
                    content=result_dict.get("content", ""),
                    event_name=result_dict.get("event_name", "Unknown"),
                    task_id=result_dict.get("task_id", ""),
                )
            await context.add_result_async(result)

        # Apply small-mode errors — convert to error EventResults so they
        # are not silently dropped.
        for error_dict in self.new_errors:
            result = EventResult(
                error=True,
                content=error_dict.get("content", ""),
                task_id=error_dict.get("task_id", ""),
                event_name=error_dict.get("type", "RemoteError"),
            )
            await context.add_result_async(result)

        if self.final_status:
            try:
                await context.update_status_async(ExecutionStatus[self.final_status])
            except KeyError as e:
                logger.warning("Unknown status string: %s", self.final_status)
                pass  # unknown status string — don't corrupt the context

        if self.end_time:
            context.metrics.end_time = self.end_time

        if self.checkpoint_data:
            context.set_task_checkpoint(self.checkpoint_data)

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: typing.Dict[str, typing.Any]) -> "ContextDelta":
        # Filter to known fields so older/newer workers don't cause TypeError
        # on unexpected or missing keys during rolling deploys.
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})
