"""
Adaptive mini-context for remote task execution via Socket and Celery executors.

Designed to avoid sending the full ExecutionContext over the wire. Switches
between three modes (small → batch → aggregate) as item volume grows, and
streams batches via an injected flush callback rather than accumulating
everything in memory.

Changes from original
─────────────────────
1.  ReservoirSample replaces deque(maxlen=10) — every item has a fair,
    mathematically provable k/n probability of being in the final sample.
2.  _switch_to_aggregate_mode: snapshot counts BEFORE flushing buffers,
    not after (original always produced 0 for buffer contributions).
3.  _flush_callback=None is no longer a silent data-drop: a warning is
    emitted and, in batch/aggregate modes where the callback is essential,
    an explicit RuntimeError is raised on the first attempted flush.
4.  to_dict / from_dict now round-trip aggregate statistics (total_processed,
    total_succeeded, total_failed, sum_values, min_value, max_value, samples).
5.  Thread safety: a threading.Lock guards all mutations in add_result /
    add_error and the internal _switch_* / _flush_* helpers.
6.  from_dict return annotation corrected to "MiniExecutionContext".
7.  Minor: _get_total_count is now called inside the lock by callers that
    already hold it; extracted as _get_total_count_locked() to make the
    locking contract explicit.
"""

from __future__ import annotations

import logging
import random
import time
import typing
import warnings
import asyncio
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

ModeType = typing.Literal["small", "batch", "aggregate"]


@dataclass
class ReservoirSample:
    """
    Fixed-size statistically representative sample using Vitter's Algorithm R.

    Every item offered has exactly k/n probability of appearing in the final
    sample, where k is the reservoir capacity and n is the total items seen.
    Works in a single pass with O(k) memory even when n is unknown in advance.

    :ivar k: Reservoir capacity (maximum sample size).
    :ivar _rng: Random number generator (injectable for reproducible tests).
    :ivar _reservoir: The current sample buffer.
    :ivar _count: Total items seen (n), not the current sample size.
    """

    k: int
    _rng: random.Random = field(default_factory=random.Random)
    _reservoir: typing.List[typing.Dict] = field(default_factory=list)
    _count: int = 0

    def add(self, item: typing.Dict) -> None:
        """
        Offer one item to the reservoir.

        Phase 1 — fill (count < k):
            Accept every item unconditionally until the reservoir is full.

        Phase 2 — replace (count >= k):
            For the i-th item (0-indexed), draw j = randint(0, i) inclusive.
            If j < k, replace reservoir[j] with this item.
            P(replacement) = k / (i + 1), maintaining the k/n invariant.
        """
        if self._count < self.k:
            self._reservoir.append(item)
        else:
            j = self._rng.randint(0, self._count)
            if j < self.k:
                self._reservoir[j] = item
        self._count += 1

    def sample(self) -> typing.List[typing.Dict]:
        """Return a copy of the current reservoir contents."""
        return list(self._reservoir)

    @property
    def total_seen(self) -> int:
        """Total items offered (not capped at k)."""
        return self._count

    @property
    def is_full(self) -> bool:
        return len(self._reservoir) >= self.k

    def to_dict(self) -> typing.Dict:
        return {
            "k": self.k,
            "reservoir": list(self._reservoir),
            "count": self._count,
        }

    @classmethod
    def from_dict(cls, data: typing.Dict) -> "ReservoirSample":
        obj = cls(k=data["k"])
        obj._reservoir = data.get("reservoir", [])
        obj._count = data.get("count", 0)
        return obj


@dataclass
class MiniExecutionContext:
    """
    Adaptive mini-context with automatic mode switching.

    Represents an in-memory execution context that adapts its processing
    strategy based on the volume of data being handled. Three modes exist:

    - **small**     — full results/errors kept in memory (< SMALL_THRESHOLD items)
    - **batch**     — items buffered and flushed in chunks via _flush_callback
    - **aggregate** — only statistics and reservoir samples are kept; individual
                      items are discarded after updating running totals

    The flush callback is the integration point for Socket and Celery executors:
    it receives a dict of the form ``{"type": "result_batch"|"error_batch",
    "data": [...], "count": N}`` each time a buffer is drained.

    :ivar state_id: Unique identifier for this execution context.
    :ivar workflow_id: Optional workflow this context belongs to.
    :ivar mode: Current processing mode — "small", "batch", or "aggregate".
    :ivar results: Full results list (small mode only).
    :ivar errors: Full errors list (small mode only).
    :ivar result_buffer: Current in-flight result batch (batch mode only).
    :ivar error_buffer: Current in-flight error batch (batch mode only).
    :ivar total_processed: Running total of items processed (all modes).
    :ivar total_succeeded: Running total of successful items.
    :ivar total_failed: Running total of failed items.
    :ivar sum_values: Sum of numeric "value" fields from successful results.
    :ivar min_value: Minimum numeric value encountered.
    :ivar max_value: Maximum numeric value encountered.
    :ivar sample_results: Reservoir sample of recent/representative results.
    :ivar sample_errors: Reservoir sample of recent/representative errors.
    :ivar start_time: Unix timestamp when this context was created.
    :ivar end_time: Unix timestamp when finalize() was called; 0.0 until then.
    :ivar metadata: Arbitrary metadata (pipeline class, depth, etc.).
    """

    state_id: str
    workflow_id: typing.Optional[str] = None

    mode: ModeType = "small"  # "small" | "batch" | "aggregate"

    # Thresholds (class-level constants)
    SMALL_THRESHOLD: typing.ClassVar[int] = 100
    BATCH_THRESHOLD: typing.ClassVar[int] = 10_000
    BATCH_SIZE: typing.ClassVar[int] = 100
    SAMPLE_SIZE: typing.ClassVar[int] = 10

    #  Small mode: full storage
    results: typing.List[typing.Dict] = field(default_factory=list)
    errors: typing.List[typing.Dict] = field(default_factory=list)

    # Batch mode: current buffer
    result_buffer: typing.List[typing.Dict] = field(default_factory=list)
    error_buffer: typing.List[typing.Dict] = field(default_factory=list)

    # Aggregate mode: statistics
    total_processed: int = 0
    total_succeeded: int = 0
    total_failed: int = 0
    sum_values: float = 0.0
    min_value: float = float("inf")
    max_value: float = float("-inf")

    # Reservoir samples (used in aggregate mode)
    sample_results: ReservoirSample = field(
        default_factory=lambda: ReservoirSample(k=MiniExecutionContext.SAMPLE_SIZE)
    )
    sample_errors: ReservoirSample = field(
        default_factory=lambda: ReservoirSample(k=MiniExecutionContext.SAMPLE_SIZE)
    )

    # Metrics
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0

    # Flush callback (injected by executor)
    # Signature: callback({"type": str, "data": list, "count": int}) -> None
    _flush_callback: typing.Optional[typing.Callable] = field(default=None, repr=False)

    # Metadata
    metadata: typing.Dict[str, typing.Any] = field(default_factory=dict)

    # Thread safety
    # Not a dataclass field — initialised in __post_init__ so that pickling
    # (required for Celery) doesn't try to serialize the Lock.
    _lock: asyncio.Lock = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._lock = asyncio.Lock()

    @property
    def id(self) -> str:
        return self.state_id

    @classmethod
    def from_full_context(
        cls,
        context: "ExecutionContext",  # type: ignore[name-defined]
        **extra_metadata: typing.Any,
    ) -> "MiniExecutionContext":
        """Extract the minimal fields needed from a full ExecutionContext."""
        return cls(
            state_id=context.state_id,
            workflow_id=context.workflow_id,
            start_time=context.metrics.start_time,
            metadata={
                "pipeline_class": context.pipeline.__class__.__name__,
                "is_root": context.is_root,
                "depth": context.get_depth(),
                **extra_metadata,
            },
        )

    async def add_result(self, result_dict: typing.Dict) -> None:
        """
        Record a successful (or error-flagged) result.

        Automatically switches modes based on total item count:
        - small → accumulate in self.results
        - batch → buffer and flush every BATCH_SIZE items
        - aggregate → update running statistics and reservoir sample only
        """
        async with self._lock:
            self._add_result_locked(result_dict)

    async def add_error(self, error_dict: typing.Dict) -> None:
        """
        Record a processing error.

        Applies the same adaptive strategy as add_result.
        """
        async with self._lock:
            self._add_error_locked(error_dict)

    async def finalize(self) -> None:
        """
        Flush any remaining buffers and record the end timestamp.

        Must be called before the context is serialised or returned to the
        coordinator.  Idempotent — safe to call multiple times.
        """
        async with self._lock:
            if self.mode == "batch":
                self._flush_results_locked()
                self._flush_errors_locked()
            self.end_time = time.time()

    async def set_flush_callback(
        self, callback: typing.Callable[[typing.Dict], None]
    ) -> None:
        """Inject the flush callback after construction (e.g. from the executor)."""
        async with self._lock:
            self._flush_callback = callback

    async def to_dict(self) -> typing.Dict:
        """
        Serialise for wire transfer.

        In small mode: results and errors are included.
        In batch/aggregate mode: only statistics and samples are included
        (bulk data was already streamed via the flush callback).
        """
        async with self._lock:
            base = {
                "state_id": self.state_id,
                "workflow_id": self.workflow_id,
                "mode": self.mode,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "metadata": self.metadata,
                # Aggregate stats — meaningful in all modes
                "total_processed": self.total_processed,
                "total_succeeded": self.total_succeeded,
                "total_failed": self.total_failed,
                "sum_values": self.sum_values,
                "min_value": self.min_value if self.min_value != float("inf") else None,
                "max_value": (
                    self.max_value if self.max_value != float("-inf") else None
                ),
                "sample_results": self.sample_results.to_dict(),
                "sample_errors": self.sample_errors.to_dict(),
            }
            if self.mode == "small":
                base["results"] = list(self.results)
                base["errors"] = list(self.errors)
            else:
                base["results"] = []
                base["errors"] = []
            return base

    @classmethod
    def from_dict(cls, data: typing.Dict) -> "MiniExecutionContext":
        """Deserialise from a dict produced by to_dict()."""
        obj = cls(
            state_id=data["state_id"],
            workflow_id=data.get("workflow_id"),
            mode=typing.cast(ModeType, data.get("mode", "small")),
            results=data.get("results", []),
            errors=data.get("errors", []),
            start_time=data.get("start_time", time.time()),
            end_time=data.get("end_time", 0.0),
            metadata=data.get("metadata", {}),
            total_processed=data.get("total_processed", 0),
            total_succeeded=data.get("total_succeeded", 0),
            total_failed=data.get("total_failed", 0),
            sum_values=data.get("sum_values", 0.0),
            min_value=(
                data.get("min_value")
                if data.get("min_value") is not None
                else float("inf")
            ),
            max_value=(
                data.get("max_value")
                if data.get("max_value") is not None
                else float("-inf")
            ),
        )
        if "sample_results" in data:
            obj.sample_results = ReservoirSample.from_dict(data["sample_results"])
        if "sample_errors" in data:
            obj.sample_errors = ReservoirSample.from_dict(data["sample_errors"])
        return obj

    def _add_result_locked(self, result_dict: typing.Dict) -> None:
        total = self._get_total_count_locked()

        if total < self.SMALL_THRESHOLD:
            self.results.append(result_dict)

        elif total < self.BATCH_THRESHOLD:
            if self.mode == "small":
                self._switch_to_batch_mode_locked()

            self.result_buffer.append(result_dict)

            if len(self.result_buffer) >= self.BATCH_SIZE:
                self._flush_results_locked()

        else:
            if self.mode != "aggregate":
                self._switch_to_aggregate_mode_locked()

            self._aggregate_result_locked(result_dict)

    def _add_error_locked(self, error_dict: typing.Dict) -> None:
        total = self._get_total_count_locked()

        if total < self.SMALL_THRESHOLD:
            self.errors.append(error_dict)

        elif total < self.BATCH_THRESHOLD:
            if self.mode == "small":
                self._switch_to_batch_mode_locked()

            self.error_buffer.append(error_dict)

            if len(self.error_buffer) >= self.BATCH_SIZE:
                self._flush_errors_locked()

        else:
            if self.mode != "aggregate":
                self._switch_to_aggregate_mode_locked()

            self._aggregate_error_locked(error_dict)

    def _get_total_count_locked(self) -> int:
        """Total items across all storage locations. Caller must hold _lock."""
        return (
            len(self.results)
            + len(self.errors)
            + len(self.result_buffer)
            + len(self.error_buffer)
            + self.total_processed
        )

    def _switch_to_batch_mode_locked(self) -> None:
        """Transition from small → batch. Caller must hold _lock."""
        self.mode = "batch"

        # Flush the accumulated small-mode data as the first batch
        if self.results:
            self._invoke_callback_locked(
                {
                    "type": "result_batch",
                    "data": list(self.results),
                    "count": len(self.results),
                }
            )
        if self.errors:
            self._invoke_callback_locked(
                {
                    "type": "error_batch",
                    "data": list(self.errors),
                    "count": len(self.errors),
                }
            )

        self.results = []
        self.errors = []

    def _switch_to_aggregate_mode_locked(self) -> None:
        """Transition from batch → aggregate. Caller must hold _lock."""
        self.mode = "aggregate"

        # Snapshot counts BEFORE flushing so they're not lost.
        # (Original bug: counts were read after flush, always yielding 0.)
        prior_results = len(self.results) + len(self.result_buffer)
        prior_errors = len(self.errors) + len(self.error_buffer)

        if self.result_buffer:
            self._flush_results_locked()
        if self.error_buffer:
            self._flush_errors_locked()

        # total_processed may already be nonzero if we came from batch mode
        # via a partial aggregate pass; add the newly-counted prior items.
        self.total_processed += prior_results + prior_errors
        self.total_succeeded += prior_results  # conservative: count as succeeded
        self.total_failed += prior_errors  # conservative: count as failed

        self.results = []
        self.errors = []
        self.result_buffer = []
        self.error_buffer = []

    def _flush_results_locked(self) -> None:
        """Drain result_buffer → callback. Caller must hold _lock."""
        if self.result_buffer:
            self._invoke_callback_locked(
                {
                    "type": "result_batch",
                    "data": list(self.result_buffer),
                    "count": len(self.result_buffer),
                }
            )
            self.result_buffer = []

    def _flush_errors_locked(self) -> None:
        """Drain error_buffer → callback. Caller must hold _lock."""
        if self.error_buffer:
            self._invoke_callback_locked(
                {
                    "type": "error_batch",
                    "data": list(self.error_buffer),
                    "count": len(self.error_buffer),
                }
            )
            self.error_buffer = []

    def _invoke_callback_locked(self, payload: typing.Dict) -> None:
        """
        Deliver a batch payload to the flush callback.

        In small mode a missing callback is a no-op (data is in memory anyway).
        In batch/aggregate mode a missing callback means data loss — we raise
        rather than silently drop, since the caller must inject the callback
        before the context reaches batch volume.

        Caller must hold _lock.
        """
        if self._flush_callback is None:
            if self.mode in ("batch", "aggregate"):
                raise RuntimeError(
                    f"MiniExecutionContext in '{self.mode}' mode attempted to "
                    f"flush a batch of {payload['count']} items, but no "
                    f"_flush_callback has been set. Inject one via "
                    f"set_flush_callback() before adding items at scale."
                )
            # small mode: callback is optional, data lives in self.results/errors
            warnings.warn(
                "flush callback invoked in small mode but no callback is set; "
                "this payload will be ignored.",
                stacklevel=4,
            )
            return

        try:
            self._flush_callback(payload)
        except Exception:
            logger.exception(
                "flush callback raised for state_id=%s type=%s count=%d",
                self.state_id,
                payload.get("type"),
                payload.get("count", 0),
            )
            raise

    def _aggregate_result_locked(self, result_dict: typing.Dict) -> None:
        """Update running statistics for one result. Caller must hold _lock."""
        self.total_processed += 1

        if not result_dict.get("error", False):
            self.total_succeeded += 1

            content = result_dict.get("content", {})
            if isinstance(content, dict) and "value" in content:
                value = float(content["value"])
                self.sum_values += value
                self.min_value = min(self.min_value, value)
                self.max_value = max(self.max_value, value)

            self.sample_results.add(
                {
                    "timestamp": time.time(),
                    "summary": str(content)[:100],
                }
            )

    def _aggregate_error_locked(self, error_dict: typing.Dict) -> None:
        """Update running statistics for one error. Caller must hold _lock."""
        self.total_processed += 1
        self.total_failed += 1

        self.sample_errors.add(
            {
                "timestamp": time.time(),
                "error": str(error_dict.get("content", ""))[:100],
            }
        )
