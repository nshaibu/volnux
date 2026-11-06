import logging
import math
import random
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union, cast

logger = logging.getLogger(__name__)


@dataclass
class NumericStreamSummary:
    """Descriptive statistics for a stream of numeric (int/float) chunks.

    Statistics are computed incrementally using Welford's online algorithm
    for mean and variance — O(1) memory regardless of stream length.

    Quantiles (p50, p95, p99) use Algorithm R reservoir sampling (size 1000).
    """

    count: int = 0
    valid_count: int = 0  # Chunks successfully converted to float
    error_count: int = 0
    minimum: float = math.inf
    maximum: float = -math.inf

    # Wilford's online mean/variance.
    _mean: float = field(default=0.0, repr=False)
    _m2: float = field(default=0.0, repr=False)  # sum of squared deviations

    # Fixed-size reservoir for quantile estimation (Algorithm R).
    _reservoir: list = field(default_factory=list, repr=False)
    _reservoir_max: int = field(default=1000, repr=False)

    # Finalised quantiles — populated by finalise().
    p50: Optional[float] = None
    p95: Optional[float] = None
    p99: Optional[float] = None

    def update(self, success: bool, value: Any) -> None:
        self.count += 1
        if not success:
            self.error_count += 1

        try:
            # Explicitly exclude booleans since bool is a subclass of int in Python
            if isinstance(value, bool):
                raise TypeError("Booleans are not treated as numeric metrics.")
            v = float(value)
        except (TypeError, ValueError):
            self.error_count += 1
            return

        self.valid_count += 1

        # Min / max.
        if v < self.minimum:
            self.minimum = v
        if v > self.maximum:
            self.maximum = v

        # Welford's online mean and variance (uses valid_count, NOT total count).
        delta = v - self._mean
        self._mean += delta / self.valid_count
        delta2 = v - self._mean
        self._m2 += delta * delta2

        # Algorithm R Reservoir Sampling for unbiased quantile estimation
        if len(self._reservoir) < self._reservoir_max:
            self._reservoir.append(v)
        else:
            # Replace random element with probability (reservoir_max / valid_count)
            idx = random.randint(0, self.valid_count - 1)
            if idx < self._reservoir_max:
                self._reservoir[idx] = v

    def finalise(self) -> "NumericStreamSummary":
        if not self._reservoir:
            return self

        sorted_r = sorted(self._reservoir)
        n = len(sorted_r)

        def _percentile(p: float) -> float:
            idx = (p / 100) * (n - 1)
            lo = int(idx)
            hi = min(lo + 1, n - 1)
            return sorted_r[lo] + (idx - lo) * (sorted_r[hi] - sorted_r[lo])

        self.p50 = _percentile(50)
        self.p95 = _percentile(95)
        self.p99 = _percentile(99)
        return self

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def variance(self) -> float:
        return self._m2 / self.valid_count if self.valid_count > 1 else 0.0

    @property
    def std_dev(self) -> float:
        return math.sqrt(self.variance)

    @property
    def error_rate(self) -> float:
        return self.error_count / self.count if self.count else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "numeric",
            "count": self.count,
            "valid_count": self.valid_count,
            "error_count": self.error_count,
            "error_rate": round(self.error_rate, 6),
            "min": self.minimum if math.isfinite(self.minimum) else None,
            "max": self.maximum if math.isfinite(self.maximum) else None,
            "mean": round(self.mean, 6),
            "std_dev": round(self.std_dev, 6),
            "variance": round(self.variance, 6),
            "p50": round(self.p50, 6) if self.p50 is not None else None,
            "p95": round(self.p95, 6) if self.p95 is not None else None,
            "p99": round(self.p99, 6) if self.p99 is not None else None,
        }


@dataclass
class CategoricalStreamSummary:
    """Summary for a stream of non-numeric or mixed chunks."""

    count: int = 0
    error_count: int = 0
    type_counts: Dict[str, int] = field(default_factory=dict)
    value_freq: Dict[Any, int] = field(default_factory=dict)
    freq_cap: int = 100
    freq_overflow: int = 0

    def update(self, success: bool, value: Any) -> None:
        self.count += 1
        if not success:
            self.error_count += 1

        type_name = type(value).__name__
        self.type_counts[type_name] = self.type_counts.get(type_name, 0) + 1

        try:
            hash(value)
            hashable = True
        except TypeError:
            hashable = False

        if hashable:
            if value in self.value_freq:
                self.value_freq[value] += 1
            elif len(self.value_freq) < self.freq_cap:
                self.value_freq[value] = 1
            else:
                self.freq_overflow += 1

    def finalise(self) -> "CategoricalStreamSummary":
        return self

    @property
    def error_rate(self) -> float:
        return self.error_count / self.count if self.count else 0.0

    @property
    def top_values(self) -> Dict[Any, int]:
        return dict(
            sorted(self.value_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "categorical",
            "count": self.count,
            "error_count": self.error_count,
            "error_rate": round(self.error_rate, 6),
            "type_counts": self.type_counts,
            "top_values": {str(k): v for k, v in self.top_values.items()},
            "freq_overflow": self.freq_overflow,
        }


class StreamSummary:
    """Unified incremental stream summary."""

    def __init__(self) -> None:
        self._impl: Optional[Union[NumericStreamSummary, CategoricalStreamSummary]] = (
            None
        )

    def update(self, success: bool, value: Any) -> None:
        if self._impl is None:
            self._impl = self._choose(value)
        elif isinstance(self._impl, NumericStreamSummary):
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                self._impl = self._promote_to_categorical()

        self._impl.update(success, value)

    def finalise(self) -> "StreamSummary":
        if self._impl is not None:
            self._impl.finalise()
        return self

    def to_dict(self) -> Dict[str, Any]:
        if self._impl is None:
            return {"type": "empty", "count": 0}
        return self._impl.to_dict()

    @property
    def count(self) -> int:
        return self._impl.count if self._impl else 0

    @property
    def error_rate(self) -> float:
        return self._impl.error_rate if self._impl else 0.0

    @staticmethod
    def _choose(
        value: Any,
    ) -> Union[NumericStreamSummary, CategoricalStreamSummary]:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return NumericStreamSummary()
        return CategoricalStreamSummary()

    def _promote_to_categorical(self) -> CategoricalStreamSummary:
        numeric = cast(NumericStreamSummary, self._impl)
        cat = CategoricalStreamSummary()
        # Transfer counts to categorical instance before calling update()
        cat.count = numeric.count
        cat.error_count = numeric.error_count
        cat.type_counts["numeric_promoted"] = numeric.valid_count
        logger.debug(
            "StreamSummary: promoted numeric→categorical at count=%d.", numeric.count
        )
        return cat
