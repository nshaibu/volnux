import logging
from datetime import datetime, timezone
from typing import Callable, Optional, Union

from .base import EventFilterBase
from ..event import Event

logger = logging.getLogger(__name__)

# A callable that extracts a comparable timestamp value from an event.
# It receives the full Event and must return a datetime or a numeric value
# (e.g. Unix timestamp). The WindowedTrigger injects window_epoch so that
# range bounds can be expressed as seconds-relative offsets.
TimestampExtractor = Callable[[Event], Union[datetime, float, int]]


def _default_extractor(event: Event) -> datetime:
    """Extract event_time from event.data, falling back to ingestion timestamp."""
    raw = event.data.get("event_time")
    if raw is None:
        return event.timestamp
    if isinstance(raw, datetime):
        return raw
    # ISO string
    return datetime.fromisoformat(str(raw))


def _to_microsecond_epoch(dt_or_num: Union[datetime, float, int]) -> int:
    """Converts a datetime or numeric timestamp to integer microseconds since Unix epoch."""
    if isinstance(dt_or_num, datetime):
        dt = dt_or_num
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000)
    # Numeric values (int or float seconds)
    return int(float(dt_or_num) * 1_000_000)


class TimestampFilter(EventFilterBase):
    """
    Filter events by a timestamp field falling within ``[start, end)``.

    Bounds are expressed as **seconds-relative offsets from** ``window_epoch``.
    ``WindowedTrigger`` sets ``window_epoch`` when it arms its aggregators, so
    that each window has a consistent reference point.

    Parameters
    ----------
    start:
        Inclusive lower bound in seconds from ``window_epoch``.
        ``None`` means no lower bound.
    end:
        Exclusive upper bound in seconds from ``window_epoch``.
        ``None`` means no upper bound.
    extractor:
        Callable that pulls the relevant timestamp from an event.
        Defaults to ``event.data["event_time"]`` with fallback to
        ``event.timestamp`` (ingestion time).

    Example
    -------
    ::

        # Accept events with event_time in the first 60 seconds of the window.
        TimestampFilter(start=0, end=60)

        # Accept events with event_time from 60 s onwards.
        TimestampFilter(start=60, end=None)

        # Custom extractor pulling from a nested field.
        TimestampFilter(
            start=0,
            end=30,
            extractor=lambda e: e.data["header"]["ts"],
        )
    """

    def __init__(
        self,
        start: Optional[float] = None,
        end: Optional[float] = None,
        extractor: Optional[TimestampExtractor] = None,
    ):
        self.start = start  # in seconds
        self.end = end  # in seconds
        self.extractor: TimestampExtractor = extractor or _default_extractor

        # Set by WindowedTrigger at arm time.
        self.window_epoch: Optional[datetime] = None
        self._epoch_micros: Optional[int] = None

        # Pre-calculated bounds in relative microseconds for zero-allocation matching
        self._start_micros: Optional[int] = (
            int(start * 1_000_000) if start is not None else None
        )
        self._end_micros: Optional[int] = (
            int(end * 1_000_000) if end is not None else None
        )

    def set_epoch(self, epoch: datetime) -> None:
        """Called by WindowedTrigger when the window opens."""
        if epoch.tzinfo is None:
            epoch = epoch.replace(tzinfo=timezone.utc)
        self.window_epoch = epoch
        self._epoch_micros = _to_microsecond_epoch(epoch)

    def matches(self, event: Event) -> bool:
        if self.window_epoch is None:
            # No epoch set — pass all events (safe default before arm).
            return True

        try:
            raw = self.extractor(event)
            event_micros = _to_microsecond_epoch(raw)
        except Exception:
            logger.exception(
                "TimestampFilter: extractor raised for event %s; excluding event.",
                event.event_id,
            )
            return False

        # # Normalise to offset in seconds from epoch.
        # if isinstance(raw, datetime):
        #     if raw.tzinfo is None:
        #         raw = raw.replace(tzinfo=timezone.utc)
        #     epoch = self.window_epoch
        #     if epoch.tzinfo is None:
        #         epoch = epoch.replace(tzinfo=timezone.utc)
        #     offset = (raw - epoch).total_seconds()
        # else:
        #     # Treat numeric values as absolute Unix timestamps.
        #     offset = float(raw) - self.window_epoch.timestamp()
        #
        # if self.start is not None and offset < self.start:
        #     return False
        # if self.end is not None and offset >= self.end:
        #     return False

        # Calculate offset using drift-free integer microsecond subtraction
        offset_micros = event_micros - self._epoch_micros

        # Half-open interval matching: [start, end)
        if self._start_micros is not None and offset_micros < self._start_micros:
            return False
        if self._end_micros is not None and offset_micros >= self._end_micros:
            return False

        return True
