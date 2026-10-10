from typing import Union
from datetime import datetime, timezone


def post_format_timestamps(self, value: float) -> datetime:
    return datetime.fromtimestamp(value, timezone.utc)


def pre_format_timestamps(self, value: Union[datetime, float]) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    return value
