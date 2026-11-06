from datetime import datetime, timezone, timedelta
from typing import Optional


class FreshnessPolicy:
    """Declares how fresh an asset must be before it is considered stale.

    Args:
        maximum_lag: The maximum age of the asset before staleness.
        cron: An optional cron expression for when the asset is expected
              to be refreshed. If set, staleness is only evaluated after
              the cron window passes.
    """

    def __init__(
        self,
        maximum_lag: Optional[timedelta] = None,
        maximum_lag_minutes: Optional[int] = None,
        cron: Optional[str] = None,
    ):
        if maximum_lag is not None:
            self.maximum_lag = maximum_lag
        elif maximum_lag_minutes is not None:
            self.maximum_lag = timedelta(minutes=maximum_lag_minutes)
        else:
            self.maximum_lag = timedelta(hours=1)  # Default: 1 hour

        self.cron = cron

    def is_exceeded(self, materialised_at: datetime) -> bool:
        """Check if the freshness policy has been exceeded."""
        if materialised_at.tzinfo is None:
            materialised_at = materialised_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - materialised_at) > self.maximum_lag
