"""
WindowedTrigger
│
├── _open_window()
│   ├── stamp window_epoch on TimestampFilters          ← event-time windowing
│   ├── arm all aggregators concurrently                ← parallel ingestion
│   │    └── each aggregator callback writes to window_state[name]
│   ├── arm WindowSink                                  ← condition gate
│   │    └── condition_fn receives live window_state
│   └── start window_timeout task (optional)
│
├── [events arrive] → aggregator._handle_event()
│   └── _make_aggregator_callback() → window_state[slot].append(params)
│
├── [sink condition met] → _on_sink_fired()
│   └── schedules _handle_sink_fired() as new task     ← deadlock-safe
│       └── _close_window(timed_out=False)
│           ├── disarm all aggregators + sink
│           ├── activate(window_state=snapshot, ...)   ← fires workflow
│           └── _open_window()                         ← re-arm next window
│
└── [timeout] → _run_timeout()
    └── _close_window(timed_out=True)                  ← silent reset
        └── _open_window()
"""

from .event import EventTrigger
from .condition import ConditionalTrigger
from .manual import ManualTrigger
from .schedule import SchedulerTrigger
from .webhook import WebhookTrigger
from .chain import (
    WorkflowChainTrigger,
    LinearChainTrigger,
    WorkflowStatus,
    WindowedTrigger,
    WindowSink,
)
from .base import TriggerBase, TriggerLifecycle, TriggerActivation, TriggerType

__all__ = [
    "EventTrigger",
    "WorkflowChainTrigger",
    "ManualTrigger",
    "ConditionalTrigger",
    "SchedulerTrigger",
    "WebhookTrigger",
    "TriggerBase",
    "TriggerLifecycle",
    "TriggerActivation",
    "TriggerType",
    "LinearChainTrigger",
    "WorkflowStatus",
    "WindowedTrigger",
    "WindowSink",
]
