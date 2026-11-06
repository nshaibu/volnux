from datetime import datetime, timezone
from typing import Any, Dict, Optional

from formax import MiniAnnotated, Attrib

from volnux.backends.fields import ForeignKeyField, FKConfig, OnDelete
from volnux.models.enums import ExecutionState, TriggerType

from .base import GovernanceModel
from .users import User, Organization, Team
from .workflows import Workflow, WorkflowVersion


class Execution(GovernanceModel):
    """A single workflow execution instance.

    Deleting a workflow (or one of its versions) deletes its executions, traces and
    HITL requests with it: an execution is history *of* a version. The API only ever
    deletes drafts, which have none.

    Reverse Relations:
        traces              — Execution traces
        hitl_requests — HITL requests
        break_glass_accesses — Break-glass access records
        child_executions — Sub-executions
    """

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="executions", on_delete=OnDelete.CASCADE),
    ]
    workflow_version: ForeignKeyField[
        WorkflowVersion,
        FKConfig(reverse_name="executions", on_delete=OnDelete.CASCADE),
    ]
    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="executions", on_delete=OnDelete.CASCADE),
    ]
    team: ForeignKeyField[
        Team,
        FKConfig(reverse_name="executions", on_delete=OnDelete.PROTECT),
    ]
    status: MiniAnnotated[ExecutionState, Attrib(default=ExecutionState.RUNNING)]
    triggered_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="triggered_executions",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    trigger_type: TriggerType
    trigger_params: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    execution_params: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    started_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    ended_at: Optional[float] = None
    checkpoint_ref: MiniAnnotated[Optional[str], Attrib(default=None)]
    parent_execution: ForeignKeyField[
        "volnux.models.Execution",
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="child_executions",
            on_delete=OnDelete.SET_NULL,
        ),
    ]


class ExecutionTrace(GovernanceModel):
    """A single step in the fractal execution tree."""

    execution: ForeignKeyField[
        Execution,
        FKConfig(reverse_name="traces", on_delete=OnDelete.CASCADE),
    ]
    event_name: str
    event_version: str
    node: ForeignKeyField[
        "volnux.models.MeshNode",
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="execution_traces",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    parent_trace: ForeignKeyField[
        "volnux.models.ExecutionTrace",
        FKConfig(
            nullable=True,
            default=None,
            reverse_name="child_traces",
            on_delete=OnDelete.SET_NULL,
        ),
    ]
    step_order: MiniAnnotated[int, Attrib(default=0)]
    status: MiniAnnotated[ExecutionState, Attrib(default=ExecutionState.RUNNING)]
    input_data: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    output_data: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    error_data: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    descriptor: MiniAnnotated[Optional[int], Attrib(default=None)]
    retry_count: MiniAnnotated[int, Attrib(default=0, ge=0)]
    max_retries: MiniAnnotated[int, Attrib(default=0, ge=0)]
    started_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    ended_at: Optional[float] = None
    checkpoint_ref: Optional[str] = None
    otel_trace_id: Optional[str] = None
