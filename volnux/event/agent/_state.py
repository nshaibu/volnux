from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


__all__ = [
    "AgentAction",
    "ReasoningStep",
    "AgentResult",
    "ToolCallRecord",
    "ReasoningStepType",
]


class AgentAction(Enum):
    """Terminal and non-terminal actions an agent may take per step."""

    THINK = "think"  # Continue reasoning — non-terminal
    TOOL_CALL = "tool_call"  # Execute a registered tool — non-terminal
    FINISH = "finish"  # Return result to workflow — terminal
    HANDOFF = "handoff"  # Delegate to another agent — terminal
    PAUSE = "pause"  # Yield control, await RESUME — non-terminal
    ERROR = "error"  # Non-recoverable failure — terminal


class ReasoningStepType(Enum):
    """Classifies each entry in the agent's reasoning trace."""

    SYSTEM_PROMPT = "system_prompt"
    USER_MESSAGE = "user_message"
    ASSISTANT_THINK = "assistant_think"
    TOOL_REQUEST = "tool_request"
    TOOL_RESULT = "tool_result"
    FINAL_ANSWER = "final_answer"
    HANDOFF_REQUEST = "handoff_request"
    ERROR_STATE = "error_state"


@dataclass
class ReasoningStep:
    """
    A single step in the agent's reasoning trace.

    Every step is checkpointed via ``enqueue_checkpoint()`` so the full
    reasoning history survives crashes. The complete sequence constitutes
    the agent's audit trail — what it thought, what it called, what
    the results were, and why it reached its conclusion.
    """

    step_index: int
    step_type: ReasoningStepType
    content: Any
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    token_usage: Optional[Dict[str, int]] = None
    latency_ms: Optional[float] = None
    tool_name: Optional[str] = None
    tool_args: Optional[Dict[str, Any]] = None
    tool_result: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """JSON-compatible form (the trace is checkpointed in this shape)."""
        data = asdict(self)
        data["step_type"] = self.step_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReasoningStep":
        data = dict(data)
        data["step_type"] = ReasoningStepType(data["step_type"])
        return cls(**data)


@dataclass
class AgentResult:
    """
    Terminal result returned by ``process()``.

    Carried through ``TriggerActivation.workflow_params`` to downstream
    events. Includes the full reasoning trace when
    ``include_trace_in_result=True`` for complete downstream audit context.
    """

    success: bool
    content: Any
    reasoning_trace: List[ReasoningStep]
    total_tokens: int
    total_latency_ms: float
    steps_taken: int
    handoff_target: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """JSON-compatible form, for results that are persisted or sent on."""
        return {
            "success": self.success,
            "content": self.content,
            "reasoning_trace": [step.to_dict() for step in self.reasoning_trace],
            "total_tokens": self.total_tokens,
            "total_latency_ms": self.total_latency_ms,
            "steps_taken": self.steps_taken,
            "handoff_target": self.handoff_target,
            "metadata": dict(self.metadata),
        }


@dataclass
class ToolCallRecord:
    """
    Immutable record of a single tool invocation.

    Persisted *before* the tool executes so the intent is recorded even
    if the call crashes. ``result`` and ``completed_at`` are filled on
    completion and form part of the checkpoint.
    """

    tool_class_name: str
    tool_args: Dict[str, Any]
    called_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    result: Optional[Any] = None
    error: Optional[str] = None
    completed_at: Optional[str] = None
    latency_ms: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ToolCallRecord":
        return cls(**data)
