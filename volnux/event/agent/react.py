from typing import Any, Dict
from .base import AgentEventBase
from ._state import AgentAction


class ReActAgentEventBase(AgentEventBase):
    """
    ``AgentEventBase`` with enforced Thought → Action → Observation format.

    Augments the governance preamble with explicit ReAct instructions.
    Structured output makes the reasoning trace more interpretable for
    compliance review — every step clearly shows thought, action, observation.
    """

    def _build_governed_system_prompt(self) -> str:
        base = super()._build_governed_system_prompt()
        return (
            f"{base}\n\nReAct format — required for every response:\n"
            "  Thought: <your reasoning>\n"
            "  Action: TOOL: <name> | <json_args>  |  FINISH: <answer>  "
            "|  HANDOFF: <target> | <ctx>  |  ERROR: <description>\n"
            "\nBegin."
        )

    def parse_action(self, response: Dict[str, Any]) -> AgentAction:
        c = str(response.get("content", ""))
        if "FINISH:" in c:
            return AgentAction.FINISH
        if "TOOL:" in c:
            return AgentAction.TOOL_CALL
        if "HANDOFF:" in c:
            return AgentAction.HANDOFF
        if "ERROR:" in c:
            return AgentAction.ERROR
        return AgentAction.THINK
