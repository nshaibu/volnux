from .base import AgentEventBase


class ReActAgentEventBase(AgentEventBase):
    """
    ``AgentEventBase`` with enforced Thought → Action → Observation format.

    Augments the governance preamble with explicit ReAct instructions.
    Structured output makes the reasoning trace more interpretable for
    compliance review — every step clearly shows thought, action, observation.

    The response is ``Thought:`` lines followed by one ``Action:`` line holding the
    directive; text before the directive is allowed (``directive_preamble``), a second
    directive is still a violation.
    """

    directive_preamble = True

    def _build_governed_system_prompt(self) -> str:
        base = super()._build_governed_system_prompt()
        return (
            f"{base}\n\nReAct format — required for every response:\n"
            "  Thought: <your reasoning>\n"
            "  Action: TOOL: <name> | <json_args>  |  FINISH: <answer>  "
            "|  HANDOFF: <target> | <ctx>  |  ERROR: <description>\n"
            "\nBegin."
        )
