from typing import Any, List, Type
from .base import AgentEventBase


class ChainOfThoughtAgentEventBase(AgentEventBase):
    """
    ``AgentEventBase`` for reasoning-only agents with no tool access.

    Suitable for analysis, classification, and decision-making where
    the agent reasons over input data and produces a conclusion without
    calling external tools. ``tools`` must remain empty.
    """

    tools: List[Type[Any]] = []

    def _validate_agent_configuration(self) -> None:
        super()._validate_agent_configuration()
        if self.tools:
            raise TypeError(
                f"ChainOfThoughtAgentEventBase '{self.__class__.__name__}' "
                f"does not support tools. Use AgentEventBase for tool-using agents."
            )
