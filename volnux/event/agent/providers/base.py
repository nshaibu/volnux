"""Provider adapters: one interface over the LLM APIs.

An adapter turns the agent's neutral conversation into one provider's wire format and
the provider's answer back into one neutral response.

Conversation (what the agent keeps and hands over)
--------------------------------------------------
::

    {"role": "system" | "user" | "assistant" | "tool", "content": str}
    assistant that asked for a tool:
        {"role": "assistant", "content": str,
         "tool_calls": [{"id": str, "name": str, "arguments": dict}]}
    the tool's result:
        {"role": "tool", "content": str, "tool_call_id": str, "name": str}

Tools are neutral function schemas: ``{"name", "description", "parameters"}`` with
``parameters`` a JSON schema object.

Response (what ``complete`` returns)
------------------------------------
::

    {"content": str, "action": AgentAction, "tool_name": str | None,
     "tool_args": dict | None, "tool_call_id": str | None,
     "token_usage": {"prompt": int, "completion": int, "total": int},
     "finish_reason": str, "raw": Any}

``action`` is ``TOOL_CALL`` when the provider reported a native function call and
``THINK`` otherwise; the agent reads the protocol directive from ``content``.
"""

import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from .._state import AgentAction
from ..protocol import ProtocolViolation, parse_directive

logger = logging.getLogger(__name__)

__all__ = ["LLMProvider", "LLMProviderAdapterBase", "ProtocolViolation"]


class LLMProvider(Enum):
    """Supported LLM provider identifiers."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GEMINI = "gemini"
    MISTRAL = "mistral"
    OLLAMA = "ollama"
    VLLM = "vllm"
    CUSTOM = "custom"


class LLMProviderAdapterBase:
    """
    Abstract interface for LLM provider adapters.

    The framework ships adapters for OpenAI, Anthropic, Gemini, Mistral, Ollama and
    vLLM. A concrete adapter sets ``provider_name`` and is registered under that name
    when its class is created; an intermediate base without one is not registered.

    All methods are async. ``options`` (``api_key``, ``base_url``, ``timeout`` ...) are
    given by the caller, e.g. from an agent's ``llm_options``.
    """

    provider_name: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        name = cls.__dict__.get("provider_name")
        if name:
            from .registry import LLMProviderRegistry

            LLMProviderRegistry.register(name, cls)

    def __init__(self, **options: Any) -> None:
        self.options: Dict[str, Any] = options

    @staticmethod
    def parse_react_tool(text: str) -> Dict[str, Any]:
        """Parse a ``TOOL: name | {json}`` line (kept for adapters and callers that
        used it before the protocol module)."""
        directive = parse_directive(text)
        if directive.tool_name is None:
            raise ProtocolViolation("Malformed TOOL format")
        return {
            "action": "TOOL_CALL",
            "tool_name": directive.tool_name,
            "namespace": directive.namespace,
            "tool_args": directive.tool_args,
        }

    @staticmethod
    def build_response(
        content: str = "",
        *,
        tool_name: Optional[str] = None,
        tool_args: Optional[Dict[str, Any]] = None,
        tool_call_id: Optional[str] = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        finish_reason: str = "",
        raw: Any = None,
    ) -> Dict[str, Any]:
        """The neutral response every adapter returns."""
        return {
            "content": content or "",
            "action": AgentAction.TOOL_CALL if tool_name else AgentAction.THINK,
            "tool_name": tool_name,
            "tool_args": tool_args if tool_name else None,
            "tool_call_id": tool_call_id,
            "token_usage": {
                "prompt": int(prompt_tokens or 0),
                "completion": int(completion_tokens or 0),
                "total": int(prompt_tokens or 0) + int(completion_tokens or 0),
            },
            "finish_reason": finish_reason or "",
            "raw": raw,
        }

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Call the LLM; see the module docstring for the response shape."""
        raise NotImplementedError

    async def validate_response(
        self,
        response: Dict[str, Any],
        context: Dict[str, Any],
    ) -> bool:
        """
        Validate the response for hallucinations or constraint violations.
        Default returns True. Override for provider-level validation.
        """
        return True
