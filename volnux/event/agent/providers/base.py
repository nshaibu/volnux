import re
import json
import logging
from enum import Enum
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


class ProtocolViolation(Exception):
    pass


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

    The framework ships adapters for OpenAI, Anthropic, Gemini, Mistral,
    Ollama, and vLLM.

    All methods are async. Sync providers are wrapped in ``asyncio.to_thread``.
    """

    provider_name: str

    @classmethod
    def __init_subclass__(cls, **kwargs):
        from .registry import LLMProviderRegistry

        super().__init_subclass__(**kwargs)
        LLMProviderRegistry.register(cls.provider_name, cls)

    @staticmethod
    def parse_react_tool(text: str):
        tool_re = re.compile(
            r"^TOOL:\s*(?:(?P<ns>[^:]+):)?(?P<tool>\w+)\s*\|\s*(?P<args>\{.*\})$",
            re.DOTALL,
        )
        m = tool_re.match(text.strip())
        if not m:
            raise ProtocolViolation("Malformed TOOL format")

        return {
            "action": "TOOL_CALL",
            "tool_name": m.group("tool"),
            "namespace": m.group("ns"),
            "tool_args": json.loads(m.group("args") or "{}"),
        }

    async def complete(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Call the LLM and return a standardised response dict::

            {
                "content":       str,
                "action":        AgentAction | None,
                "tool_name":     str | None,
                "tool_args":     dict | None,
                "tool_call_id":  str | None,
                "token_usage":   {"prompt": int, "completion": int, "total": int},
                "finish_reason": str,
                "raw":           dict,
            }
        """
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
