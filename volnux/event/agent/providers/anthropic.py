from typing import Any, Dict

from .base import LLMProviderAdapterBase
from .._state import AgentAction
from volnux.exceptions import LLMProviderError


class AnthropicProviderAdapter(LLMProviderAdapterBase):
    """
    Anthropic provider adapter supporting Claude 3 and Claude 4 series.
    Tool use is handled via Anthropic's native tool_use content blocks.
    """

    provider_name = "anthropic"

    async def complete(
        self, messages, model, temperature, max_tokens, tools=None, **kwargs
    ) -> Dict[str, Any]:
        try:
            import anthropic
        except ImportError:
            raise LLMProviderError(
                "anthropic package not installed. " "Run: pip install anthropic"
            )
        client = anthropic.AsyncAnthropic()

        # Anthropic separates system prompt from messages
        system = next((m["content"] for m in messages if m["role"] == "system"), "")
        msgs = [m for m in messages if m["role"] != "system"]

        params = dict(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system,
            messages=msgs,
        )
        if tools:
            params["tools"] = tools

        response = await client.messages.create(**params)

        action = AgentAction.THINK
        tool_name = None
        tool_args = None
        content_text = ""

        for block in response.content:
            if block.type == "text":
                content_text = block.text
            elif block.type == "tool_use":
                action = AgentAction.TOOL_CALL
                tool_name = block.name
                tool_args = block.input

        usage = response.usage
        return {
            "content": content_text,
            "action": action,
            "tool_name": tool_name,
            "tool_args": tool_args,
            "tool_call_id": None,
            "token_usage": {
                "prompt": usage.input_tokens,
                "completion": usage.output_tokens,
                "total": usage.input_tokens + usage.output_tokens,
            },
            "finish_reason": response.stop_reason,
            "raw": response.model_dump(),
        }
