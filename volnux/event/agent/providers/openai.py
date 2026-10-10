import json
from typing import Any, Dict

from .base import LLMProviderAdapterBase
from .._state import AgentAction
from volnux.exceptions import LLMProviderError


class OpenAIProviderAdapter(LLMProviderAdapterBase):
    """
    OpenAI provider adapter supporting GPT-4o, GPT-4-turbo, and o-series.
    Handles function calling natively — tool calls are parsed from the
    choices[0].message.tool_calls response field.
    """

    provider_name = "openai"

    async def complete(
        self, messages, model, temperature, max_tokens, tools=None, **kwargs
    ) -> Dict[str, Any]:
        try:
            import openai
        except ImportError:
            raise LLMProviderError(
                "openai package not installed. " "Run: pip install openai"
            )
        client = openai.AsyncOpenAI()
        params = dict(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        if tools:
            params["tools"] = [{"type": "function", "function": t} for t in tools]
            params["tool_choice"] = "auto"

        response = await client.chat.completions.create(**params)
        choice = response.choices[0]
        message = choice.message
        usage = response.usage

        # Detect function call
        action = AgentAction.THINK
        tool_name = None
        tool_args = None
        tool_call_id = None

        if message.tool_calls:
            tc = message.tool_calls[0]
            action = AgentAction.TOOL_CALL
            tool_name = tc.function.name
            tool_call_id = tc.id

            tool_args = json.loads(tc.function.arguments or "{}")

        return {
            "content": message.content or "",
            "action": action,
            "tool_name": tool_name,
            "tool_args": tool_args,
            "tool_call_id": tool_call_id,
            "token_usage": {
                "prompt": usage.prompt_tokens,
                "completion": usage.completion_tokens,
                "total": usage.total_tokens,
            },
            "finish_reason": choice.finish_reason,
            "raw": response.model_dump(),
        }
