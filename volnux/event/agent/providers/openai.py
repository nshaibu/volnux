import json
from typing import Any, Dict, List, Optional

from volnux.exceptions import LLMProviderError
from .http import HTTPProviderAdapter, json_arguments

__all__ = [
    "OpenAICompatibleAdapter",
    "OpenAIProviderAdapter",
    "MistralProviderAdapter",
    "VLLMProviderAdapter",
]


class OpenAICompatibleAdapter(HTTPProviderAdapter):
    """Any server that speaks OpenAI's ``/chat/completions``: OpenAI, Mistral, vLLM,
    LM Studio, llama.cpp's server, gateways. Function calling is native: calls come
    back in ``choices[0].message.tool_calls``.

    Extra ``llm_options`` for the request itself are passed through as ``params``
    (``top_p``, ``seed``, ...).
    """

    chat_path = "/chat/completions"
    #: OpenAI's reasoning models reject ``max_tokens``; others know only that name.
    max_tokens_param = "max_tokens"

    def _headers(self) -> Dict[str, str]:
        headers = super()._headers()
        key = self._api_key()
        if key:
            headers["Authorization"] = f"Bearer {key}"
        return headers

    @staticmethod
    def to_wire(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        wire: List[Dict[str, Any]] = []
        for m in messages:
            role = m["role"]
            if role == "assistant" and m.get("tool_calls"):
                wire.append(
                    {
                        "role": "assistant",
                        "content": m.get("content") or None,
                        "tool_calls": [
                            {
                                "id": call["id"],
                                "type": "function",
                                "function": {
                                    "name": call["name"],
                                    "arguments": json.dumps(
                                        call.get("arguments") or {}
                                    ),
                                },
                            }
                            for call in m["tool_calls"]
                        ],
                    }
                )
            elif role == "tool":
                wire.append(
                    {
                        "role": "tool",
                        "tool_call_id": m.get("tool_call_id", ""),
                        "content": str(m.get("content", "")),
                    }
                )
            else:
                wire.append({"role": role, "content": m.get("content", "")})
        return wire

    @staticmethod
    def to_wire_tools(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [{"type": "function", "function": t} for t in tools]

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "model": model,
            "messages": self.to_wire(messages),
            "temperature": temperature,
            self.max_tokens_param: max_tokens,
        }
        if tools:
            payload["tools"] = self.to_wire_tools(tools)
            payload["tool_choice"] = "auto"
        payload.update(self.options.get("params") or {})
        payload.update(kwargs.get("params") or {})

        data = await self._post_json(self.chat_path, payload)
        return self.parse(data)

    def parse(self, data: Dict[str, Any]) -> Dict[str, Any]:
        choices = data.get("choices") or []
        if not choices:
            raise LLMProviderError(
                f"{self.provider_name} returned no choices: {str(data)[:300]}"
            )
        choice = choices[0]
        message = choice.get("message") or {}
        usage = data.get("usage") or {}

        tool_name = tool_args = tool_call_id = None
        calls = message.get("tool_calls") or []
        if calls:  # the agent runs one call per step
            call = calls[0]
            function = call.get("function") or {}
            tool_name = function.get("name")
            tool_call_id = call.get("id")
            tool_args = json_arguments(function.get("arguments"))

        return self.build_response(
            message.get("content") or "",
            tool_name=tool_name,
            tool_args=tool_args,
            tool_call_id=tool_call_id,
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            finish_reason=choice.get("finish_reason") or "",
            raw=data,
        )


class OpenAIProviderAdapter(OpenAICompatibleAdapter):
    """OpenAI (GPT-4o, GPT-4.1, o-series). Key from ``OPENAI_API_KEY``; the endpoint
    from ``OPENAI_BASE_URL`` (Azure, a proxy) or ``https://api.openai.com/v1``."""

    provider_name = "openai"
    default_base_url = "https://api.openai.com/v1"
    base_url_env = "OPENAI_BASE_URL"
    api_key_env = "OPENAI_API_KEY"
    max_tokens_param = "max_completion_tokens"


class MistralProviderAdapter(OpenAICompatibleAdapter):
    """Mistral's hosted API. Key from ``MISTRAL_API_KEY``."""

    provider_name = "mistral"
    default_base_url = "https://api.mistral.ai/v1"
    base_url_env = "MISTRAL_BASE_URL"
    api_key_env = "MISTRAL_API_KEY"


class VLLMProviderAdapter(OpenAICompatibleAdapter):
    """A vLLM (or any OpenAI-compatible) server you run. ``VLLM_BASE_URL`` (default
    ``http://localhost:8000/v1``); ``VLLM_API_KEY`` only if the server wants one."""

    provider_name = "vllm"
    default_base_url = "http://localhost:8000/v1"
    base_url_env = "VLLM_BASE_URL"
    api_key_env = "VLLM_API_KEY"
    api_key_required = False
