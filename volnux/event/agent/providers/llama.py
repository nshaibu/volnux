from typing import Any, Dict, List, Optional

from volnux.exceptions import LLMProviderError
from .http import HTTPProviderAdapter, json_arguments

__all__ = ["OllamaProviderAdapter"]


class OllamaProviderAdapter(HTTPProviderAdapter):
    """Ollama (locally hosted models) through its native ``/api/chat``.

    The server is ``OLLAMA_BASE_URL`` (default ``http://localhost:11434``); no key.
    Models with tool support (llama3.1+, qwen2.5, mistral-nemo ...) get native
    function calling. For a model without it Ollama answers 400 "does not support
    tools"; the adapter then asks again as plain text, with tool results as user
    messages, and the agent's ``TOOL:`` directive carries the call.

    Non-streaming (``stream=false``): Ollama reports token usage only that way.
    """

    provider_name = "ollama"
    default_base_url = "http://localhost:11434"
    base_url_env = "OLLAMA_BASE_URL"
    api_key_required = False
    default_timeout = 1200.0

    @staticmethod
    def to_wire(
        messages: List[Dict[str, Any]], *, native_tools: bool = True
    ) -> List[Dict[str, Any]]:
        wire: List[Dict[str, Any]] = []
        for m in messages:
            role = m["role"]
            if role == "assistant" and m.get("tool_calls") and native_tools:
                wire.append(
                    {
                        "role": "assistant",
                        "content": m.get("content", ""),
                        "tool_calls": [
                            {
                                "function": {
                                    "name": call["name"],
                                    "arguments": call.get("arguments") or {},
                                }
                            }
                            for call in m["tool_calls"]
                        ],
                    }
                )
            elif role == "tool":
                if native_tools:
                    wire.append(
                        {
                            "role": "tool",
                            "content": str(m.get("content", "")),
                            "tool_name": m.get("name", ""),
                        }
                    )
                else:
                    wire.append(
                        {
                            "role": "user",
                            "content": f"[Tool result]: {m.get('content', '')}",
                        }
                    )
            else:
                wire.append({"role": role, "content": m.get("content", "")})
        return wire

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        def payload(native: bool) -> Dict[str, Any]:
            body: Dict[str, Any] = {
                "model": model,
                "messages": self.to_wire(messages, native_tools=native),
                "stream": False,
                "options": {"temperature": temperature, "num_predict": max_tokens},
            }
            if tools and native:
                body["tools"] = [{"type": "function", "function": t} for t in tools]
            body["options"].update(self.options.get("params") or {})
            return body

        try:
            data = await self._post_json("/api/chat", payload(native=bool(tools)))
        except LLMProviderError as exc:
            if tools and exc.status_code == 400 and "tool" in str(exc).lower():
                data = await self._post_json("/api/chat", payload(native=False))
            else:
                raise
        return self.parse(data)

    def parse(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if data.get("error"):
            raise LLMProviderError(f"ollama error: {data['error']}")
        message = data.get("message") or {}
        tool_name = tool_args = None
        calls = message.get("tool_calls") or []
        if calls:
            function = calls[0].get("function") or {}
            tool_name = function.get("name")
            tool_args = json_arguments(function.get("arguments"))
        prompt = data.get("prompt_eval_count", 0)
        completion = data.get("eval_count", 0)
        return self.build_response(
            message.get("content") or "",
            tool_name=tool_name,
            tool_args=tool_args,
            tool_call_id=calls[0].get("id") if calls else None,
            prompt_tokens=prompt,
            completion_tokens=completion,
            finish_reason=data.get("done_reason", "stop"),
            raw=data,
        )
