from typing import Any, Dict, List, Optional

from volnux.exceptions import LLMProviderError
from .http import HTTPProviderAdapter

__all__ = ["AnthropicProviderAdapter"]


class AnthropicProviderAdapter(HTTPProviderAdapter):
    """Anthropic's Messages API (Claude). Key from ``ANTHROPIC_API_KEY``; endpoint
    from ``ANTHROPIC_BASE_URL`` or ``https://api.anthropic.com/v1``.

    Tool use is native: the model's call is a ``tool_use`` content block, and its
    result goes back as a ``tool_result`` block in a user message.
    """

    provider_name = "anthropic"
    default_base_url = "https://api.anthropic.com/v1"
    base_url_env = "ANTHROPIC_BASE_URL"
    api_key_env = "ANTHROPIC_API_KEY"
    api_version = "2023-06-01"

    def _headers(self) -> Dict[str, str]:
        headers = super()._headers()
        headers["x-api-key"] = self._api_key() or ""
        headers["anthropic-version"] = self.options.get("api_version", self.api_version)
        return headers

    @staticmethod
    def to_wire(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """``{"system": str, "messages": [...]}``: system text is a parameter, not a
        message, and consecutive turns of one role are one message of several blocks
        (the API wants user and assistant to alternate)."""
        system: List[str] = []
        turns: List[Dict[str, Any]] = []

        def add(role: str, blocks: List[Dict[str, Any]]) -> None:
            if turns and turns[-1]["role"] == role:
                turns[-1]["content"].extend(blocks)
            else:
                turns.append({"role": role, "content": list(blocks)})

        for m in messages:
            role = m["role"]
            content = m.get("content", "")
            if role == "system":
                system.append(content)
            elif role == "assistant":
                blocks: List[Dict[str, Any]] = []
                if content:
                    blocks.append({"type": "text", "text": content})
                for call in m.get("tool_calls") or []:
                    blocks.append(
                        {
                            "type": "tool_use",
                            "id": call["id"],
                            "name": call["name"],
                            "input": call.get("arguments") or {},
                        }
                    )
                add("assistant", blocks or [{"type": "text", "text": ""}])
            elif role == "tool":
                add(
                    "user",
                    [
                        {
                            "type": "tool_result",
                            "tool_use_id": m.get("tool_call_id", ""),
                            "content": str(content),
                        }
                    ],
                )
            else:
                add("user", [{"type": "text", "text": content}])
        return {"system": "\n\n".join(s for s in system if s), "messages": turns}

    @staticmethod
    def to_wire_tools(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "name": t["name"],
                "description": t.get("description", ""),
                "input_schema": t.get("parameters")
                or {"type": "object", "properties": {}},
            }
            for t in tools
        ]

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        wire = self.to_wire(messages)
        payload: Dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": wire["messages"],
        }
        if wire["system"]:
            payload["system"] = wire["system"]
        if tools:
            payload["tools"] = self.to_wire_tools(tools)
        payload.update(self.options.get("params") or {})
        payload.update(kwargs.get("params") or {})

        return self.parse(await self._post_json("/messages", payload))

    def parse(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if data.get("type") == "error":
            error = data.get("error") or {}
            raise LLMProviderError(
                f"anthropic error: {error.get('type', '')}: {error.get('message', '')}"
            )
        texts: List[str] = []
        tool_name = tool_args = tool_call_id = None
        for block in data.get("content") or []:
            if block.get("type") == "text":
                texts.append(block.get("text", ""))
            elif block.get("type") == "tool_use" and tool_name is None:
                tool_name = block.get("name")
                tool_args = block.get("input") or {}
                tool_call_id = block.get("id")
        usage = data.get("usage") or {}
        return self.build_response(
            "\n".join(t for t in texts if t),
            tool_name=tool_name,
            tool_args=tool_args,
            tool_call_id=tool_call_id,
            prompt_tokens=usage.get("input_tokens", 0),
            completion_tokens=usage.get("output_tokens", 0),
            finish_reason=data.get("stop_reason") or "",
            raw=data,
        )
