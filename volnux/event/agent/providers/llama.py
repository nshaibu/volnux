import json
import os

try:
    import httpx
except ImportError:
    # raise ImportError("httpx package not installed. Run: pip install httpx")
    pass
from typing import Any, Dict, List, Optional

from .base import LLMProviderAdapterBase
from volnux.exceptions import LLMProviderError


class OllamaProviderAdapter(LLMProviderAdapterBase):
    """
    Ollama provider adapter for locally hosted models.

    Supports any model available in the local Ollama installation:
    llama3, mistral, phi3, gemma2, qwen2, deepseek-r1, codellama, etc.

    Ollama exposes an OpenAI-compatible ``/api/chat`` endpoint. This adapter
    calls it directly via ``httpx`` to avoid an ``openai`` SDK dependency
    for local deployments. The endpoint is configurable via the
    ``OLLAMA_BASE_URL`` environment variable (default: ``http://localhost:11434``).

    Tool use
    --------
    Ollama supports function calling for models that include tool use in
    their training (llama3.1+, mistral-nemo, qwen2.5, etc.). Models that
    do not support function calling fall back to structured text output —
    the agent's default action parser handles FINISH:/TOOL:/etc. prefixes.

    Streaming
    ---------
    This adapter uses non-streaming mode (``stream=false``) for simplicity
    and compatibility with the synchronous ``token_usage`` reporting that
    Ollama provides only in non-streaming responses.

    Requirements: Ollama running locally (https://ollama.ai).
                  ``pip install httpx``
    """

    provider_name = "ollama"

    _DEFAULT_BASE_URL = "http://localhost:11434"

    async def complete(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        from ..base import AgentAction

        base_url = os.environ.get("OLLAMA_BASE_URL", self._DEFAULT_BASE_URL).rstrip("/")
        endpoint = f"{base_url}/api/chat"

        # Ollama's /api/chat is OpenAI-compatible for basic message passing.
        # role="tool" is not supported by all models — convert to "user"
        # with a structured prefix for compatibility.
        converted_messages = []
        for msg in messages:
            if msg["role"] == "tool":
                converted_messages.append(
                    {
                        "role": "user",
                        "content": f"[Tool result]: {msg['content']}",
                    }
                )
            else:
                converted_messages.append(msg)

        payload: Dict[str, Any] = {
            "model": model,
            "messages": converted_messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
        }

        # Include tools only when provided — Ollama ignores unknown fields,
        # but some older versions may reject them.
        # if tools:
        #     payload["tools"] = [{"type": "function", "function": t} for t in tools]

        try:
            async with httpx.AsyncClient(timeout=1200) as client:
                http_response = await client.post(
                    endpoint,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                http_response.raise_for_status()
                data = http_response.json()
        except httpx.ConnectError as exc:
            raise LLMProviderError(
                f"Cannot connect to Ollama at '{base_url}'. "
                f"Ensure Ollama is running: https://ollama.ai. Error: {exc}"
            ) from exc
        except httpx.HTTPStatusError as exc:
            raise LLMProviderError(
                f"Ollama HTTP error {exc.response.status_code}: " f"{exc.response.text}"
            ) from exc
        except Exception as exc:
            raise LLMProviderError(f"Ollama request failed: {exc}") from exc

        message = data.get("message", {})
        content_text = message.get("content", "")
        tool_calls = message.get("tool_calls", [])

        action = AgentAction.THINK
        tool_name = None
        tool_args = None
        tool_call_id = None

        if tool_calls:
            # Ollama returns tool_calls in OpenAI-compatible format
            tc = tool_calls[0]
            fn = tc.get("function", {})
            action = AgentAction.TOOL_CALL
            tool_name = fn.get("name", "")
            tool_call_id = tc.get("id")
            raw_args = fn.get("arguments", {})
            # arguments may be a JSON string or already a dict
            if isinstance(raw_args, str):
                try:
                    tool_args = json.loads(raw_args)
                except json.JSONDecodeError:
                    tool_args = {}
            else:
                tool_args = raw_args

        if content_text.upper().startswith("TOOL:"):
            components = self.parse_react_tool(content_text)
            action = AgentAction.TOOL_CALL
            tool_name = components["tool_name"]
            tool_args = components.get("tool_args", {})

        # Ollama reports usage at the response root in non-streaming mode.
        prompt_tok = data.get("prompt_eval_count", 0)
        output_tok = data.get("eval_count", 0)

        finish_reason = data.get("done_reason", "stop")

        return {
            "content": content_text,
            "action": action,
            "tool_name": tool_name,
            "tool_args": tool_args,
            "tool_call_id": tool_call_id,
            "token_usage": {
                "prompt": prompt_tok,
                "completion": output_tok,
                "total": prompt_tok + output_tok,
            },
            "finish_reason": finish_reason,
            "raw": data,
        }
