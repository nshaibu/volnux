"""Google Gemini through the ``google-genai`` SDK (``pip install google-genai``).

Key from ``GEMINI_API_KEY`` (or ``GOOGLE_API_KEY``) or the ``api_key`` option; never
hardcode it. A pre-built client can be passed as the ``client`` option (tests, Vertex).
"""

import os
from typing import Any, Dict, List, Optional

from volnux.exceptions import LLMProviderError
from .base import LLMProviderAdapterBase

__all__ = ["GeminiProviderAdapter"]

_TYPES = {
    "object": "OBJECT",
    "array": "ARRAY",
    "string": "STRING",
    "integer": "INTEGER",
    "number": "NUMBER",
    "boolean": "BOOLEAN",
}


class GeminiProviderAdapter(LLMProviderAdapterBase):
    """Gemini with native function calling.

    Gemini's roles are ``user`` and ``model`` and they must alternate, so consecutive
    turns merge into one ``Content``; the system prompt goes in ``system_instruction``;
    an assistant tool call is a ``function_call`` part and its result a
    ``function_response`` part (matched by the tool's name: Gemini has no call ids).
    """

    provider_name = "gemini"

    def _build_client(self) -> Any:
        client = self.options.get("client")
        if client is not None:
            return client
        try:
            from google import genai
        except ImportError as exc:
            raise LLMProviderError(
                "google-genai is not installed: pip install google-genai",
                retryable=False,
            ) from exc
        api_key = (
            self.options.get("api_key")
            or os.environ.get("GEMINI_API_KEY", "").strip()
            or os.environ.get("GOOGLE_API_KEY", "").strip()
        )
        if not api_key:
            raise LLMProviderError(
                "No API key for 'gemini': set GEMINI_API_KEY or pass api_key in "
                "llm_options. Never hardcode keys in source.",
                retryable=False,
            )
        self.options["client"] = genai.Client(api_key=api_key)
        return self.options["client"]

    # --- conversion -------------------------------------------------------

    def _contents(self, messages: List[Dict[str, Any]], types: Any) -> List[Any]:
        contents: List[Any] = []

        def add(role: str, part: Any) -> None:
            if contents and contents[-1].role == role:
                contents[-1].parts.append(part)
            else:
                contents.append(types.Content(role=role, parts=[part]))

        for m in messages:
            role = m["role"]
            text = m.get("content", "")
            if role == "assistant":
                if text:
                    add("model", types.Part(text=text))
                for call in m.get("tool_calls") or []:
                    add(
                        "model",
                        types.Part(
                            function_call=types.FunctionCall(
                                name=call["name"], args=call.get("arguments") or {}
                            )
                        ),
                    )
                if not text and not m.get("tool_calls"):
                    add("model", types.Part(text=""))
            elif role == "tool":
                add(
                    "user",
                    types.Part(
                        function_response=types.FunctionResponse(
                            name=m.get("name", ""), response={"result": str(text)}
                        )
                    ),
                )
            elif role == "user":
                add("user", types.Part(text=text))
        if not contents:
            contents.append(types.Content(role="user", parts=[types.Part(text="")]))
        return contents

    def _schema(self, schema: Dict[str, Any], types: Any) -> Any:
        kind = _TYPES.get(str(schema.get("type", "object")).lower(), "STRING")
        props = {
            k: self._schema(v, types)
            for k, v in (schema.get("properties") or {}).items()
        }
        return types.Schema(
            type=getattr(types.Type, kind),
            description=schema.get("description") or None,
            properties=props or None,
            required=schema.get("required") or None,
            items=self._schema(schema["items"], types) if "items" in schema else None,
        )

    def _declarations(self, tools: List[Dict[str, Any]], types: Any) -> List[Any]:
        return [
            types.Tool(
                function_declarations=[
                    types.FunctionDeclaration(
                        name=t["name"],
                        description=(t.get("description") or "")[:1000],
                        parameters=self._schema(t.get("parameters") or {}, types),
                    )
                    for t in tools
                ]
            )
        ]

    # --- call -------------------------------------------------------------

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        from google.genai import types

        client = self._build_client()
        system = "\n\n".join(m["content"] for m in messages if m["role"] == "system")
        config = types.GenerateContentConfig(
            temperature=temperature,
            max_output_tokens=max_tokens,
            system_instruction=system or None,
            tools=self._declarations(tools, types) if tools else None,
            # the agent runs the tools itself
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                disable=True
            ),
        )
        try:
            response = await client.aio.models.generate_content(
                model=model,
                contents=self._contents(messages, types),
                config=config,
            )
        except LLMProviderError:
            raise
        except Exception as exc:
            status = getattr(exc, "code", None)
            retryable = not (
                isinstance(status, int) and 400 <= status < 500 and status != 429
            )
            raise LLMProviderError(
                f"Gemini API error (model={model}): {exc}",
                retryable=retryable,
                status_code=status if isinstance(status, int) else None,
            ) from exc
        return self.parse(response)

    def parse(self, response: Any) -> Dict[str, Any]:
        candidates = getattr(response, "candidates", None) or []
        if not candidates:
            raise LLMProviderError("gemini returned no candidates")
        candidate = candidates[0]
        texts: List[str] = []
        tool_name = tool_args = None
        parts = getattr(getattr(candidate, "content", None), "parts", None) or []
        for part in parts:
            call = getattr(part, "function_call", None)
            if call is not None and getattr(call, "name", None) and tool_name is None:
                tool_name = call.name
                tool_args = dict(call.args) if call.args else {}
            elif getattr(part, "text", None):
                texts.append(part.text)
        usage = getattr(response, "usage_metadata", None)
        finish = getattr(candidate, "finish_reason", "")
        return self.build_response(
            "".join(texts),
            tool_name=tool_name,
            tool_args=tool_args,
            tool_call_id=None,
            prompt_tokens=getattr(usage, "prompt_token_count", 0) if usage else 0,
            completion_tokens=(
                getattr(usage, "candidates_token_count", 0) if usage else 0
            ),
            finish_reason=str(getattr(finish, "name", finish) or ""),
            raw=str(response),
        )
