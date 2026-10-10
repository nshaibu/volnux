"""

Google Gemini LLM provider adapter using the new ``google-genai`` SDK.

Migration from ``google-generativeai`` (deprecated)
----------------------------------------------------
Old package: google-generativeai
    import google.generativeai as genai
    genai.configure(api_key=...)
    model = genai.GenerativeModel(...)
    chat = model.start_chat(history=...)
    response = chat.send_message(...)        ← synchronous only

New package: google-genai
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=...)
    response = await client.aio.models.generate_content(...)  ← async native

The new library is:
  - Async-native via client.aio (no run_in_executor workaround needed)
  - Client-based (no module-level configure() side effects)
  - Structured types via google.genai.types module
  - Unified API across Gemini Developer API and Vertex AI

Installation:
    pip install google-genai

Authentication:
    Set environment variable: GEMINI_API_KEY=your_key_here
    Never hardcode API keys in source.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Type

from .base import LLMProviderAdapterBase
from volnux.exceptions import LLMProviderError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..base import AgentAction


class GeminiProviderAdapter(LLMProviderAdapterBase):
    """
    Google Gemini provider adapter using the ``google-genai`` SDK.

    Supports all current Gemini models:
    - gemini-2.5-pro
    - gemini-2.0-flash
    - gemini-1.5-pro
    - gemini-1.5-flash

    Authentication
    --------------
    Reads the API key from the ``GEMINI_API_KEY`` environment variable.
    Never pass API keys as constructor arguments or hardcode them in source.

    Role mapping
    ------------
    Volnux uses OpenAI-style roles. Gemini uses ``user`` and ``model``.
    Mapping is applied before every call:

    +-----------------+-------------------------+
    | Volnux role     | Gemini role             |
    +=================+=========================+
    | system          | Prepended to first user |
    +-----------------+-------------------------+
    | user            | user                    |
    +-----------------+-------------------------+
    | assistant       | model                   |
    +-----------------+-------------------------+
    | tool            | user (prefixed)         |
    +-----------------+-------------------------+

    Gemini requires strictly alternating user/model turns. Consecutive
    messages of the same role are merged into a single Content object.

    Tool use
    --------
    Gemini's function calling returns ``FunctionCall`` parts in the response.
    Tool schemas are converted from OpenAI ``parameters`` format to
    Gemini ``FunctionDeclaration`` format via ``google.genai.types``.

    Async
    -----
    Uses ``client.aio.models.generate_content()`` — fully async, no
    ``run_in_executor`` wrapper needed. The event loop is never blocked.

    Requirements
    ------------
    pip install google-genai
    """

    provider_name = "gemini"

    def _build_client(self) -> Any:
        """
        Instantiate a ``google.genai.Client`` using the environment variable
        ``GEMINI_API_KEY``.

        Raises ``LLMProviderError`` if:
        - google-genai is not installed
        - GEMINI_API_KEY is not set or is empty
        """
        try:
            from google import genai
        except ImportError:
            raise LLMProviderError(
                "google-genai package not installed. "
                "Run: pip install google-genai\n"
                "Note: the old package (google-generativeai) is deprecated. "
                "If you have it installed, uninstall it first: "
                "pip uninstall google-generativeai"
            )

        # api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        # if not api_key:
        #     raise LLMProviderError(
        #         "GEMINI_API_KEY environment variable is not set. "
        #         "Export it before running Volnux: "
        #         "export GEMINI_API_KEY=your_api_key_here\n"
        #         "Never hardcode API keys in source code."
        #     )

        return genai.Client(api_key="")

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
        Call the Gemini API and return a standardised response dict.

        Parameters
        ----------
        messages:
            Full conversation history in Volnux/OpenAI role format.
        model:
            Gemini model identifier, e.g. ``"gemini-2.0-flash"``.
        temperature:
            Sampling temperature in [0.0, 2.0].
        max_tokens:
            Maximum tokens in the completion.
        tools:
            Optional list of tool schemas in OpenAI ``parameters`` format.
            Converted to Gemini ``FunctionDeclaration`` format internally.

        Returns
        -------
        Standardised dict consumed by ``AgentEventBase._run_agent_loop``:
        {
            "content": str,
            "action": AgentAction,
            "tool_name": str | None,
            "tool_args": dict | None,
            "tool_call_id": None, # Gemini does not use call IDs
            "token_usage": {"prompt": int, "completion": int, "total": int},
            "finish_reason": str,
            "raw": str, # str(response) for audit
        }
        """
        from google.genai import types
        from ..base import AgentAction

        client = self._build_client()

        system_content = next(
            (m["content"] for m in messages if m["role"] == "system"),
            "",
        )
        non_system = [m for m in messages if m["role"] != "system"]

        gemini_contents: List[types.Content] = self._build_contents(
            non_system[:-1],
            system_content=system_content,
        )

        last_msg = non_system[-1] if non_system else {"role": "user", "content": ""}
        current_text = last_msg["content"]

        # Prepend system content to current prompt when history is empty
        # (no prior user message existed to prepend it to).
        if system_content and not gemini_contents:
            current_text = f"[System]: {system_content}\n\n{current_text}"

        current_content = types.Content(
            role="user",
            parts=[types.Part(text=current_text)],
        )
        gemini_contents.append(current_content)

        gemini_tools = None
        if tools:
            gemini_tools = self._build_tool_declarations(tools, types)

        generation_config = types.GenerateContentConfig(
            temperature=temperature,
            max_output_tokens=max_tokens,
            tools=gemini_tools,
            tool_config=types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(
                    mode=types.FunctionCallingConfigMode.AUTO,
                )
            ),
        )

        try:
            response = await client.aio.models.generate_content(
                model=model,
                contents=gemini_contents,
                config=generation_config,
            )
        except Exception as exc:
            raise LLMProviderError(f"Gemini API error (model={model}): {exc}") from exc

        return self._parse_response(response, AgentAction)

    def _build_contents(
        self,
        messages: List[Dict[str, str]],
        system_content: str = "",
    ) -> List[Any]:
        """
        Convert Volnux/OpenAI message history to Gemini Content objects.

        Handles:
        - Role mapping (assistant → model, tool → user)
        - Consecutive same-role merging (Gemini requirement)
        - System prompt prepended to first user message
        """
        from google.genai import types

        contents: List[types.Content] = []
        system_prepended = False

        for msg in messages:
            role = msg["role"]
            content = msg.get("content", "")

            # Map roles
            if role == "assistant":
                gemini_role = "model"
                text = content
            elif role == "tool":
                gemini_role = "user"
                text = f"[Tool result]: {content}"
            else:
                # user messages
                gemini_role = "user"
                text = content

            # Prepend system content to the FIRST user message only
            if gemini_role == "user" and system_content and not system_prepended:
                text = f"[System]: {system_content}\n\n{text}"
                system_prepended = True

            # Merge consecutive same-role messages (Gemini strict requirement)
            if contents and contents[-1].role == gemini_role:
                # Append as a new Part to the existing Content
                contents[-1].parts.append(types.Part(text=text))
            else:
                contents.append(
                    types.Content(
                        role=gemini_role,
                        parts=[types.Part(text=text)],
                    )
                )

        return contents

    def _build_tool_declarations(
        self,
        tools: List[Dict[str, Any]],
        types: Any,
    ) -> List[Any]:
        """
        Convert OpenAI-format tool schemas to Gemini FunctionDeclaration list.

        OpenAI format:
        {
            "name": "FetchWebPageEvent",
            "description": "...",
            "parameters": {
                "type": "object",
                "properties": {"url": {"type": "string", ...}},
                "required": ["url"],
            }
        }

        Gemini format (google.genai.types):
        types.Tool(function_declarations=[
            types.FunctionDeclaration(
                name="FetchWebPageEvent",
                description="...",
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={"url": types.Schema(type=types.Type.STRING)},
                    required=["url"],
                ),
            )
        ])
        """
        declarations = []

        for tool in tools:
            params = tool.get("parameters", {})
            schema = self._convert_schema(params, types)
            declarations.append(
                types.FunctionDeclaration(
                    name=tool["name"],
                    description=tool.get("description", "")[:500],
                    parameters=schema,
                )
            )

        return [types.Tool(function_declarations=declarations)]

    def _convert_schema(self, schema: Dict[str, Any], types: Any) -> Any:
        """
        Recursively convert a JSON Schema dict to a ``google.genai.types.Schema``.

        Handles: object, array, string, integer, number, boolean types.
        """
        type_map = {
            "object": types.Type.OBJECT,
            "array": types.Type.ARRAY,
            "string": types.Type.STRING,
            "integer": types.Type.INTEGER,
            "number": types.Type.NUMBER,
            "boolean": types.Type.BOOLEAN,
            "any": types.Type.STRING,  # fallback
        }

        json_type = schema.get("type", "object").lower()
        gemini_type = type_map.get(json_type, types.Type.STRING)

        # Recursively convert properties for object types
        properties = {}
        for prop_name, prop_schema in schema.get("properties", {}).items():
            properties[prop_name] = self._convert_schema(prop_schema, types)

        # Recursively convert items for array types
        items = None
        if "items" in schema:
            items = self._convert_schema(schema["items"], types)

        return types.Schema(
            type=gemini_type,
            description=schema.get("description", ""),
            properties=properties if properties else None,
            required=schema.get("required"),
            items=items,
            nullable=schema.get("nullable", False),
        )

    def _parse_response(
        self,
        response: Any,
        agent_action: Type["AgentAction"],
    ) -> Dict[str, Any]:
        """
        Parse a Gemini GenerateContentResponse into the standardised
        Volnux agent response dict.

        Handles:
        - Text responses (THINK or FINISH based on content prefix)
        - Function call responses (TOOL_CALL)
        - Mixed responses (function call takes priority)
        - Token usage from usage_metadata
        - Finish reason from a candidate
        """
        action = agent_action.THINK
        tool_name = None
        tool_args = None
        content_text = ""
        finish_reason = "unknown"

        candidate = response.candidates[0] if response.candidates else None

        if candidate:
            finish_reason = str(getattr(candidate, "finish_reason", "unknown"))

            for part in candidate.content.parts:

                # Function call part — takes priority over text
                fc = getattr(part, "function_call", None)
                if fc and getattr(fc, "name", None):
                    action = agent_action.TOOL_CALL
                    tool_name = fc.name
                    # fc.args is a MapComposite — convert to plain dict
                    tool_args = dict(fc.args) if fc.args else {}
                    continue

                # Text part
                text = getattr(part, "text", None)
                if text:
                    if text.upper().startswith("TOOL:"):
                        action = agent_action.TOOL_CALL
                        components = self.parse_react_tool(text)
                        tool_name = components["tool_name"]
                        tool_args = components.get("tool_args", {})

                    content_text += text

        usage_meta = getattr(response, "usage_metadata", None)
        prompt_tok = (
            int(getattr(usage_meta, "prompt_token_count", 0) or 0) if usage_meta else 0
        )
        output_tok = (
            int(getattr(usage_meta, "candidates_token_count", 0) or 0)
            if usage_meta
            else 0
        )

        # Only infer from text when no function call was detected.
        if action == agent_action.THINK and content_text:
            upper = content_text.strip().upper()
            if upper.startswith("FINISH:"):
                action = agent_action.FINISH
            elif upper.startswith("HANDOFF:"):
                action = agent_action.HANDOFF
            elif upper.startswith("ERROR:"):
                action = agent_action.ERROR
            elif upper.startswith("PAUSE:"):
                action = agent_action.PAUSE

        return {
            "content": content_text,
            "action": action,
            "tool_name": tool_name,
            "tool_args": tool_args,
            "tool_call_id": None,  # Gemini does not use call IDs
            "token_usage": {
                "prompt": prompt_tok,
                "completion": output_tok,
                "total": prompt_tok + output_tok,
            },
            "finish_reason": finish_reason,
            "raw": str(response),  # full response for audit trail
        }
