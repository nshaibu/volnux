"""Provider adapters against canned HTTP answers (no network) and a fake Gemini client."""

import json
from types import SimpleNamespace
from typing import Any, Callable, Dict, List

import httpx
import pytest

from volnux.event.agent._state import AgentAction
from volnux.event.agent.providers import (
    AnthropicProviderAdapter,
    GeminiProviderAdapter,
    LLMProvider,
    LLMProviderAdapterBase,
    LLMProviderRegistry,
    MistralProviderAdapter,
    OllamaProviderAdapter,
    OpenAIProviderAdapter,
    VLLMProviderAdapter,
)
from volnux.exceptions import LLMProviderError

CONVERSATION = [
    {"role": "system", "content": "be brief"},
    {"role": "user", "content": "weather?"},
    {
        "role": "assistant",
        "content": "checking",
        "tool_calls": [{"id": "c1", "name": "Weather", "arguments": {"city": "Accra"}}],
    },
    {"role": "tool", "tool_call_id": "c1", "name": "Weather", "content": "sunny"},
]
TOOLS = [
    {
        "name": "Weather",
        "description": "Weather for a city",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string", "description": "the city"}},
            "required": ["city"],
        },
    }
]


class Server:
    """An ``httpx`` transport that records requests and answers from a script."""

    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.requests: List[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.answers.pop(0)
        if isinstance(answer, httpx.Response):
            return answer
        if isinstance(answer, Exception):
            raise answer
        return httpx.Response(200, json=answer)

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)

    @property
    def body(self) -> Dict[str, Any]:
        return json.loads(self.requests[-1].content)


async def complete(adapter, **kw):
    return await adapter.complete(
        CONVERSATION, kw.pop("model", "m"), 0.2, 100, tools=kw.pop("tools", TOOLS), **kw
    )


# --- registry --------------------------------------------------------------------


def test_the_builtin_providers_are_registered():
    assert {"openai", "anthropic", "gemini", "mistral", "ollama", "vllm"} <= set(
        LLMProviderRegistry.available()
    )


def test_names_are_case_insensitive_and_accept_the_enum():
    assert isinstance(
        LLMProviderRegistry.resolve("OpenAI", api_key="k"), OpenAIProviderAdapter
    )
    assert isinstance(
        LLMProviderRegistry.resolve(LLMProvider.OLLAMA), OllamaProviderAdapter
    )


def test_an_unknown_provider_names_the_available_ones():
    with pytest.raises(LLMProviderError, match="Available") as caught:
        LLMProviderRegistry.resolve("nope")
    assert caught.value.retryable is False


def test_a_subclass_registers_itself_but_an_intermediate_base_does_not():
    class Base(LLMProviderAdapterBase):
        pass  # no provider_name: not a provider

    class Mine(Base):
        provider_name = "registry-test-mine"

    try:
        assert LLMProviderRegistry.is_registered("registry-test-mine")
        assert not LLMProviderRegistry.is_registered("Base")
        assert LLMProviderRegistry.resolve("registry-test-mine", x=1).options == {
            "x": 1
        }
    finally:
        assert LLMProviderRegistry.unregister("registry-test-mine") is True
    assert LLMProviderRegistry.unregister("registry-test-mine") is False


def test_registering_another_class_under_a_name_warns_but_the_same_class_does_not(
    recwarn,
):
    class A(LLMProviderAdapterBase):
        provider_name = "registry-test-dup"

    try:
        LLMProviderRegistry.register("registry-test-dup", A)  # same class: quiet
        assert not recwarn.list

        class B(LLMProviderAdapterBase):
            pass

        with pytest.warns(UserWarning, match="overwriting"):
            LLMProviderRegistry.register("registry-test-dup", B)
    finally:
        LLMProviderRegistry.unregister("registry-test-dup")


def test_build_response_is_the_neutral_shape():
    plain = LLMProviderAdapterBase.build_response(
        "hi", prompt_tokens=3, completion_tokens=4
    )
    assert plain["action"] is AgentAction.THINK and plain["token_usage"] == {
        "prompt": 3,
        "completion": 4,
        "total": 7,
    }
    call = LLMProviderAdapterBase.build_response(tool_name="T", tool_args={"a": 1})
    assert call["action"] is AgentAction.TOOL_CALL and call["tool_args"] == {"a": 1}


# --- OpenAI-compatible ---------------------------------------------------------------


OPENAI_TEXT = {
    "choices": [{"message": {"content": "FINISH: hi"}, "finish_reason": "stop"}],
    "usage": {"prompt_tokens": 7, "completion_tokens": 2},
}
OPENAI_CALL = {
    "choices": [
        {
            "message": {
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_9",
                        "type": "function",
                        "function": {
                            "name": "Weather",
                            "arguments": '{"city": "Accra"}',
                        },
                    }
                ],
            },
            "finish_reason": "tool_calls",
        }
    ],
    "usage": {"prompt_tokens": 5, "completion_tokens": 3},
}


@pytest.mark.asyncio
async def test_openai_request_carries_messages_tools_and_the_key():
    server = Server(OPENAI_TEXT)
    adapter = OpenAIProviderAdapter(api_key="sk-test", transport=server.transport)

    await complete(adapter, model="gpt-x")

    request = server.requests[0]
    assert str(request.url) == "https://api.openai.com/v1/chat/completions"
    assert request.headers["authorization"] == "Bearer sk-test"
    body = server.body
    assert body["model"] == "gpt-x" and body["temperature"] == 0.2
    assert body["max_completion_tokens"] == 100 and body["tool_choice"] == "auto"
    assert body["tools"] == [{"type": "function", "function": TOOLS[0]}]
    assistant, tool = body["messages"][2], body["messages"][3]
    assert assistant["tool_calls"][0] == {
        "id": "c1",
        "type": "function",
        "function": {"name": "Weather", "arguments": '{"city": "Accra"}'},
    }
    assert tool == {"role": "tool", "tool_call_id": "c1", "content": "sunny"}


@pytest.mark.asyncio
async def test_openai_text_answer_and_usage():
    adapter = OpenAIProviderAdapter(
        api_key="k", transport=Server(OPENAI_TEXT).transport
    )

    out = await complete(adapter)

    assert out["content"] == "FINISH: hi" and out["tool_name"] is None
    assert out["token_usage"] == {"prompt": 7, "completion": 2, "total": 9}
    assert out["finish_reason"] == "stop"


@pytest.mark.asyncio
async def test_openai_native_tool_call_is_parsed():
    adapter = OpenAIProviderAdapter(
        api_key="k", transport=Server(OPENAI_CALL).transport
    )

    out = await complete(adapter)

    assert out["action"] is AgentAction.TOOL_CALL
    assert (out["tool_name"], out["tool_args"], out["tool_call_id"]) == (
        "Weather",
        {"city": "Accra"},
        "call_9",
    )


@pytest.mark.asyncio
async def test_no_tools_means_no_tool_fields_in_the_request():
    server = Server(OPENAI_TEXT)
    await complete(
        OpenAIProviderAdapter(api_key="k", transport=server.transport), tools=None
    )
    assert "tools" not in server.body and "tool_choice" not in server.body


@pytest.mark.asyncio
async def test_extra_request_params_are_passed_through():
    server = Server(OPENAI_TEXT)
    adapter = OpenAIProviderAdapter(
        api_key="k", transport=server.transport, params={"top_p": 0.5}
    )
    await complete(adapter)
    assert server.body["top_p"] == 0.5


@pytest.mark.asyncio
async def test_the_key_and_endpoint_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "from-env")
    monkeypatch.setenv("OPENAI_BASE_URL", "https://proxy.example/v1/")
    server = Server(OPENAI_TEXT)

    await complete(OpenAIProviderAdapter(transport=server.transport))

    assert str(server.requests[0].url) == "https://proxy.example/v1/chat/completions"
    assert server.requests[0].headers["authorization"] == "Bearer from-env"


@pytest.mark.asyncio
async def test_a_missing_key_is_a_clear_error_that_is_not_retried(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    adapter = OpenAIProviderAdapter(transport=Server().transport)

    with pytest.raises(LLMProviderError, match="OPENAI_API_KEY") as caught:
        await complete(adapter)
    assert caught.value.retryable is False


@pytest.mark.asyncio
async def test_mistral_and_vllm_speak_the_same_protocol_at_their_own_endpoints(
    monkeypatch,
):
    monkeypatch.delenv("VLLM_BASE_URL", raising=False)
    mistral, vllm = Server(OPENAI_TEXT), Server(OPENAI_TEXT)

    await complete(MistralProviderAdapter(api_key="k", transport=mistral.transport))
    await complete(VLLMProviderAdapter(transport=vllm.transport))  # no key needed

    assert str(mistral.requests[0].url) == "https://api.mistral.ai/v1/chat/completions"
    assert str(vllm.requests[0].url) == "http://localhost:8000/v1/chat/completions"
    assert "authorization" not in vllm.requests[0].headers


@pytest.mark.parametrize(
    "response, retryable",
    [
        (httpx.Response(429, text="slow down"), True),
        (httpx.Response(503, text="overloaded"), True),
        (httpx.Response(401, text="bad key"), False),
        (httpx.Response(400, text="bad request"), False),
    ],
)
@pytest.mark.asyncio
async def test_http_errors_say_whether_a_retry_can_help(response, retryable):
    adapter = OpenAIProviderAdapter(api_key="k", transport=Server(response).transport)

    with pytest.raises(LLMProviderError) as caught:
        await complete(adapter)

    assert caught.value.retryable is retryable
    assert caught.value.status_code == response.status_code


@pytest.mark.asyncio
async def test_timeouts_and_connection_failures_are_retryable_provider_errors():
    for failure in (httpx.ReadTimeout("slow"), httpx.ConnectError("refused")):
        adapter = OpenAIProviderAdapter(
            api_key="k", transport=Server(failure).transport
        )
        with pytest.raises(LLMProviderError) as caught:
            await complete(adapter)
        assert caught.value.retryable is True


@pytest.mark.asyncio
async def test_a_reply_that_is_not_json_or_has_no_choices_is_an_error():
    garbage = httpx.Response(200, text="<html>oops</html>")
    with pytest.raises(LLMProviderError, match="not JSON"):
        await complete(
            OpenAIProviderAdapter(api_key="k", transport=Server(garbage).transport)
        )
    with pytest.raises(LLMProviderError, match="no choices"):
        await complete(
            OpenAIProviderAdapter(
                api_key="k", transport=Server({"choices": []}).transport
            )
        )


# --- Anthropic ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_anthropic_request_shape():
    server = Server({"content": [{"type": "text", "text": "FINISH: hi"}], "usage": {}})
    adapter = AnthropicProviderAdapter(api_key="ak", transport=server.transport)

    await complete(adapter, model="claude-x")

    request = server.requests[0]
    assert str(request.url) == "https://api.anthropic.com/v1/messages"
    assert request.headers["x-api-key"] == "ak"
    assert request.headers["anthropic-version"] == "2023-06-01"
    body = server.body
    assert body["system"] == "be brief" and body["max_tokens"] == 100
    assert body["tools"][0] == {
        "name": "Weather",
        "description": "Weather for a city",
        "input_schema": TOOLS[0]["parameters"],
    }
    assert [m["role"] for m in body["messages"]] == ["user", "assistant", "user"]
    assert body["messages"][1]["content"] == [
        {"type": "text", "text": "checking"},
        {"type": "tool_use", "id": "c1", "name": "Weather", "input": {"city": "Accra"}},
    ]
    assert body["messages"][2]["content"] == [
        {"type": "tool_result", "tool_use_id": "c1", "content": "sunny"}
    ]


def test_anthropic_merges_consecutive_turns_of_one_role():
    wire = AnthropicProviderAdapter.to_wire(
        [
            {"role": "system", "content": "s1"},
            {"role": "system", "content": "s2"},
            {"role": "user", "content": "a"},
            {"role": "user", "content": "b"},
        ]
    )
    assert wire["system"] == "s1\n\ns2"
    assert len(wire["messages"]) == 1 and len(wire["messages"][0]["content"]) == 2


@pytest.mark.asyncio
async def test_anthropic_tool_use_block_becomes_a_native_call():
    answer = {
        "content": [
            {"type": "text", "text": "looking"},
            {
                "type": "tool_use",
                "id": "tu_1",
                "name": "Weather",
                "input": {"city": "Accra"},
            },
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 11, "output_tokens": 6},
    }
    adapter = AnthropicProviderAdapter(api_key="k", transport=Server(answer).transport)

    out = await complete(adapter)

    assert out["content"] == "looking" and out["tool_name"] == "Weather"
    assert out["tool_args"] == {"city": "Accra"} and out["tool_call_id"] == "tu_1"
    assert out["token_usage"] == {"prompt": 11, "completion": 6, "total": 17}
    assert out["finish_reason"] == "tool_use"


@pytest.mark.asyncio
async def test_anthropic_error_body_is_an_error():
    answer = {"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}
    adapter = AnthropicProviderAdapter(api_key="k", transport=Server(answer).transport)
    with pytest.raises(LLMProviderError, match="overloaded_error"):
        await complete(adapter)


# --- Ollama --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ollama_native_tools_round_trip():
    answer = {
        "message": {
            "content": "",
            "tool_calls": [
                {"function": {"name": "Weather", "arguments": {"city": "Kumasi"}}}
            ],
        },
        "prompt_eval_count": 20,
        "eval_count": 8,
        "done_reason": "stop",
    }
    server = Server(answer)
    adapter = OllamaProviderAdapter(transport=server.transport)

    out = await complete(adapter, model="llama3.1")

    assert str(server.requests[0].url) == "http://localhost:11434/api/chat"
    body = server.body
    assert body["stream"] is False and body["options"] == {
        "temperature": 0.2,
        "num_predict": 100,
    }
    assert body["messages"][3] == {
        "role": "tool",
        "content": "sunny",
        "tool_name": "Weather",
    }
    assert (out["tool_name"], out["tool_args"]) == ("Weather", {"city": "Kumasi"})
    assert out["token_usage"] == {"prompt": 20, "completion": 8, "total": 28}


@pytest.mark.asyncio
async def test_ollama_falls_back_to_plain_text_for_a_model_without_tools():
    refusal = httpx.Response(
        400, json={"error": "registry.ollama.ai/library/gemma does not support tools"}
    )
    ok = {"message": {"content": "FINISH: sunny"}, "done_reason": "stop"}
    server = Server(refusal, ok)

    out = await complete(OllamaProviderAdapter(transport=server.transport))

    assert len(server.requests) == 2
    retry = server.body
    assert "tools" not in retry
    assert retry["messages"][-1] == {"role": "user", "content": "[Tool result]: sunny"}
    assert out["content"] == "FINISH: sunny"


@pytest.mark.asyncio
async def test_ollama_other_errors_are_not_mistaken_for_missing_tool_support():
    server = Server(httpx.Response(400, text="invalid model name"))
    with pytest.raises(LLMProviderError):
        await complete(OllamaProviderAdapter(transport=server.transport))
    assert len(server.requests) == 1


# --- Gemini ----------------------------------------------------------------------------


class FakeGemini:
    """Stands in for ``genai.Client``; records what it was asked."""

    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: List[Dict[str, Any]] = []
        self.aio = SimpleNamespace(
            models=SimpleNamespace(generate_content=self._generate)
        )

    async def _generate(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def gemini_response(parts: List[Any], finish="STOP", prompt=9, completion=4):
    return SimpleNamespace(
        candidates=[
            SimpleNamespace(
                content=SimpleNamespace(parts=parts),
                finish_reason=SimpleNamespace(name=finish),
            )
        ],
        usage_metadata=SimpleNamespace(
            prompt_token_count=prompt, candidates_token_count=completion
        ),
    )


@pytest.mark.asyncio
async def test_gemini_conversation_is_converted_to_contents_and_config():
    client = FakeGemini(
        gemini_response([SimpleNamespace(text="FINISH: ok", function_call=None)])
    )
    adapter = GeminiProviderAdapter(client=client)

    out = await complete(adapter, model="gemini-x")

    call = client.calls[0]
    assert call["model"] == "gemini-x"
    config = call["config"]
    assert config.system_instruction == "be brief"
    assert config.temperature == 0.2 and config.max_output_tokens == 100
    assert config.tools[0].function_declarations[0].name == "Weather"
    assert [c.role for c in call["contents"]] == ["user", "model", "user"]
    model_parts = call["contents"][1].parts
    assert model_parts[0].text == "checking"
    assert model_parts[1].function_call.name == "Weather"
    assert call["contents"][2].parts[0].function_response.name == "Weather"
    assert out["content"] == "FINISH: ok" and out["finish_reason"] == "STOP"
    assert out["token_usage"] == {"prompt": 9, "completion": 4, "total": 13}


@pytest.mark.asyncio
async def test_gemini_function_call_part_is_a_native_call():
    part = SimpleNamespace(
        text=None,
        function_call=SimpleNamespace(name="Weather", args={"city": "Tamale"}),
    )
    adapter = GeminiProviderAdapter(client=FakeGemini(gemini_response([part])))

    out = await complete(adapter)

    assert out["action"] is AgentAction.TOOL_CALL
    assert (out["tool_name"], out["tool_args"]) == ("Weather", {"city": "Tamale"})


@pytest.mark.asyncio
async def test_gemini_without_a_key_is_a_clear_error(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    with pytest.raises(LLMProviderError, match="GEMINI_API_KEY") as caught:
        await complete(GeminiProviderAdapter())
    assert caught.value.retryable is False


@pytest.mark.asyncio
async def test_gemini_api_failures_are_provider_errors_and_client_errors_are_final():
    transient = GeminiProviderAdapter(client=FakeGemini(RuntimeError("unavailable")))
    with pytest.raises(LLMProviderError) as caught:
        await complete(transient)
    assert caught.value.retryable is True

    refused = RuntimeError("bad request")
    refused.code = 400  # type: ignore[attr-defined]
    with pytest.raises(LLMProviderError) as caught:
        await complete(GeminiProviderAdapter(client=FakeGemini(refused)))
    assert caught.value.retryable is False and caught.value.status_code == 400


@pytest.mark.asyncio
async def test_gemini_with_no_candidates_is_an_error():
    empty = SimpleNamespace(candidates=[], usage_metadata=None)
    with pytest.raises(LLMProviderError, match="no candidates"):
        await complete(GeminiProviderAdapter(client=FakeGemini(empty)))
