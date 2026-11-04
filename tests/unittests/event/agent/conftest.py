"""Fixtures for the agent tests: a scripted model, so no test touches a real API."""

import itertools
from typing import Any, Callable, Dict, List, Optional

import pytest

from volnux.event.agent import AgentEventBase
from volnux.event.agent.providers import LLMProviderAdapterBase, LLMProviderRegistry
from volnux.signal import signals

_names = itertools.count()


class ScriptedModel:
    """What the model will say, turn by turn, and what it was asked."""

    def __init__(self) -> None:
        self.script: List[Any] = []
        self.calls: List[Dict[str, Any]] = []

    def say(self, *turns: Any) -> "ScriptedModel":
        self.script.extend(turns)
        return self

    def next(self, **request: Any) -> Dict[str, Any]:
        self.calls.append(request)
        if not self.script:
            raise AssertionError("the model was asked for more turns than scripted")
        turn = self.script.pop(0)
        if isinstance(turn, BaseException):
            raise turn
        if isinstance(turn, str):
            turn = {"content": turn}
        turn = dict(turn)
        return LLMProviderAdapterBase.build_response(
            turn.pop("content", ""),
            prompt_tokens=turn.pop("prompt", 10),
            completion_tokens=turn.pop("completion", 5),
            **turn,
        )


@pytest.fixture
def model() -> ScriptedModel:
    return ScriptedModel()


@pytest.fixture
def provider(model: ScriptedModel):
    """A provider name whose adapter answers from ``model``."""
    name = "scripted%d" % next(_names)

    class Adapter(LLMProviderAdapterBase):
        provider_name = name

        async def complete(self, messages, model_name, temperature, max_tokens, tools=None, **kw):  # type: ignore[override]
            raise NotImplementedError

    async def complete(
        self, messages, model, temperature, max_tokens, tools=None, **kw
    ):
        return model_script.next(
            messages=messages,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            tools=tools,
        )

    model_script = model
    Adapter.complete = complete  # type: ignore[method-assign]
    yield name
    LLMProviderRegistry.unregister(name)


@pytest.fixture
def make_agent(
    build: Callable[..., Any], provider: str
) -> Callable[..., AgentEventBase]:
    """``make_agent(**class_attrs)`` -> an agent instance on the scripted model."""

    async def process(self: Any, **kwargs: Any):
        return await self._run_agent_loop(**kwargs)

    def build_prompt(self: Any, **kwargs: Any) -> str:
        return "the task"

    def factory(base: type = AgentEventBase, **attrs: Any) -> AgentEventBase:
        attrs.setdefault("llm_provider", provider)
        attrs.setdefault("system_prompt", "You are a test agent.")
        attrs.setdefault("llm_retry_delay_s", 0.0)
        attrs.setdefault("process", process)
        attrs.setdefault("build_prompt", build_prompt)
        cls = type("Agent%d" % next(_names), (base,), attrs)
        return build(cls)

    return factory


@pytest.fixture
def agent_signals():
    """Record the agent's soft signals: ``{name: [kwargs, ...]}``."""
    names = [
        "agent_execution_start",
        "agent_step_complete",
        "agent_tool_call_start",
        "agent_tool_call_end",
        "agent_step_limit_exceeded",
        "agent_hallucination_detected",
        "agent_governance_violation",
        "agent_protocol_violation",
        "agent_execution_end",
    ]
    log: Dict[str, List[Dict[str, Any]]] = {n: [] for n in names}
    keep = []
    for name in names:

        def make(name: str) -> Callable[..., None]:
            def listener(sender: Any = None, signal: Any = None, **kwargs: Any) -> None:
                log[name].append(kwargs)

            return listener

        fn = make(name)
        keep.append(fn)
        getattr(signals, name).connect(None, fn)
    yield log
    for name, fn in zip(names, keep):
        getattr(signals, name).disconnect(None, fn)
