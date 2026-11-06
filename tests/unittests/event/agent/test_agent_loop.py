"""The governed agent loop, driven by a scripted model."""

import asyncio
from typing import Any

import pytest

from volnux.event.agent import ChainOfThoughtAgentEventBase, ReActAgentEventBase
from volnux.event.agent._state import AgentAction, ReasoningStep, ToolCallRecord
from volnux.exceptions import LLMProviderError, SwitchTask


def native(name: str, args: Any = None, call_id: str = "call-1", content: str = ""):
    return {
        "content": content,
        "tool_name": name,
        "tool_args": args or {},
        "tool_call_id": call_id,
    }


# --- finishing --------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_finish_directive_ends_the_run_with_the_answer(make_agent, model):
    agent = make_agent()
    model.say("FINISH: all done")

    ok, result = await agent.process()

    assert ok is True
    assert result.success and result.content == "all done"  # directive stripped
    assert result.steps_taken == 1
    assert result.total_tokens == 15
    assert result.metadata["llm_model"] == "gpt-4o"


@pytest.mark.asyncio
async def test_the_model_is_called_with_the_governed_prompt_and_the_task(
    make_agent, model
):
    agent = make_agent(tools=[])
    model.say("FINISH: x")

    await agent.process()

    request = model.calls[0]
    system, user = request["messages"][:2]
    assert system["role"] == "system" and "VOLNUX GOVERNANCE" in system["content"]
    assert "You are a test agent." in system["content"]
    assert user == {"role": "user", "content": "the task"}
    assert request["tools"] is None  # no tools, no schemas


@pytest.mark.asyncio
async def test_a_class_attribute_is_the_default_not_the_schema(make_agent, model):
    """The schema's defaults (gpt-4o, 20 steps ...) used to overwrite what a
    subclass declared as class attributes."""
    agent = make_agent(llm_model="custom-model", max_reasoning_steps=3, temperature=0.4)
    model.say("FINISH: x")

    await agent.process()

    assert model.calls[0]["model"] == "custom-model"
    assert model.calls[0]["temperature"] == 0.4
    assert agent.max_reasoning_steps == 3


@pytest.mark.asyncio
async def test_thinking_continues_and_the_runtime_answers_each_think(make_agent, model):
    agent = make_agent()
    model.say("THINK: let me see", "FINISH: ok")

    ok, result = await agent.process()

    assert ok and result.steps_taken == 2
    roles = [m["role"] for m in model.calls[1]["messages"]]
    # system, task, the model's thought, then a runtime turn: never ends on assistant
    assert roles == ["system", "user", "assistant", "user"]


@pytest.mark.asyncio
async def test_an_error_directive_fails_the_run(make_agent, model):
    agent = make_agent()
    model.say("ERROR: FATAL | cannot continue")

    ok, result = await agent.process()

    assert ok is False and result.content == "cannot continue"
    assert result.metadata["error_code"] == "FATAL"


# --- protocol ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_response_without_a_directive_is_corrected_and_the_run_goes_on(
    make_agent, model, agent_signals
):
    agent = make_agent()
    model.say("Sure! Happy to help.", "FINISH: ok")

    ok, result = await agent.process()

    assert ok and result.content == "ok"
    assert result.metadata["protocol_violations"] == 1
    correction = model.calls[1]["messages"][-1]
    assert (
        correction["role"] == "user" and "PROTOCOL VIOLATION" in correction["content"]
    )
    assert agent_signals["agent_protocol_violation"][0]["count"] == 1


@pytest.mark.asyncio
async def test_two_directives_in_one_response_are_a_violation(make_agent, model):
    agent = make_agent()
    model.say("TOOL: Anything | {}\nFINISH: and done", "FINISH: fine")

    ok, result = await agent.process()

    assert ok and result.metadata["protocol_violations"] == 1
    assert result.metadata["tool_calls"] == 0  # the first directive was not acted on


@pytest.mark.asyncio
async def test_too_many_violations_stop_the_agent(make_agent, model):
    agent = make_agent(max_protocol_violations=2)
    model.say("a", "b", "c")

    ok, result = await agent.process()

    assert ok is False
    assert "protocol" in result.content and len(model.calls) == 3


@pytest.mark.asyncio
async def test_lenient_mode_takes_plain_text_as_a_thought(make_agent, model):
    agent = make_agent(strict_protocol=False)
    model.say("just chatting", "FINISH: ok")

    ok, result = await agent.process()

    assert ok and result.metadata["protocol_violations"] == 0
    assert result.steps_taken == 2


# --- limits ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_step_limit_ends_a_run_that_never_finishes(
    make_agent, model, agent_signals
):
    agent = make_agent(max_reasoning_steps=2)
    model.say("THINK: a", "THINK: b")

    ok, result = await agent.process()

    assert ok is False and "max_reasoning_steps=2" in result.content
    assert len(agent_signals["agent_step_limit_exceeded"]) == 1


@pytest.mark.asyncio
async def test_the_token_budget_is_enforced(make_agent, model):
    """max_total_tokens used to be declared in the schema and never read."""
    agent = make_agent(max_total_tokens=20)
    model.say("THINK: a", "THINK: b", "FINISH: never reached")  # 15 tokens each

    ok, result = await agent.process()

    assert ok is False and "max_total_tokens=20" in result.content
    assert len(model.calls) == 2


@pytest.mark.asyncio
async def test_long_conversations_are_trimmed_without_orphaning_tool_results(
    make_agent, model
):
    agent = make_agent(max_context_messages=6, max_reasoning_steps=10)
    model.say(*["THINK: step"] * 6, "FINISH: done")

    ok, _ = await agent.process()

    assert ok
    last = model.calls[-1]["messages"]
    assert len(last) <= 6
    assert last[0]["role"] == "system" and last[1]["content"] == "the task"
    assert any("CONTEXT TRIMMED" in m["content"] for m in last)


# --- the model failing -----------------------------------------------------


@pytest.mark.asyncio
async def test_a_failing_provider_is_retried_then_succeeds(make_agent, model):
    agent = make_agent()
    model.say(LLMProviderError("rate limited"), "FINISH: ok")

    ok, result = await agent.process()

    assert ok and result.content == "ok" and len(model.calls) == 2


@pytest.mark.asyncio
async def test_a_provider_that_keeps_failing_fails_the_run(make_agent, model):
    agent = make_agent(llm_max_retries=2)
    model.say(LLMProviderError("down"), LLMProviderError("still down"))

    ok, result = await agent.process()

    assert ok is False and "failed after 2 attempts" in result.content


@pytest.mark.asyncio
async def test_an_error_that_cannot_succeed_is_not_retried(make_agent, model):
    agent = make_agent(llm_max_retries=5)
    model.say(LLMProviderError("bad key", retryable=False), "FINISH: unreachable")

    ok, result = await agent.process()

    assert ok is False and "bad key" in result.content
    assert len(model.calls) == 1


@pytest.mark.asyncio
async def test_control_flow_exceptions_are_not_swallowed(make_agent, model, provider):
    """A SwitchTask is how the workflow is steered; it is not a failed agent."""
    agent = make_agent()
    model.say(SwitchTask("t", 1, None))

    with pytest.raises(SwitchTask):
        await agent.process()


# --- tools -------------------------------------------------------------------


def make_tool(make_event, **attrs):
    calls = []

    async def process(self, **kwargs):
        calls.append(kwargs)
        return True, {"echo": kwargs}

    return make_event(process=process, **attrs), calls


@pytest.mark.asyncio
async def test_a_tool_directive_runs_the_tool_with_its_arguments(
    make_agent, make_event, model
):
    tool, calls = make_tool(make_event)
    agent = make_agent(tools=[tool])
    model.say(f'TOOL: {tool.__name__} | {{"q": "x"}}', "FINISH: ok")

    ok, result = await agent.process()

    assert ok and calls == [{"q": "x"}]  # the arguments used to be passed positionally
    tool_message = model.calls[1]["messages"][-1]
    assert tool_message["role"] == "tool" and "echo" in tool_message["content"]
    request = model.calls[1]["messages"][-2]
    assert request["role"] == "assistant"
    assert request["tool_calls"][0]["name"] == tool.__name__
    assert request["tool_calls"][0]["arguments"] == {"q": "x"}
    assert tool_message["tool_call_id"] == request["tool_calls"][0]["id"]


@pytest.mark.asyncio
async def test_a_native_function_call_runs_the_tool_too(make_agent, make_event, model):
    tool, calls = make_tool(make_event)
    agent = make_agent(tools=[tool])
    model.say(native(tool.__name__, {"n": 1}, call_id="abc"), "FINISH: ok")

    ok, _ = await agent.process()

    assert ok and calls == [{"n": 1}]
    assert model.calls[1]["messages"][-1]["tool_call_id"] == "abc"


@pytest.mark.asyncio
async def test_the_tools_schema_is_offered_to_the_model(make_agent, make_event, model):
    tool, _ = make_tool(
        make_event,
        INIT_PARAMS_SCHEMA={
            "q": {"type": str, "required": True, "description": "query"}
        },
    )
    agent = make_agent(tools=[tool])
    model.say("FINISH: x")

    await agent.process()

    schema = model.calls[0]["tools"][0]
    assert schema["name"] == tool.__name__
    assert schema["parameters"]["properties"]["q"] == {
        "type": "string",
        "description": "query",
    }
    assert schema["parameters"]["required"] == ["q"]


@pytest.mark.asyncio
async def test_a_call_to_an_undeclared_tool_is_a_governance_violation(
    make_agent, make_event, model, agent_signals
):
    declared, _ = make_tool(make_event)
    agent = make_agent(tools=[declared])
    model.say("TOOL: Sneaky | {}")

    ok, result = await agent.process()

    assert ok is False and "undeclared tool 'Sneaky'" in result.content
    assert len(agent_signals["agent_governance_violation"]) == 1
    assert not any(m.get("tool_calls") for m in agent._messages)  # never recorded


@pytest.mark.asyncio
async def test_a_failing_tool_is_reported_to_the_model_which_can_react(
    make_agent, make_event, model
):
    async def process(self, **kwargs):
        raise ValueError("boom")

    tool = make_event(process=process)
    agent = make_agent(tools=[tool])
    model.say(f"TOOL: {tool.__name__} | {{}}", "FINISH: handled")

    ok, result = await agent.process()

    assert ok and result.content == "handled"
    assert "error" in model.calls[1]["messages"][-1]["content"]
    assert agent._tool_call_records[0]["error"]


@pytest.mark.asyncio
async def test_a_tool_that_takes_too_long_times_out(make_agent, make_event, model):
    async def process(self, **kwargs):
        await asyncio.sleep(5)
        return True, "late"

    tool = make_event(process=process)
    agent = make_agent(tools=[tool], tool_timeout_seconds=0.05)
    model.say(f"TOOL: {tool.__name__} | {{}}", "FINISH: moved on")

    ok, _ = await agent.process()

    assert ok
    assert "timed out" in model.calls[1]["messages"][-1]["content"]


@pytest.mark.asyncio
async def test_a_tool_can_run_on_a_thread_pool(make_agent, make_event, model):
    """Tools use their own executor, as in a flow."""
    tool, calls = make_tool(make_event, executor="thread")
    agent = make_agent(tools=[tool])
    model.say(f'TOOL: {tool.__name__} | {{"k": 1}}', "FINISH: ok")

    ok, _ = await agent.process()

    assert ok and calls == [{"k": 1}]


@pytest.mark.asyncio
async def test_the_tool_and_its_result_are_in_the_trace_and_records(
    make_agent, make_event, model
):
    tool, _ = make_tool(make_event)
    agent = make_agent(tools=[tool], include_trace_in_result=True)
    model.say(f"TOOL: {tool.__name__} | {{}}", "FINISH: ok")

    _, result = await agent.process()

    kinds = [s.step_type.value for s in result.reasoning_trace]
    assert kinds == [
        "system_prompt",
        "user_message",
        "tool_request",
        "tool_result",
        "final_answer",
    ]
    assert all(isinstance(s, ReasoningStep) for s in result.reasoning_trace)
    assert agent._tool_call_records[0]["tool_class_name"] == tool.__name__
    assert agent._tool_call_records[0]["completed_at"]


# --- handoff ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_handoff_to_a_permitted_agent_ends_the_run(make_agent, model):
    agent = make_agent(permitted_handoffs=["Reviewer"])
    model.say('HANDOFF: Reviewer | {"case": 7}')

    ok, result = await agent.process()

    assert ok and result.handoff_target == "Reviewer"
    assert result.metadata["handoff_context"] == {"case": 7}


@pytest.mark.asyncio
async def test_a_handoff_to_an_unlisted_agent_is_refused(
    make_agent, model, agent_signals
):
    agent = make_agent(permitted_handoffs=["Reviewer"])
    model.say("HANDOFF: Other | {}")

    ok, result = await agent.process()

    assert ok is False and "undeclared handoff to 'Other'" in result.content
    assert len(agent_signals["agent_governance_violation"]) == 1


@pytest.mark.asyncio
async def test_an_agent_with_no_permitted_handoffs_may_hand_over_to_nobody(
    make_agent, model
):
    """An empty list used to disable the check, permitting every target."""
    agent = make_agent()
    model.say("HANDOFF: Anyone | {}")

    ok, result = await agent.process()

    assert ok is False and "undeclared handoff" in result.content


# --- hallucination -------------------------------------------------------------


@pytest.mark.asyncio
async def test_domain_hallucination_detection_stops_the_agent(
    make_agent, model, agent_signals
):
    agent = make_agent(
        enable_hallucination_detection=True,
        detect_hallucination=lambda self, response, context: "forbidden"
        not in response["content"],
    )
    model.say("FINISH: a forbidden answer")

    ok, result = await agent.process()

    assert ok is False and "Domain validation failed" in result.content
    assert len(agent_signals["agent_hallucination_detected"]) == 1


@pytest.mark.asyncio
async def test_an_async_detector_is_awaited(make_agent, model):
    async def detect(self, response, context):
        return True

    agent = make_agent(enable_hallucination_detection=True, detect_hallucination=detect)
    model.say("FINISH: fine")

    ok, _ = await agent.process()

    assert ok


# --- signals, hooks ------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_lifecycle_is_announced_by_signals(make_agent, model, agent_signals):
    agent = make_agent()
    model.say("THINK: a", "FINISH: b")

    await agent.process()

    assert len(agent_signals["agent_execution_start"]) == 1
    assert [e["step_index"] for e in agent_signals["agent_step_complete"]] == [0, 1]
    end = agent_signals["agent_execution_end"][0]
    assert end["success"] is True and end["steps_taken"] == 2


@pytest.mark.asyncio
async def test_hooks_see_steps_tool_records_and_the_result(
    make_agent, make_event, model
):
    seen = {"steps": [], "records": [], "result": []}
    tool, _ = make_tool(make_event)
    agent = make_agent(
        tools=[tool],
        on_step_complete=lambda self, step: seen["steps"].append(step),
        on_tool_result=lambda self, record: seen["records"].append(record),
        on_agent_complete=lambda self, result: seen["result"].append(result),
    )
    model.say(f"TOOL: {tool.__name__} | {{}}", "FINISH: ok")

    await agent.process()

    assert len(seen["steps"]) == 2 and isinstance(seen["steps"][0], ReasoningStep)
    assert isinstance(seen["records"][0], ToolCallRecord)
    assert seen["result"][0].content == "ok"


@pytest.mark.asyncio
async def test_a_hook_that_raises_does_not_stop_the_agent(make_agent, model):
    def broken(self, *args):
        raise RuntimeError("hook bug")

    agent = make_agent(on_step_complete=broken, on_agent_complete=broken)
    model.say("FINISH: ok")

    ok, _ = await agent.process()

    assert ok


# --- state, checkpoint ---------------------------------------------------------


@pytest.mark.asyncio
async def test_the_run_state_is_plain_data_the_checkpoint_can_hold(
    make_agent, make_event, model
):
    import json

    tool, _ = make_tool(make_event)
    agent = make_agent(tools=[tool])
    model.say(f"TOOL: {tool.__name__} | {{}}", "FINISH: ok")

    await agent.process()

    for name in (
        "_messages",
        "_reasoning_trace",
        "_tool_call_records",
        "_total_tokens",
    ):
        json.dumps(getattr(agent, name))  # would raise on a dataclass


@pytest.mark.asyncio
async def test_a_resumed_agent_continues_from_its_restored_conversation(
    make_agent, model
):
    agent = make_agent()
    # what a checkpoint restore puts back: one step already taken
    agent._messages = [
        {"role": "system", "content": "SYS"},
        {"role": "user", "content": "the task"},
        {"role": "assistant", "content": "THINK: halfway"},
        {"role": "user", "content": "OBSERVATION: Continue with your next directive."},
    ]
    agent._step_index = 1
    agent._total_tokens = {"prompt": 10, "completion": 5, "total": 15}
    model.say("FINISH: resumed")

    ok, result = await agent.process()

    assert ok and result.content == "resumed"
    assert model.calls[0]["messages"][0]["content"] == "SYS"  # not rebuilt
    assert len(model.calls) == 1 and result.steps_taken == 2
    assert result.total_tokens == 30  # carried over


@pytest.mark.asyncio
async def test_a_tool_call_interrupted_by_a_crash_is_answered_on_resume(
    make_agent, model
):
    """The history must not end on a tool request with no result: providers reject it."""
    agent = make_agent()
    agent._messages = [
        {"role": "system", "content": "SYS"},
        {"role": "user", "content": "the task"},
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [{"id": "c1", "name": "T", "arguments": {}}],
        },
    ]
    agent._step_index = 1
    model.say("FINISH: ok")

    await agent.process()

    sent = model.calls[0]["messages"]
    assert sent[-1]["role"] == "tool" and sent[-1]["tool_call_id"] == "c1"
    assert "interrupted" in sent[-1]["content"]


@pytest.mark.asyncio
async def test_pause_clears_the_gate_and_resume_continues(make_agent, model):
    agent = make_agent()
    model.say("PAUSE: waiting for approval", "FINISH: approved")

    task = asyncio.create_task(agent.process())
    for _ in range(100):
        await asyncio.sleep(0.01)
        if not agent._pause_gate.is_set():
            break
    assert not agent._pause_gate.is_set() and not task.done()

    agent._pause_gate.set()  # the coordinator's RESUME
    ok, result = await asyncio.wait_for(task, 5)

    assert ok and result.content == "approved"


# --- variants ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_react_accepts_thought_lines_before_the_action(make_agent, model):
    agent = make_agent(ReActAgentEventBase)
    model.say("Thought: I know this.\nAction: FINISH: 42")

    ok, result = await agent.process()

    assert ok and result.content == "42"
    assert "Thought:" in model.calls[0]["messages"][0]["content"]


@pytest.mark.asyncio
async def test_react_still_refuses_two_actions(make_agent, make_event, model):
    tool, _ = make_tool(make_event)
    agent = make_agent(ReActAgentEventBase, tools=[tool])
    model.say(
        f"Thought: x\nAction: TOOL: {tool.__name__} | {{}}\nAction: TOOL: {tool.__name__} | {{}}",
        "Action: FINISH: ok",
    )

    ok, result = await agent.process()

    assert (
        ok
        and result.metadata["protocol_violations"] == 1
        and result.metadata["tool_calls"] == 0
    )


@pytest.mark.asyncio
async def test_chain_of_thought_rejects_tools(make_agent, make_event):
    tool, _ = make_tool(make_event)
    agent = make_agent(ChainOfThoughtAgentEventBase, tools=[tool])

    with pytest.raises(TypeError, match="does not support tools"):
        await agent.process()


@pytest.mark.asyncio
async def test_a_custom_parse_action_still_decides(make_agent, model):
    agent = make_agent(parse_action=lambda self, response: AgentAction.FINISH)
    model.say("whatever the model says")

    ok, result = await agent.process()

    assert ok and result.content == "whatever the model says"


@pytest.mark.asyncio
async def test_a_misconfigured_agent_raises_before_calling_the_model(make_agent):
    agent = make_agent(system_prompt="")

    with pytest.raises(TypeError, match="no system_prompt"):
        await agent.process()


def test_handoff_to_validates_the_target(make_agent):
    from volnux.exceptions import ToolPermissionError

    agent = make_agent(permitted_handoffs=["Reviewer"])

    with pytest.raises(ToolPermissionError):
        agent.handoff_to("Other", {})

    with pytest.raises(SwitchTask):  # goto() routes the graph
        agent.handoff_to("Reviewer", {"k": 1})
