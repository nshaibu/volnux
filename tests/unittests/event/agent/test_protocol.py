"""The directive protocol, tool schemas and context trimming: pure functions."""

import pytest

from volnux.event.agent._state import AgentAction
from volnux.event.agent.protocol import (
    TRIM_MARKER,
    ProtocolViolation,
    build_tool_schemas,
    json_type,
    parse_directive,
    trim_messages,
)


def test_each_directive_token_maps_to_its_action():
    expected = {
        "THINK: hmm": AgentAction.THINK,
        "TOOL: Search | {}": AgentAction.TOOL_CALL,
        "HANDOFF: Other | {}": AgentAction.HANDOFF,
        "FINISH: done": AgentAction.FINISH,
        "ERROR: bad": AgentAction.ERROR,
        "PAUSE: wait": AgentAction.PAUSE,
    }
    for text, action in expected.items():
        assert parse_directive(text).action is action


def test_a_tool_directive_carries_the_name_and_json_arguments():
    d = parse_directive('TOOL: Search | {"q": "volnux", "n": 3}')
    assert d.tool_name == "Search" and d.tool_args == {"q": "volnux", "n": 3}


def test_tool_arguments_are_optional_and_may_be_fenced():
    assert parse_directive("TOOL: Ping").tool_args == {}
    fenced = parse_directive('tool: ns:Search | ```json\n{"q": 1}\n```')
    assert (fenced.tool_name, fenced.namespace, fenced.tool_args) == (
        "Search",
        "ns",
        {"q": 1},
    )


@pytest.mark.parametrize(
    "text",
    [
        "TOOL: Search | {not json}",
        'TOOL: Search | ["a"]',
        "TOOL: | {}",
        "HANDOFF: | {}",
    ],
)
def test_malformed_directives_are_violations(text):
    with pytest.raises(ProtocolViolation):
        parse_directive(text)


def test_a_handoff_carries_target_and_context():
    d = parse_directive('HANDOFF: Reviewer | {"id": 4}')
    assert d.handoff_target == "Reviewer" and d.handoff_context == {"id": 4}


def test_an_error_carries_a_code_when_there_is_one():
    assert parse_directive("ERROR: FATAL | disk full").error_code == "FATAL"
    assert parse_directive("ERROR: disk full").error_code is None


def test_text_without_a_directive_is_a_violation():
    with pytest.raises(ProtocolViolation, match="No directive"):
        parse_directive("Sure, I can help with that.")


def test_text_before_the_directive_is_a_violation_unless_allowed():
    text = "Let me think.\nFINISH: 4"
    with pytest.raises(ProtocolViolation, match="begin with a directive"):
        parse_directive(text)
    assert parse_directive(text, allow_preamble=True).content == "4"


def test_a_second_directive_is_a_violation():
    with pytest.raises(ProtocolViolation, match="More than one"):
        parse_directive("TOOL: A | {}\nTOOL: B | {}")


def test_a_finish_answer_may_quote_directive_looking_lines():
    d = parse_directive("FINISH: the log said:\nERROR: disk full\nso I stopped")
    assert d.action is AgentAction.FINISH and "ERROR: disk full" in d.content


def test_react_action_prefix_is_understood():
    assert parse_directive("Action: FINISH: yes", allow_preamble=True).content == "yes"


# --- tool schemas --------------------------------------------------------------


def test_tool_schemas_use_json_types_and_required_fields():
    class Search:
        """Search the web."""

        INIT_PARAMS_SCHEMA = {
            "q": {"type": str, "required": True, "description": "query"},
            "limit": {"type": int, "required": False},
            "exact": {"type": bool},
            "extra": {"type": object},
        }

    [schema] = build_tool_schemas([Search])

    assert schema["name"] == "Search" and schema["description"] == "Search the web."
    props = schema["parameters"]["properties"]
    assert props["q"]["type"] == "string" and props["limit"]["type"] == "integer"
    assert props["exact"]["type"] == "boolean" and props["extra"]["type"] == "string"
    assert schema["parameters"]["required"] == ["q"]


def test_json_type_accepts_json_names_and_python_types():
    assert json_type(float) == "number" and json_type("Integer") == "integer"
    assert json_type("weird") == "string" and json_type(None) == "string"


def test_no_tools_no_schemas():
    assert build_tool_schemas(None) == [] and build_tool_schemas([]) == []


# --- trimming --------------------------------------------------------------------


def conversation(n):
    msgs = [{"role": "system", "content": "S"}, {"role": "user", "content": "task"}]
    for i in range(n):
        msgs.append({"role": "assistant", "content": f"a{i}"})
        msgs.append({"role": "user", "content": f"u{i}"})
    return msgs


def test_a_short_conversation_is_returned_untouched():
    msgs = conversation(2)
    assert trim_messages(msgs, 50) is msgs


def test_trimming_keeps_the_system_prompt_the_task_and_the_newest():
    out = trim_messages(conversation(20), 10, step_index=20)

    assert len(out) == 10
    assert out[0]["content"] == "S" and out[1]["content"] == "task"
    assert out[2]["content"].startswith(TRIM_MARKER)
    assert out[-1]["content"] == "u19"


def test_trimming_never_starts_on_a_tool_result():
    msgs = [{"role": "system", "content": "S"}, {"role": "user", "content": "task"}]
    for i in range(6):
        msgs.append(
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": str(i), "name": "T", "arguments": {}}],
            }
        )
        msgs.append({"role": "tool", "tool_call_id": str(i), "content": "r"})

    for limit in range(4, 12):
        out = trim_messages(msgs, limit)
        # the first message kept after the marker (none, when only the marker fits)
        assert len(out) == 3 or out[3]["role"] != "tool", limit


def test_trimming_twice_keeps_one_marker():
    once = trim_messages(conversation(20), 10)
    once += conversation(5)[2:]
    twice = trim_messages(once, 10)

    assert sum(m["content"].startswith(TRIM_MARKER) for m in twice) == 1
