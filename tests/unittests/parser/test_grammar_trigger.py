import pytest

from volnux.parser import pointy_parser
from volnux.parser.ast import CallNode, ComparisonExprNode, LiteralNode, TriggerNode
from volnux.parser.sema import Sema


def lit(node):
    return node.value if isinstance(node, LiteralNode) else node


SPEC = """\
@trigger: schedule("0 6 * * MON-FRI", timezone="America/New_York")
@trigger: webhook("/hooks/trade-alert")
@trigger: when file_exists("/data/input.csv")
A -> B
"""


class TestTriggerDeclarations:
    def test_the_specification_example(self):
        program = pointy_parser(SPEC)
        schedule, webhook, when = program.triggers
        assert [t.kind for t in program.triggers] == ["schedule", "webhook", "when"]
        assert lit(schedule.args[0]) == "0 6 * * MON-FRI"
        assert lit(schedule.kwargs["timezone"]) == "America/New_York"
        assert lit(webhook.args[0]) == "/hooks/trade-alert"
        assert isinstance(when.condition, CallNode)
        assert when.condition.name == "file_exists"
        assert lit(when.condition.args[0]) == "/data/input.csv"
        assert program.chain is not None

    def test_triggers_are_not_directives(self):
        program = pointy_parser(SPEC)
        assert "trigger" not in program.directives

    def test_source_positions(self):
        program = pointy_parser(SPEC)
        assert [t.line for t in program.triggers] == [1, 2, 3]
        assert program.triggers[0].column == 1

    def test_expression_condition(self):
        (trigger,) = pointy_parser("@trigger: when $env.QUEUE_DEPTH > 100\nA").triggers
        assert isinstance(trigger.condition, ComparisonExprNode)
        assert trigger.condition.operator == ">"

    def test_parenthesised_condition(self):
        (trigger,) = pointy_parser("@trigger: when ($env.X == 1)\nA").triggers
        assert isinstance(trigger.condition, ComparisonExprNode)

    def test_no_arguments_and_keyword_only(self):
        a, b = pointy_parser(
            '@trigger: webhook()\n@trigger: schedule(timezone="UTC")\nA'
        ).triggers
        assert a.args == [] and a.kwargs == {}
        assert b.args == [] and list(b.kwargs) == ["timezone"]

    def test_mixed_with_other_declarations_in_any_order(self):
        program = pointy_parser(
            '@mode: "CFG"\n@trigger: webhook("/a")\n@descriptor: late\n'
            '@trigger: when file_exists("/x")\n@n = 3\nA -> B'
        )
        assert [t.kind for t in program.triggers] == ["webhook", "when"]
        assert program.descriptors == {"late": 2}
        assert "n" in program.global_variables

    def test_chain_in_parentheses_right_after_a_trigger(self):
        program = pointy_parser('@trigger: webhook("/a")\n(A -> B)')
        assert len(program.triggers) == 1 and program.chain is not None

    def test_program_without_triggers(self):
        assert pointy_parser("A -> B").triggers == []


class TestEventTrigger:
    SRC = (
        '@trigger: event("order.created", "order.updated", '
        'pattern={"status": "paid"}, name="orders")\nA'
    )

    def test_parses_types_pattern_and_name(self):
        (t,) = pointy_parser(self.SRC).triggers
        assert t.kind == "event" and [lit(a) for a in t.args] == [
            "order.created",
            "order.updated",
        ]
        assert set(t.kwargs) == {"pattern", "name"} and t.is_static

    def test_a_variable_pattern_is_dynamic_with_env(self):
        (t,) = pointy_parser('@trigger: event("a", pattern={"k": $env.K})\nA').triggers
        assert not t.is_static

    def test_clean(self):
        assert Sema().analyse(pointy_parser(self.SRC))

    @pytest.mark.parametrize(
        "source, fragment",
        [
            ("@trigger: event()\nA", "at least 1"),
            ("@trigger: event(5)\nA", "must be a str"),
            ('@trigger: event("")\nA', "must not be empty"),
            ('@trigger: event("a", pattern="x")\nA', "must be a map"),
            ('@trigger: event("a", foo=1)\nA', "unknown argument 'foo'"),
            ('@trigger: event("a")\n@trigger: event("b")\nA', "Duplicate"),
            ('@trigger: schedule({"a": 1})\nA', "must not be a map"),
        ],
    )
    def test_errors(self, source, fragment):
        result = Sema().analyse(pointy_parser(source))
        assert any(fragment in d.message for d in result.all), result.all


class TestStaticAndDynamic:
    def test_literals_are_static(self):
        a, b = pointy_parser(
            '@trigger: schedule("* * * * *")\n@trigger: webhook("/a")\nA'
        ).triggers
        assert a.is_static and b.is_static

    def test_declared_variables_are_static(self):
        (t,) = pointy_parser('@path = "/a"\n@trigger: webhook($path)\nA').triggers
        assert t.is_static

    def test_env_and_when_are_dynamic(self):
        a, b, c = pointy_parser(
            '@trigger: webhook($env.HOOK)\n@trigger: when file_exists("/x")\n'
            "@trigger: schedule($env.CRON)\nA"
        ).triggers
        assert not a.is_static and not b.is_static and not c.is_static

    def test_variable_holding_env_is_dynamic(self):
        (t,) = pointy_parser("@p = $env.HOOK\n@trigger: webhook($p)\nA").triggers
        assert not t.is_static


class TestSyntaxErrors:
    @pytest.mark.parametrize(
        "source",
        [
            '@trigger: schedule("* * * * *", "UTC", tz="x"\nA',  # unclosed
            '@trigger: schedule(timezone="UTC", "* * * * *")\nA',  # positional after keyword
            '@trigger: schedule("* * * * *", timezone="a", timezone="b")\nA',  # duplicate
            "@trigger: nothing\nA",  # no call, no `when`
            "@trigger:\nA",
            "@trigger: when\nA",
        ],
    )
    def test_rejected(self, source):
        with pytest.raises(SyntaxError):
            pointy_parser(source)


class TestWhenIsOnlyAKeywordInATrigger:
    def test_when_as_task_name(self):
        program = pointy_parser("when -> B")
        assert program.chain is not None

    def test_when_as_descriptor_label(self):
        program = pointy_parser("@descriptor: when\nA (when -> B)")
        assert "when" in program.descriptors

    def test_a_failed_parse_does_not_leak_the_keyword(self):
        with pytest.raises(SyntaxError):
            pointy_parser("@trigger: 5 5")
        # the next program must see `when` as an ordinary name again
        assert pointy_parser("when -> B").chain is not None


class TestSemantics:
    def analyse(self, source):
        return Sema().analyse(pointy_parser(source))

    def messages(self, source):
        result = self.analyse(source)
        return [d.message for d in result.all]

    def test_the_specification_example_is_clean(self):
        assert self.analyse(SPEC)

    @pytest.mark.parametrize(
        "source, fragment",
        [
            ('@trigger: cron("* * * * *")\nA', "Unknown trigger 'cron'"),
            ('@trigger: schedule("* * * *")\nA', "5 fields"),
            ('@trigger: schedule("* * * * $")\nA', "invalid cron"),
            ("@trigger: schedule(5)\nA", "must be a str"),
            ('@trigger: schedule("* * * * *", timezone="Mars/Base")\nA', "time zone"),
            ('@trigger: schedule("* * * * *", tz="UTC")\nA', "unknown argument 'tz'"),
            ("@trigger: schedule()\nA", "positional"),
            ('@trigger: webhook("hooks/a")\nA', "start with '/'"),
            ('@trigger: webhook("/a", secret="")\nA', "must not be empty"),
            ('@trigger: webhook("/a", tolerance_seconds=0)\nA', "positive"),
            ('@trigger: webhook("/a", tolerance_seconds="x")\nA', "int or float"),
            ('@trigger: webhook("/a")\n@trigger: webhook("/b")\nA', "Duplicate"),
            ('@trigger: when nope("x")\nA', "unknown condition function"),
            ("@trigger: when file_exists()\nA", "takes 1 positional"),
        ],
    )
    def test_errors(self, source, fragment):
        assert any(fragment in m for m in self.messages(source)), self.messages(source)

    def test_run_time_values_are_not_checked_early(self):
        assert not self.messages(
            "@trigger: webhook($env.HOOK)\n@trigger: schedule($env.CRON)\nA"
        )

    def test_variable_used_only_in_a_trigger_counts_as_used(self):
        assert not self.messages('@hook = "/a"\n@trigger: webhook($hook)\nA')

    def test_literal_checks_follow_a_variable(self):
        assert any(
            "start with '/'" in m
            for m in self.messages('@p = "bad"\n@trigger: webhook($p)\nA')
        )
