import pytest

from volnux.parser.exceptions import PointyParseError
from volnux.parser.grammar import pointy_parser

from .scoping_support import build, by_name, extras, walk

SPEC = "{A -> B, {A, B -> {C, D}[a=$a]}[a=1]}[a=2]"


class TestSpecTrace:
    def test_each_task_sees_its_nearest_declaration(self):
        root = build(SPEC)
        a_outer, a_middle = by_name(root, "A")
        b_outer, b_middle = by_name(root, "B")
        (c,), (d,) = by_name(root, "C"), by_name(root, "D")
        assert extras(a_outer) == extras(b_outer) == {"a": 2}
        assert extras(a_middle) == extras(b_middle) == {"a": 1}
        assert extras(c) == extras(d) == {"a": 1}

    def test_the_groups_themselves(self):
        groups = [t for t in walk(build(SPEC)) if t.is_grouping]
        assert [extras(g) for g in groups] == [{"a": 2}, {"a": 1}, {"a": 1}]

    def test_the_inner_value_reads_the_enclosing_scope_not_its_own(self):
        # [a=$a] is where $a is used; it finds the enclosing a=7
        groups = [g for g in walk(build("{ {C}[a=$a] }[a=7]")) if g.is_grouping]
        assert [extras(g) for g in groups] == [{"a": 7}, {"a": 7}]


class TestResolutionOrder:
    def test_local_shadows_global(self):
        root = build("@a = 5\n{ T[x=$a] }[a=1]")
        (t,) = by_name(root, "T")
        assert extras(t)["x"] == 1

    def test_global_when_nothing_encloses(self):
        (t,) = by_name(build("@a = 5\nT[x=$a]"), "T")
        assert extras(t) == {"x": 5}

    def test_globals_are_evaluated_outside_the_blocks(self):
        # @total reads $base; a block declaring `base` must not change @total
        root = build("@base = 2\n@total = $base * 10\n{ T[x=$total] }[base=100]")
        (t,) = by_name(root, "T")
        assert extras(t)["x"] == 20

    def test_nearest_of_several_enclosing_blocks(self):
        (t,) = by_name(build("{ { { T[x=$a] }[b=3] }[a=2] }[a=1]"), "T")
        assert extras(t)["x"] == 2

    def test_keys_of_one_bracket_do_not_see_each_other(self):
        with pytest.raises(SyntaxError, match="'a' is not declared"):
            pointy_parser("{ T }[a=1, b=$a]")

    def test_a_task_annotation_does_not_leak_to_the_next_task(self):
        with pytest.raises(SyntaxError, match="'k' is not declared"):
            pointy_parser("A[k=1] -> B[x=$k]")

    def test_a_task_annotation_does_not_reach_its_branches(self):
        with pytest.raises(SyntaxError, match="'k' is not declared"):
            pointy_parser("A[k=1] (success -> B[x=$k])")

    def test_block_scope_reaches_branches_inside_it(self):
        root = build("{ A (success -> B[x=$k]) }[k=4]")
        (b,) = by_name(root, "B")
        assert extras(b)["x"] == 4

    def test_expressions_use_scoped_values(self):
        (t,) = by_name(build("{ T[x=$n * 2 + 1] }[n=5]"), "T")
        assert extras(t)["x"] == 11

    def test_index_into_a_scoped_list(self):
        (t,) = by_name(build("{ T[x=$xs[1]] }[xs=[10, 20, 30]]"), "T")
        assert extras(t)["x"] == 20

    def test_index_into_a_global_list_still_works(self):
        (t,) = by_name(build("@xs = [10, 20]\nT[x=$xs[0]]"), "T")
        assert extras(t)["x"] == 10


class TestUnresolved:
    @pytest.mark.parametrize(
        "source",
        [
            "A[x=$nope]",
            "{ A[x=$nope] }[a=1]",
            "{ A }[a=$nope]",
            "A (success -> B[x=$nope])",
            "A[x=$nope + 1]",
            "A[x=$nope ?? 1]",
            "A[x=[1, $nope]]",
            "A * 2 -> B[x=$nope]",
            "MAP<T>[x=$nope]",
        ],
    )
    def test_error_at_parse_time(self, source):
        with pytest.raises(
            SyntaxError, match="Variable 'nope' is not declared in any enclosing scope"
        ):
            pointy_parser(source)

    def test_the_environment_is_never_consulted_implicitly(self, monkeypatch):
        monkeypatch.setenv("a", "1")
        monkeypatch.setenv("A", "1")
        with pytest.raises(SyntaxError, match="'a' is not declared"):
            pointy_parser("T[x=$a]")

    def test_explicit_env_still_works(self, monkeypatch):
        monkeypatch.setenv("LIMIT", "9")
        (t,) = by_name(build("T[x=$env.LIMIT]"), "T")
        assert extras(t)["x"] == "9"

    def test_inner_variable_is_not_visible_outside(self):
        with pytest.raises(SyntaxError, match="'inner' is not declared"):
            pointy_parser("{ A }[inner=1] -> B[x=$inner]")


class TestCascade:
    def test_extras_known_options_and_variables_flow_down(self):
        root = build('{ A -> B }[executor="pool", bypass_event_checks=true, tag="x"]')
        for task in (*by_name(root, "A"), *by_name(root, "B")):
            assert task.options.executor == "pool"
            assert task.options.bypass_event_checks is True
            assert extras(task) == {"tag": "x"}

    def test_retry_attempts_does_not_cascade(self):
        # it retries the group as a unit; cascading would multiply the attempts
        root = build("{ A -> B }[retry_attempts=3]")
        group = walk(root)[0]
        assert group.options.retry_attempts == 3
        for task in (*by_name(root, "A"), *by_name(root, "B")):
            assert task.options is None

    def test_group_level_options_do_not_cascade(self):
        root = build(
            '{ A, B }[max_concurrency=2, result_evaluation_strategy="ALL_MUST_SUCCEED"]'
        )
        for task in (*by_name(root, "A"), *by_name(root, "B")):
            assert task.options is None

    def test_retry_attempts_is_still_a_variable(self):
        (t,) = by_name(
            build("{ A[retry_attempts=$retry_attempts] }[retry_attempts=4]"), "A"
        )
        assert t.options.retry_attempts == 4

    def test_own_option_beats_the_cascaded_one(self):
        (a,) = by_name(build('{ A[executor="mine"] }[executor="outer"]'), "A")
        assert a.options.executor == "mine"

    def test_inner_block_beats_outer_block(self):
        (a,) = by_name(build('{ { A }[executor="inner"] }[executor="outer"]'), "A")
        assert a.options.executor == "inner"

    def test_explicit_retry_beats_everything(self):
        (a,) = by_name(build("{ A * 3 }[retry_attempts=9]"), "A")
        assert a.options.retry_attempts == 3

    def test_no_options_stays_none(self):
        assert all(t.options is None for t in walk(build("A -> {B, C} -> D")))

    def test_tasks_outside_a_block_get_nothing(self):
        root = build("{ A }[tag=1] -> B")
        (b,) = by_name(root, "B")
        assert b.options is None

    def test_meta_task_keeps_its_template_and_gets_the_cascade(self):
        root = build('{ MAP<T>[size=2] }[executor="pool"]')
        (m,) = by_name(root, "MAP")
        assert m.options.extras["template_class"] == "T"
        assert m.options.extras["size"] == 2 and m.options.executor == "pool"


class TestCodeGenErrors:
    def test_unbound_name_reaching_code_generation(self):
        # (check_scopes catches this at parse time; the generator is the backstop)
        from volnux.parser.ast import VariableAccessNode
        from volnux.parser.code_gen import ExecutableASTGenerator
        from volnux.task import PipelineTask, PipelineTaskGrouping

        generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
        with pytest.raises(PointyParseError, match="not declared"):
            generator.visit_variable_access(VariableAccessNode("x", None))
