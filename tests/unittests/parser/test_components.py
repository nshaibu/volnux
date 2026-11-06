import pytest

from volnux.parser.grammar import pointy_parser
from volnux.parser.protocols import GroupingStrategy
from volnux.parser.sema import Sema

from .scoping_support import build, by_name, extras, walk


class TestGrammar:
    def test_definition_is_kept(self):
        program = pointy_parser("@component File: A -> B\nD -> File -> F")
        component = program.components["File"]
        assert component.name == "File" and component.line == 1
        assert component.options == []

    def test_defaults_and_main_chain_after_several_components(self):
        program = pointy_parser(
            "@component One[x=1]: A\n@component Two: B -> C\n\nD -> One -> Two"
        )
        assert list(program.components) == ["One", "Two"]
        assert [o.attr for o in program.components["One"].options] == ["x"]

    def test_invocation_becomes_a_reference_node(self):
        from volnux.parser.ast import ComponentRefNode

        program = pointy_parser("@component File: A\nD -> File[a=3] -> F")
        refs = []

        def find(n):
            if isinstance(n, ComponentRefNode):
                refs.append(n)
            for attr in ("left", "right"):
                if hasattr(n, attr):
                    find(getattr(n, attr))

        find(program.chain)
        assert [r.name for r in refs] == ["File"] and refs[0].options[0].attr == "a"

    def test_duplicate_name(self):
        with pytest.raises(SyntaxError, match="Duplicate component 'File'"):
            pointy_parser("@component File: A\n@component File: B\nD")

    def test_a_component_cannot_call_itself(self):
        # the name is registered after the body, so inside it File is an event
        program = pointy_parser("@component File: A -> File\nD -> File")
        body = program.components["File"].body
        assert body.right.__class__.__name__ == "TaskNode"

    def test_a_later_component_is_not_visible_to_an_earlier_one(self):
        program = pointy_parser("@component One: Two\n@component Two: B\nD -> One")
        assert program.components["One"].body.__class__.__name__ == "TaskNode"

    def test_component_as_meta_template_is_refused(self):
        with pytest.raises(
            SyntaxError, match="cannot be used as a meta event template"
        ):
            pointy_parser("@component File: A\nMAP<File>")

    def test_namespaced_task_is_never_a_component(self):
        program = pointy_parser("@component File: A\nother::File")
        assert program.chain.__class__.__name__ == "TaskNode"

    def test_a_task_named_like_a_component_before_its_definition_is_a_task(self):
        # components must be defined before the chain that uses them
        program = pointy_parser("@a = 1\n@component File: A\nFile")
        assert program.chain.__class__.__name__ == "ComponentRefNode"

    def test_component_keyword_is_only_a_directive(self):
        assert pointy_parser("component -> B").chain is not None


class TestExpansion:
    def test_the_spec_example(self):
        root = build("@component File: A -> B[a=$timeout]\nD -> File[timeout=30] -> F")
        (b,) = by_name(root, "B")
        assert extras(b)["a"] == 30
        assert [t.get_event_name() for t in walk(root)][:2] == ["D", "TaskGrouping"]

    def test_an_invocation_is_a_single_chain_group(self):
        root = build("@component File: A -> B\nD -> File -> F")
        group = [t for t in walk(root) if t.is_grouping][0]
        assert group.strategy is GroupingStrategy.SINGLE_CHAIN
        assert [t.get_event_name() for t in walk(group.chains[0])] == ["A", "B"]

    def test_sequencing_around_a_component(self):
        root = build("@component File: A -> B\nD -> File -> F")
        names = [t.get_event_name() for t in walk(root)]
        assert names == ["D", "TaskGrouping", "A", "B", "F"]

    def test_every_invocation_builds_its_own_tasks(self):
        root = build(
            "@component File: A[a=$v]\nD -> File[v=1] -> File[v=2] -> File[v=3]"
        )
        a_tasks = by_name(root, "A")
        assert [extras(t)["a"] for t in a_tasks] == [1, 2, 3]
        assert len({id(t) for t in a_tasks}) == 3

    def test_defaults_are_overridden_by_the_invocation(self):
        root = build("@component File[v=1]: A[a=$v]\nD -> File -> File[v=9]")
        assert [extras(t)["a"] for t in by_name(root, "A")] == [1, 9]

    def test_invocation_scope_is_discarded_afterwards(self):
        with pytest.raises(SyntaxError, match="'v' is not declared"):
            pointy_parser("@component File: A[a=$v]\nD -> File[v=1] -> F[x=$v]")

    def test_caller_scope_is_visible_inside(self):
        root = build("@component File: A[a=$limit]\n{ File }[limit=5]")
        (a,) = by_name(root, "A")
        assert extras(a)["a"] == 5

    def test_invocation_beats_the_caller_scope(self):
        root = build("@component File: A[a=$v]\n{ File[v=2] }[v=5]")
        (a,) = by_name(root, "A")
        assert extras(a)["a"] == 2

    def test_global_is_the_last_resort(self):
        root = build("@v = 7\n@component File: A[a=$v]\nFile")
        (a,) = by_name(root, "A")
        assert extras(a)["a"] == 7

    def test_invocation_variables_cascade_into_the_body(self):
        root = build("@component File: A -> B\nFile[tag=1]")
        for t in (*by_name(root, "A"), *by_name(root, "B")):
            assert extras(t) == {"tag": 1}

    def test_group_options_on_the_invocation_apply_to_the_group(self):
        root = build("@component File: A -> B\nFile[retry_attempts=2]")
        group = walk(root)[0]
        assert group.options.retry_attempts == 2
        assert by_name(root, "A")[0].options is None  # retry does not cascade

    def test_nested_components(self):
        root = build(
            "@component Inner: X[x=$v]\n@component Outer: Y -> Inner[v=$w]\n"
            "Outer[w=4]"
        )
        (x,) = by_name(root, "X")
        assert extras(x)["x"] == 4

    def test_component_with_branches_and_followers(self):
        root = build(
            "@component Check: A (success -> B, failure -> C)\n" "D -> Check -> F"
        )
        assert {"A", "B", "C", "F"} <= {t.get_event_name() for t in walk(root)}

    def test_conditional_on_a_component(self):
        root = build("@component File: A\nFile (success -> B, failure -> C)")
        assert {t.get_event_name() for t in walk(root)} >= {"A", "B", "C"}

    def test_retry_on_a_component(self):
        root = build("@component File: A\nFile * 3")
        assert walk(root)[0].options.retry_attempts == 3


class TestParameters:
    def test_missing_parameter_names_the_component_and_the_variable(self):
        with pytest.raises(
            SyntaxError,
            match=r"Variable 'p' \(used by component 'C'\) is not declared",
        ):
            pointy_parser("@component C: A[t=$p]\nD -> C")

    def test_parameter_supplied_by_the_invocation(self):
        pointy_parser("@component C: A[t=$p]\nD -> C[p=1]")

    def test_parameter_supplied_by_the_default(self):
        pointy_parser("@component C[p=1]: A[t=$p]\nD -> C")

    def test_parameter_supplied_by_a_global(self):
        pointy_parser("@p = 1\n@component C: A[t=$p]\nD -> C")

    def test_parameter_supplied_by_an_enclosing_block(self):
        pointy_parser("@component C: A[t=$p]\n{ C }[p=1]")

    def test_each_invocation_is_checked(self):
        with pytest.raises(SyntaxError, match="'p' \\(used by component 'C'\\)"):
            pointy_parser("@component C: A[t=$p]\nD -> C[p=1] -> C")

    def test_nested_component_parameter_must_come_from_somewhere(self):
        with pytest.raises(SyntaxError, match="'p'"):
            pointy_parser("@component In: A[t=$p]\n@component Out: In\nD -> Out")

    def test_nested_component_parameter_supplied_by_the_outer_invocation(self):
        pointy_parser("@component In: A[t=$p]\n@component Out: In\nD -> Out[p=1]")


class TestSemantics:
    def messages(self, source):
        return [d.message for d in Sema().analyse(pointy_parser(source)).all]

    def test_clean(self):
        assert not self.messages("@component File: A -> B\nD -> File -> F")

    def test_unused_component(self):
        assert any(
            "'File' is defined but never used" in m
            for m in self.messages("@component File: A\nD")
        )

    def test_component_used_only_by_another_counts_as_used(self):
        msgs = self.messages("@component In: A\n@component Out: In\nOut")
        assert not any("never used" in m and "'In'" in m for m in msgs)

    def test_name_of_a_source_namespace(self):
        msgs = self.messages('@source: local("File", "./f")\n@component File: A\nFile')
        assert any("has the name of a @source namespace" in m for m in msgs)

    def test_annotation_keys_that_are_read_are_not_unknown_options(self):
        assert not self.messages("{ A[retry_attempts=$level] }[level=2]")

    def test_annotation_keys_nobody_reads_still_warn(self):
        assert any(
            "Unknown option 'level'" in m for m in self.messages("{ A }[level=2]")
        )

    def test_component_default_keys_are_checked_like_any_annotation(self):
        msgs = self.messages("@component C[retry_attempts=-1]: A\nC")
        assert any("must be >= 0" in m for m in msgs)


class TestDag:
    def test_a_cycle_through_a_component_is_found(self):
        with pytest.raises(SyntaxError, match="CYCLE DETECTED"):
            pointy_parser('@mode: "DAG"\n@component F: A\nX -> F -> X')

    def test_acyclic_dag_with_a_component(self):
        pointy_parser('@mode: "DAG"\n@component F: A -> B\nX -> F -> Y')
