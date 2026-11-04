"""Tests for ExecutableASTGenerator: AST -> task hierarchy."""

import os
import unittest
from unittest import mock

from volnux.parser.exceptions import PointyParseError
from volnux.parser.grammar import pointy_parser
from volnux.parser.operator import PipeType
from volnux.parser.protocols import GroupingStrategy
from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.task import PipelineTask, PipelineTaskGrouping


def build(code):
    generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    generator.visit_program(pointy_parser(code))
    return generator.generate()


def next_of(task):
    return task.condition_node.on_success_event


def pipe_of(task):
    return task.condition_node.on_success_pipe


class TestSequencing(unittest.TestCase):
    def test_sequence(self):
        a = build("A -> B -> C")
        b = next_of(a)
        c = next_of(b)
        self.assertEqual([t.event for t in (a, b, c)], ["A", "B", "C"])
        self.assertEqual(pipe_of(a), PipeType.POINTER)
        self.assertEqual(pipe_of(b), PipeType.POINTER)
        self.assertIsNone(next_of(c))
        self.assertIs(b.parent_node, a)
        self.assertIs(c.get_root(), a)

    def test_pipe_result(self):
        a = build("A |-> B")
        self.assertEqual(pipe_of(a), PipeType.PIPE_POINTER)

    def test_parallel_then_pipe(self):
        a = build("A || B |-> C")
        b = next_of(a)
        self.assertEqual(pipe_of(a), PipeType.PARALLELISM)
        self.assertEqual(pipe_of(b), PipeType.PIPE_POINTER)
        self.assertEqual(next_of(b).event, "C")
        self.assertTrue(a.is_parallel_execution_node)

    def test_parallel_binds_tighter_than_sequence(self):
        a = build("A -> B || C -> D")
        b = next_of(a)
        c = next_of(b)
        d = next_of(c)
        self.assertEqual([t.event for t in (a, b, c, d)], list("ABCD"))
        self.assertEqual(
            [pipe_of(a), pipe_of(b), pipe_of(c)],
            [PipeType.POINTER, PipeType.PARALLELISM, PipeType.POINTER],
        )
        self.assertEqual(list(t.event for t in b.get_parallel_nodes()), ["B", "C"])

    def test_three_way_parallel(self):
        a = build("A || B || C")
        self.assertEqual([t.event for t in a.get_parallel_nodes()], ["A", "B", "C"])

    def test_parenthesised_chain(self):
        a = build("(A -> B) -> C")
        self.assertEqual(next_of(next_of(a)).event, "C")

    def test_no_chain(self):
        self.assertIsNone(build('@author: "me"'))

    def test_namespaces(self):
        a = build("billing::Charge -> Notify")
        self.assertEqual(a.namespace, "billing")
        self.assertEqual(a.event, "Charge")
        self.assertEqual(next_of(a).namespace, "local")


class TestConditionals(unittest.TestCase):
    def test_named_builtin_descriptors_and_sink(self):
        a = build("A (failure -> B, success -> C) -> S")
        self.assertTrue(a.is_conditional)
        failure = a.condition_node.on_failure_event
        success = a.condition_node.on_success_event
        self.assertEqual((failure.event, success.event), ("B", "C"))
        self.assertTrue(failure.is_descriptor_task)
        self.assertEqual(failure.descriptor, 0)
        self.assertEqual(success.descriptor, 1)
        self.assertIs(failure.parent_node, a)
        self.assertEqual(a.sink_node.event, "S")
        self.assertEqual(a.sink_pipe, PipeType.POINTER)
        self.assertTrue(a.sink_node.is_sink)
        self.assertEqual(len(a.get_children()), 3)

    def test_numeric_descriptors_match_named(self):
        a = build("A (0 -> B, 1 -> C)")
        self.assertEqual(a.condition_node.on_failure_event.event, "B")
        self.assertEqual(a.condition_node.on_success_event.event, "C")

    def test_descriptors_above_nine(self):
        a = build("A (10 -> B, 40 -> C)")
        self.assertEqual(a.get_descriptor(10).event, "B")
        self.assertEqual(a.get_descriptor(40).event, "C")
        self.assertEqual(a.get_descriptor(40).descriptor, 40)

    def test_custom_descriptors(self):
        a = build(
            "@descriptor: out_of_balance, unsupported\n"
            "A (out_of_balance -> B, unsupported |-> C, success -> D)"
        )
        self.assertEqual(a.get_descriptor(2).event, "B")
        self.assertEqual(a.get_descriptor(3).event, "C")
        self.assertEqual(a.get_descriptor(3).descriptor_pipe, PipeType.PIPE_POINTER)
        self.assertEqual(
            a.get_descriptor(3).get_pointer_to_task(), PipeType.PIPE_POINTER
        )

    def test_branch_chain_is_linked(self):
        a = build("A (failure -> B -> C, success -> D)")
        b = a.condition_node.on_failure_event
        self.assertEqual(next_of(b).event, "C")
        self.assertIs(next_of(b).parent_node, b)

    def test_nested_conditional(self):
        a = build("A (failure -> B, success -> C (failure -> X, success -> Y))")
        c = a.condition_node.on_success_event
        self.assertTrue(c.is_conditional)
        self.assertEqual(c.condition_node.on_success_event.event, "Y")

    def test_single_branch_conditional_followed_by_task_uses_sink(self):
        a = build("A (failure -> B) -> S")
        self.assertEqual(a.sink_node.event, "S")
        self.assertIsNone(next_of(a))

    def test_conditional_inside_chain(self):
        a = build("X -> A (failure -> B, success -> C) -> S")
        self.assertEqual(next_of(a).event, "A")
        self.assertEqual(next_of(a).sink_node.event, "S")

    def test_duplicate_descriptor_is_rejected(self):
        with self.assertRaises(PointyParseError):
            build("A (failure -> B, 0 -> C)")

    def test_headless_conditional_is_rejected(self):
        with self.assertRaises(PointyParseError):
            build("(failure -> B, success -> C)")


class TestRetry(unittest.TestCase):
    def test_retry_on_tasks(self):
        a = build("A * 2 -> B * 4 -> C")
        self.assertEqual(a.options.retry_attempts, 2)
        self.assertEqual(next_of(a).options.retry_attempts, 4)
        self.assertIsNone(next_of(next_of(a)).options)

    def test_retry_on_chain_wraps_in_group(self):
        group = build("(A -> B) * 3 -> C")
        self.assertIsInstance(group, PipelineTaskGrouping)
        self.assertEqual(group.options.retry_attempts, 3)
        self.assertEqual(group.chains[0].event, "A")
        self.assertEqual(next_of(group).event, "C")

    def test_retry_with_branches_on_subject(self):
        a = build("A * 2 (failure -> B * 3, success -> C)")
        self.assertEqual(a.options.retry_attempts, 2)
        self.assertEqual(a.condition_node.on_failure_event.options.retry_attempts, 3)

    def test_retry_keeps_other_options(self):
        a = build('A[executor = "X"] * 2')
        self.assertEqual(a.options.retry_attempts, 2)
        self.assertEqual(a.options.executor, "X")

    def test_retry_on_conditional_is_rejected(self):
        with self.assertRaises(PointyParseError):
            build("(failure -> B, success -> C) * 2")


class TestGroups(unittest.TestCase):
    def test_group(self):
        group = build("{ A -> B }[retry_attempts = 2] -> C")
        self.assertIsInstance(group, PipelineTaskGrouping)
        self.assertEqual(group.strategy, GroupingStrategy.SINGLE_CHAIN)
        self.assertEqual(group.options.retry_attempts, 2)
        self.assertEqual(group.chains[0].event, "A")
        self.assertEqual(next_of(group.chains[0]).event, "B")
        self.assertEqual(next_of(group).event, "C")

    def test_group_can_be_parallel_member(self):
        a = build("{ A -> B } || C")
        self.assertIsInstance(a, PipelineTaskGrouping)
        self.assertEqual(pipe_of(a), PipeType.PARALLELISM)
        self.assertEqual(next_of(a).event, "C")

    def test_multi_task_parallel_operands_are_rejected(self):
        for code in ("(A -> B) || C", "A || (B -> C)", "A (failure -> B) || C"):
            with self.assertRaises(PointyParseError, msg=code):
                build(code)


class TestMetaEvents(unittest.TestCase):
    def test_meta_event_is_task_with_template_in_extras(self):
        task = build("MAP<ProcessItem>[concurrent = true, batch_size = 10] -> Merge")
        self.assertEqual(task.event, "MAP")
        self.assertEqual(
            task.options.extras,
            {
                "concurrent": True,
                "batch_size": 10,
                "template_class": "ProcessItem",
                "template_namespace": "local",
            },
        )
        self.assertEqual(next_of(task).event, "Merge")

    def test_namespaced_template(self):
        task = build("FOREACH<audit::Record>")
        self.assertEqual(task.options.extras["template_class"], "Record")
        self.assertEqual(task.options.extras["template_namespace"], "audit")

    def test_meta_event_without_attributes(self):
        task = build("REDUCE<Sum>")
        self.assertEqual(task.options.extras["template_class"], "Sum")


class TestAttributeExpressions(unittest.TestCase):
    def extras(self, code):
        return build(code).options.extras

    def test_arithmetic_and_precedence(self):
        self.assertEqual(
            self.extras("A[a = 1 + 2 * 3, b = (1 + 2) * 3, c = 7 % 4, d = 7 / 2]"),
            {"a": 7, "b": 9, "c": 3, "d": 3.5},
        )

    def test_bitwise_and_shifts(self):
        extras = self.extras(
            "A[a = 6 & 3, b = 6 | 1, c = 6 ^ 3, d = 1 << 4, e = -8 >>> 1, f = -8 >> 60, g = ~5]"
        )
        self.assertEqual(
            extras, {"a": 2, "b": 7, "c": 5, "d": 16, "e": -4, "f": 15, "g": -6}
        )

    def test_logic_comparison_and_unary(self):
        extras = self.extras(
            "A[a = 1 < 2 && 2 < 3, b = 0 || 5, c = !0, d = 2 == 3, e = -(1 + 1)]"
        )
        self.assertEqual(extras, {"a": True, "b": True, "c": True, "d": False, "e": -2})

    def test_ternary_and_null_coalesce(self):
        extras = self.extras(
            'A[a = 1 > 0 ? "yes" : "no", b = null ?? 4, c = 3 ?? 4, d = null ?? null ?? 7]'
        )
        self.assertEqual(extras, {"a": "yes", "b": 4, "c": 3, "d": 7})

    def test_lists_maps_and_nulls(self):
        extras = self.extras('A[a = [1, "x", [true]], b = {"k": {"n": null}}]')
        self.assertEqual(extras, {"a": [1, "x", [True]], "b": {"k": {"n": None}}})

    def test_variables_and_indexing(self):
        extras = self.extras(
            '@items = [10, 20]\n@cfg = {"size": 5}\n@n = 3\n'
            'A[a = $n * 2, b = $items[1], c = $cfg["size"], d = $items]'
        )
        self.assertEqual(extras, {"a": 6, "b": 20, "c": 5, "d": [10, 20]})

    def test_environment_variable(self):
        with mock.patch.dict(os.environ, {"POINTY_TEST_VAR": "xyz"}):
            extras = self.extras(
                "A[a = $env.POINTY_TEST_VAR, b = $env.POINTY_MISSING ?? 1]"
            )
        self.assertEqual(extras, {"a": "xyz", "b": 1})

    def test_known_options_are_not_extras(self):
        task = build(
            'A[retry_attempts = 1 + 2, stop_condition = "on_error", other = 1]'
        )
        self.assertEqual(task.options.retry_attempts, 3)
        self.assertEqual(task.options.extras, {"other": 1})

    def test_evaluation_errors(self):
        for code in ("A[a = 1 / 0]", 'A[a = "x" - 1]', "A[a = ~1.5]"):
            with self.assertRaises(PointyParseError, msg=code):
                build(code)

    def test_invalid_option_value(self):
        with self.assertRaises(PointyParseError):
            build('A[stop_condition = "bogus"]')


class TestDescriptorTranslation(unittest.TestCase):
    SOURCE = (
        "@descriptor: out_of_balance, unsupported\n"
        "A (out_of_balance -> {B -> C}, failure -> MAP<P>) -> (D -> E) * 2"
    )

    def all_tasks(self):
        from volnux.task.base import TaskBase

        root = build(self.SOURCE)
        stack, seen = [root], []
        while stack:
            task = stack.pop()
            seen.append(task)
            stack.extend(task.get_children())
            stack.extend(getattr(task, "chains", []))
        return root, seen

    def test_every_task_gets_the_descriptor_map(self):
        _, tasks = self.all_tasks()
        self.assertGreaterEqual(len(tasks), 6)
        for task in tasks:
            self.assertEqual(task.descriptors, {"out_of_balance": 2, "unsupported": 3})

    def test_each_task_has_its_own_copy(self):
        root, _ = self.all_tasks()
        root.descriptors["extra"] = 9
        self.assertNotIn("extra", root.get_children()[0].descriptors)

    def test_two_way_translation(self):
        root = build(self.SOURCE)
        self.assertEqual(root.get_descriptor_value("out_of_balance"), 2)
        self.assertEqual(root.get_descriptor_value("unsupported"), 3)
        self.assertEqual(root.get_descriptor_value("failure"), 0)
        self.assertEqual(root.get_descriptor_value("success"), 1)
        self.assertIsNone(root.get_descriptor_value("nope"))
        self.assertEqual(root.get_descriptor_name(2), "out_of_balance")
        self.assertEqual(root.get_descriptor_name(0), "failure")
        self.assertEqual(root.get_descriptor_name(1), "success")
        self.assertIsNone(root.get_descriptor_name(42))

    def test_no_directive_means_only_builtins(self):
        task = build("A -> B")
        self.assertEqual(task.descriptors, {})
        self.assertEqual(task.get_descriptor_name(1), "success")
        self.assertIsNone(task.get_descriptor_name(2))


class TestGeneratorState(unittest.TestCase):
    def test_generate_before_visit_is_none(self):
        self.assertIsNone(
            ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping).generate()
        )

    def test_descriptors_are_exposed(self):
        generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
        generator.visit_program(pointy_parser("@descriptor: a, b\nX (a -> Y)"))
        self.assertEqual(generator.descriptors, {"a": 2, "b": 3})

    def test_generator_can_be_reused(self):
        generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
        generator.visit_program(pointy_parser("A -> B"))
        generator.visit_program(pointy_parser("X"))
        self.assertEqual(generator.generate().event, "X")
        self.assertIsNone(next_of(generator.generate()))


if __name__ == "__main__":
    unittest.main()
