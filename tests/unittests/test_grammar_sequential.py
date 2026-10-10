import unittest

from volnux.parser.grammar import pointy_parser
from volnux.parser.ast import (
    TaskNode,
    BinOpNode,
    RetryNode,
    PipelineGroupingNode,
    MetaTaskNode,
    AttributeNode,
    LiteralNode,
    VariableAccessNode,
)


class TestGrammarSequential(unittest.TestCase):
    def test_sequential_single_task_is_task(self):
        program = pointy_parser("TaskA")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "TaskA")

    def test_sequential_two_tasks(self):
        program = pointy_parser("TaskA -> TaskB")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "TaskA")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskB")

    def test_sequential_three_tasks_left_associative(self):
        program = pointy_parser("TaskA -> TaskB -> TaskC")
        node = program.chain
        # top-level should be (A->B) -> C
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.left, BinOpNode)
        self.assertEqual(node.left.op, "->")
        self.assertIsInstance(node.left.left, TaskNode)
        self.assertEqual(node.left.left.task, "TaskA")
        self.assertIsInstance(node.left.right, TaskNode)
        self.assertEqual(node.left.right.task, "TaskB")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskC")

    def test_sequential_with_retry_on_left(self):
        program = pointy_parser("TaskA * 3 -> TaskB")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.left, RetryNode)
        self.assertIsInstance(node.left.job, TaskNode)
        self.assertEqual(node.left.job.task, "TaskA")
        self.assertEqual(node.left.attempts.value, 3)
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskB")

    def test_sequential_with_retry_on_right(self):
        program = pointy_parser("TaskA -> TaskB * 2")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "TaskA")
        self.assertIsInstance(node.right, RetryNode)
        self.assertIsInstance(node.right.job, TaskNode)
        self.assertEqual(node.right.job.task, "TaskB")
        self.assertEqual(node.right.attempts.value, 2)

    def test_sequential_grouped_to_task(self):
        program = pointy_parser("{Run}[opt = 1] -> Worker")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.left, PipelineGroupingNode)
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "Worker")

    def test_sequential_task_to_meta_event(self):
        program = pointy_parser("Worker -> MAP<ProcessPayment>")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.left, TaskNode)
        self.assertIsInstance(node.right, MetaTaskNode)
        self.assertEqual(node.left.task, "Worker")
        self.assertEqual(node.right.mode, "MAP")

    def test_sequential_task_with_attributes(self):
        program = pointy_parser("Worker[retries = 3] -> DoIt")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertIsInstance(node.left, TaskNode)
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "DoIt")
        # left options preserved
        self.assertIsInstance(node.left.options, list)
        self.assertEqual(len(node.left.options), 1)
        attr = node.left.options[0]
        self.assertIsInstance(attr, AttributeNode)
        self.assertEqual(attr.attr, "retries")
        self.assertIsInstance(attr.value, LiteralNode)
        self.assertEqual(attr.value.value, 3)

    def test_sequential_task_two_attributes(self):
        program = pointy_parser("Worker[retries = 3, timeout = 30] -> DoIt")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        left = node.left
        self.assertIsInstance(left, TaskNode)
        names = {a.attr: a for a in left.options}
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)

    def test_sequential_task_three_attributes(self):
        program = pointy_parser(
            "Worker[retries = 3, timeout = 30, verbose = true] -> DoIt"
        )
        node = program.chain
        left = node.left
        self.assertIsInstance(left, TaskNode)
        names = {a.attr: a for a in left.options}
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)
        self.assertEqual(names["verbose"].value.value, True)

    def test_sequential_namespaced_task_with_attributes(self):
        program = pointy_parser("pypi::Run[opt = 1, level = 2] -> Worker")
        node = program.chain
        left = node.left
        self.assertIsInstance(left, TaskNode)
        self.assertEqual(left.task, "Run")
        self.assertEqual(left.namespace, "pypi")
        names = {a.attr: a for a in left.options}
        self.assertEqual(names["opt"].value.value, 1)
        self.assertEqual(names["level"].value.value, 2)

    def test_sequential_meta_event_with_attributes(self):
        program = pointy_parser("MAP<FetchUserData>[concurrency = 4] -> Worker")
        node = program.chain
        left = node.left
        self.assertIsInstance(left, MetaTaskNode)
        names = {a.attr: a for a in left.options}
        self.assertEqual(names["concurrency"].value.value, 4)

    def test_sequential_task_attribute_list_value(self):
        program = pointy_parser("Worker[params = [1, 2]] -> DoIt")
        node = program.chain
        left = node.left
        from volnux.parser.ast import ListNode

        names = {a.attr: a for a in left.options}
        self.assertIsInstance(names["params"].value, ListNode)
        self.assertEqual(len(names["params"].value.value), 2)

    def test_sequential_task_attribute_map_value(self):
        program = pointy_parser('Worker[config = {"a": 1}] -> DoIt')
        node = program.chain
        left = node.left
        from volnux.parser.ast import MapNode

        names = {a.attr: a for a in left.options}
        self.assertIsInstance(names["config"].value, MapNode)
        self.assertIn("a", names["config"].value.value)

    def test_sequential_task_attribute_arithmetic_value(self):
        program = pointy_parser("Worker[port = 8000 + 80] -> DoIt")
        node = program.chain
        left = node.left
        from volnux.parser.ast import BinOpNode

        names = {a.attr: a for a in left.options}
        self.assertIsInstance(names["port"].value, BinOpNode)
        self.assertEqual(names["port"].value.op, "+")

    def test_sequential_task_attribute_variable_reference(self):
        program = pointy_parser("@v = 10 Worker[use = $v] -> DoIt")
        node = program.chain
        left = node.left
        names = {a.attr: a for a in left.options}
        self.assertIsInstance(names["use"].value, VariableAccessNode)
        self.assertEqual(names["use"].value.name, "v")

    def test_sequential_task_attribute_null_coalesce(self):
        program = pointy_parser("Worker[opt = 1 ?? 2] -> DoIt")
        node = program.chain
        left = node.left
        from volnux.parser.ast import NullCoalesceExprNode

        names = {a.attr: a for a in left.options}
        self.assertIsInstance(names["opt"].value, NullCoalesceExprNode)

    def test_sequential_task_attribute_ternary(self):
        program = pointy_parser("Worker[opt = (1 ?? 2) ? 3 : 4] -> DoIt")
        node = program.chain
        left = node.left
        from volnux.parser.ast import TernaryExprNode

        names = {a.attr: a for a in left.options}
        self.assertIsInstance(names["opt"].value, TernaryExprNode)

    def test_sequential_task_attribute_env_var(self):
        program = pointy_parser("Worker[path = $env.PATH] -> DoIt")
        node = program.chain
        left = node.left
        from volnux.parser.ast import EnvironmentVariableAccessNode

        names = {a.attr: a for a in left.options}
        self.assertIsInstance(names["path"].value, EnvironmentVariableAccessNode)

    def test_sequential_grouped_with_attributes(self):
        program = pointy_parser("{Run}[opt = 1] -> Worker")
        node = program.chain
        left = node.left
        self.assertIsInstance(left, PipelineGroupingNode)
        names = {a.attr: a for a in left.options}
        self.assertEqual(names["opt"].value.value, 1)

    def test_sequential_leading_operator_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("-> A")

    def test_sequential_trailing_operator_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("A ->")

    def test_sequential_double_operator_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("A -> -> B")

    def test_sequential_invalid_retry_count_in_seq_raises(self):
        # retry count < 2 should raise (parser's retry rule enforcement)
        with self.assertRaises(SyntaxError):
            pointy_parser("A * 1 -> B")

    def test_sequential_missing_retry_count_raises(self):
        # '*' must be followed by an INT; missing the INT should cause a syntax error
        with self.assertRaises(SyntaxError):
            pointy_parser("A * -> B")

    def test_sequential_nonint_retry_token_raises(self):
        # '*' followed by an identifier instead of INT
        with self.assertRaises(SyntaxError):
            pointy_parser("A * x -> B")

    def test_sequential_long_chain(self):
        program = pointy_parser(
            'TaskA -> DoIt -> Worker[timeout = 30] -> pypi::Run[version = "1.2.3"]'
        )
        node = program.chain
        # left-associative: (((TaskA -> DoIt) -> Worker) -> pypi::Run)
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        # drill into left-associative chain
        left1 = node.left
        self.assertIsInstance(left1, BinOpNode)
        self.assertEqual(left1.op, "->")
        # rightmost target is namespaced Run
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "Run")
        self.assertEqual(node.right.namespace, "pypi")

    def test_sequential_with_meta_and_grouping(self):
        program = pointy_parser(
            "MAP<FetchUserData> -> {Enrich}[opt = 1] -> pypi::Run[opt = 2]"
        )
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertIsInstance(node.left, BinOpNode)
        self.assertIsInstance(node.left.left, MetaTaskNode)
        self.assertIsInstance(node.left.right, PipelineGroupingNode)
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.namespace, "pypi")

    def test_sequential_attribute_value_variants(self):
        # variable declared before usage
        program = pointy_parser(
            '@v = 10 Worker[params = [1, 2], config = {"a": 1}, port = 8000 + 80, use = $v] -> DoIt'
        )
        node = program.chain

        # self.assertIsInstance(node, BinOpNode)
        left = node.left
        self.assertIsInstance(left, TaskNode)
        names = {a.attr: a for a in left.options}
        from volnux.parser.ast import ListNode, MapNode, BinOpNode, VariableAccessNode

        self.assertIsInstance(names["params"].value, ListNode)
        self.assertIsInstance(names["config"].value, MapNode)
        self.assertIsInstance(names["port"].value, BinOpNode)
        self.assertIsInstance(names["use"].value, VariableAccessNode)
        self.assertEqual(names["use"].value.name, "v")

    def test_sequential_complex_mixture_with_retries(self):
        program = pointy_parser("TaskA * 2 -> pypi::Run[opt = 1] -> TaskB * 3")
        node = program.chain
        # top-level is ((TaskA * 2) -> pypi::Run) -> (TaskB * 3)
        self.assertIsInstance(node, BinOpNode)
        self.assertIsInstance(node.left, BinOpNode)
        self.assertIsInstance(node.left.left, RetryNode)
        self.assertEqual(node.left.left.attempts.value, 2)
        self.assertIsInstance(node.right, RetryNode)
        self.assertEqual(node.right.attempts.value, 3)

    # --- Negative sequential tests added ---
    def test_seq_attribute_missing_bracket_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("Worker[timeout = 30 -> DoIt")

    def test_seq_invalid_namespace_raises(self):
        # invalid (unknown) namespace used in a sequential chain should raise ValueError
        with self.assertRaises(ValueError):
            pointy_parser("unknown::Task -> DoIt")

    def test_seq_retry_invalid_count_in_chain_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA * 1 -> DoIt")

    def test_seq_mixed_operator_ordering_raises(self):
        # invalid mixing of operators (malformed order)
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA -> || TaskB")


if __name__ == "__main__":
    unittest.main()
