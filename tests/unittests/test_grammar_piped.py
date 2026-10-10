import unittest

from volnux.parser.grammar import pointy_parser
from volnux.parser.ast import (
    TaskNode,
    BinOpNode,
    RetryNode,
    PipelineGroupingNode,
    MetaTaskNode,
    AttributeNode,
)


class TestGrammarPiped(unittest.TestCase):
    def test_piped_two_tasks(self):
        program = pointy_parser("TaskA |-> TaskB")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "|->")
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "TaskA")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskB")

    def test_piped_three_tasks_left_associative(self):
        program = pointy_parser("TaskA |-> TaskB |-> TaskC")
        node = program.chain
        # (TaskA |-> TaskB) |-> TaskC
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "|->")
        self.assertIsInstance(node.left, BinOpNode)
        self.assertEqual(node.left.op, "|->")
        self.assertEqual(node.left.left.task, "TaskA")
        self.assertEqual(node.left.right.task, "TaskB")
        self.assertEqual(node.right.task, "TaskC")

    def test_piped_with_retry_operands(self):
        program = pointy_parser("TaskA * 3 |-> TaskB * 2")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "|->")
        self.assertIsInstance(node.left, RetryNode)
        self.assertEqual(node.left.job.task, "TaskA")
        self.assertEqual(node.left.attempts.value, 3)
        self.assertIsInstance(node.right, RetryNode)
        self.assertEqual(node.right.job.task, "TaskB")
        self.assertEqual(node.right.attempts.value, 2)

    def test_piped_sequential_mixture(self):
        # Left is a sequential chain, right is a piped chain to a meta-event
        program = pointy_parser("TaskA -> TaskB |-> TaskC -> TaskD")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskD")
        # left should be a sequential BinOp
        self.assertIsInstance(node.left, BinOpNode)
        self.assertEqual(node.left.op, "|->")
        self.assertIsInstance(node.left.right, TaskNode)
        self.assertEqual(node.left.right.task, "TaskC")
        self.assertIsInstance(node.left.left, BinOpNode)
        self.assertEqual(node.left.left.op, "->")
        self.assertIsInstance(node.left.left.left, TaskNode)
        self.assertEqual(node.left.left.left.task, "TaskA")
        self.assertIsInstance(node.left.left.right, TaskNode)
        self.assertEqual(node.left.left.right.task, "TaskB")

    def test_piped_task_with_attributes(self):
        program = pointy_parser("Worker[retries = 3] |-> DoIt")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        left = node.left
        self.assertIsInstance(left, TaskNode)
        self.assertEqual(left.task, "Worker")
        self.assertIsInstance(left.options, list)
        self.assertEqual(len(left.options), 1)
        attr = left.options[0]
        self.assertIsInstance(attr, AttributeNode)
        self.assertEqual(attr.attr, "retries")
        self.assertEqual(attr.value.value, 3)

    def test_piped_namespaced_and_meta_event(self):
        program = pointy_parser("pypi::Run |-> MAP<FetchUserData>")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "Run")
        self.assertEqual(node.left.namespace, "pypi")
        self.assertIsInstance(node.right, MetaTaskNode)
        self.assertEqual(node.right.mode, "MAP")

    def test_piped_grouped_with_attributes(self):
        program = pointy_parser("{DoIt}[opt = 1] |-> {Run}[opt = 2]")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertIsInstance(node.left, PipelineGroupingNode)
        self.assertIsInstance(node.right, PipelineGroupingNode)
        left_names = {a.attr: a for a in node.left.options}
        right_names = {a.attr: a for a in node.right.options}
        self.assertEqual(left_names["opt"].value.value, 1)
        self.assertEqual(right_names["opt"].value.value, 2)

    def test_piped_task_attribute_value_variants(self):
        program = pointy_parser(
            '@v = 1 Worker[params=[1,2], config={"a":1}] |-> Worker[port = 8000 + 80, use = $v]'
        )
        node = program.chain
        left = node.left
        right = node.right
        from volnux.parser.ast import ListNode, MapNode, BinOpNode, VariableAccessNode

        names_l = {a.attr: a for a in left.options}
        names_r = {a.attr: a for a in right.options}
        self.assertIsInstance(names_l["params"].value, ListNode)
        self.assertIsInstance(names_l["config"].value, MapNode)
        self.assertIsInstance(names_r["port"].value, BinOpNode)

    def test_piped_sequential_complex_mix(self):
        # Left-to-right associativity with equal precedence for '->' and '|->'
        program = pointy_parser(
            'TaskA * 2 -> Worker[retries = 3, timeout = 30] |-> pypi::Run[version = "1.2.3"] -> MAP<FetchUserData>'
        )
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        # final operator should be the right-most '->' due to left-associativity
        self.assertEqual(node.op, "->")
        # left side should be a BinOp representing the prefix ((TaskA * 2 -> Worker) |-> pypi::Run)
        self.assertIsInstance(node.left, BinOpNode)
        self.assertEqual(node.left.op, "|->")
        # left.left should be the sequential BinOp (TaskA * 2 -> Worker)
        self.assertIsInstance(node.left.left, BinOpNode)
        self.assertEqual(node.left.left.op, "->")
        # left.left.left should be a RetryNode
        self.assertIsInstance(node.left.left.left, RetryNode)
        self.assertEqual(node.left.left.left.attempts.value, 2)
        # left.left.right should be a TaskNode with attributes
        left_right = node.left.left.right
        self.assertIsInstance(left_right, TaskNode)
        names = {a.attr: a for a in left_right.options}
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)
        # right side should be a MetaEventNode
        self.assertIsInstance(node.right, MetaTaskNode)
        self.assertEqual(node.right.mode, "MAP")
        self.assertEqual(node.right.template_task, "FetchUserData")

    # Negative tests for piped grammar
    def test_piped_leading_operator_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("|-> TaskA")

    def test_piped_trailing_operator_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA |->")

    def test_piped_double_operator_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA |-> |-> TaskB")

    def test_piped_with_invalid_retry_count_raises(self):
        # invalid retry inside a piped should raise
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA * 1 |-> TaskB")

    def test_piped_malformed_attribute_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("Worker[retries = 3 |-> TaskB")

    def test_piped_invalid_namespace_raises(self):
        with self.assertRaises(ValueError):
            pointy_parser("unknown::Task |-> TaskB")


if __name__ == "__main__":
    unittest.main()
