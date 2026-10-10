import unittest

from volnux.parser.grammar import pointy_parser
from volnux.parser.ast import (
    TaskNode,
    BinOpNode,
    ConditionalNode,
)


class TestGrammarConditional(unittest.TestCase):
    def test_simple_conditional(self):
        program = pointy_parser("TaskA(0 -> TaskB, 1 -> TaskC)")
        conditional = program.chain

        self.assertIsInstance(conditional, ConditionalNode)
        self.assertEqual(conditional.task.task, "TaskA")
        self.assertEqual(len(conditional.branches), 2)

        branches = {a.condition.value: a for a in conditional.branches}

        self.assertEqual(branches[0].operator, "->")
        self.assertIsInstance(branches[0].task, TaskNode)
        self.assertEqual(branches[0].task.task, "TaskB")

        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, TaskNode)
        self.assertEqual(branches[1].task.task, "TaskC")

    def test_simple_sequential_conditional(self):
        program = pointy_parser("TaskF -> TaskA(0 -> TaskB, 1 -> TaskC, 2 |-> TaskD)")
        node = program.chain

        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "TaskF")
        self.assertIsInstance(node.right, ConditionalNode)
        conditional = node.right
        self.assertEqual(conditional.task.task, "TaskA")
        self.assertEqual(len(conditional.branches), 3)

        branches = {a.condition.value: a for a in conditional.branches}
        self.assertEqual(branches[0].operator, "->")
        self.assertIsInstance(branches[0].task, TaskNode)
        self.assertEqual(branches[0].task.task, "TaskB")

        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, TaskNode)
        self.assertEqual(branches[1].task.task, "TaskC")

        self.assertEqual(branches[2].operator, "|->")
        self.assertIsInstance(branches[2].task, TaskNode)
        self.assertEqual(branches[2].task.task, "TaskD")

    def test_conditional_with_grouping(self):
        program = pointy_parser("TaskA(0 -> (TaskB || TaskC), 1 -> TaskD)")
        conditional = program.chain

        self.assertIsInstance(conditional, ConditionalNode)
        self.assertEqual(conditional.task.task, "TaskA")
        self.assertEqual(len(conditional.branches), 2)

        branches = {a.condition.value: a for a in conditional.branches}

        self.assertEqual(branches[0].operator, "->")
        self.assertIsInstance(branches[0].task, BinOpNode)
        self.assertEqual(branches[0].task.op, "||")
        self.assertIsInstance(branches[0].task.left, TaskNode)
        self.assertEqual(branches[0].task.left.task, "TaskB")
        self.assertIsInstance(branches[0].task.right, TaskNode)
        self.assertEqual(branches[0].task.right.task, "TaskC")

        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, TaskNode)
        self.assertEqual(branches[1].task.task, "TaskD")

    def test_conditional_with_nested_conditionals(self):
        program = pointy_parser("TaskA(0 -> TaskB(0 -> TaskC, 1 -> TaskD), 1 -> TaskE)")
        conditional = program.chain

        self.assertIsInstance(conditional, ConditionalNode)
        self.assertEqual(conditional.task.task, "TaskA")
        self.assertEqual(len(conditional.branches), 2)

        branches = {a.condition.value: a for a in conditional.branches}

        self.assertEqual(branches[0].operator, "->")
        self.assertIsInstance(branches[0].task, ConditionalNode)
        nested_conditional = branches[0].task
        self.assertEqual(nested_conditional.task.task, "TaskB")
        self.assertEqual(len(nested_conditional.branches), 2)

        nested_branches = {a.condition.value: a for a in nested_conditional.branches}
        self.assertEqual(nested_branches[0].operator, "->")
        self.assertIsInstance(nested_branches[0].task, TaskNode)
        self.assertEqual(nested_branches[0].task.task, "TaskC")

        self.assertEqual(nested_branches[1].operator, "->")
        self.assertIsInstance(nested_branches[1].task, TaskNode)
        self.assertEqual(nested_branches[1].task.task, "TaskD")

        self.assertEqual(branches[1].operator, "->")
        self.assertIsInstance(branches[1].task, TaskNode)
        self.assertEqual(branches[1].task.task, "TaskE")

    def test_conditional_with_descriptor_out_of_range(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA(0 -> TaskB, 1 -> TaskC, 14 -> TaskD)")

        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA(-1 -> TaskB, 1 -> TaskC, 9 -> TaskD)")

    def test_conditional_with_invalid_operator(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA(0 -> TaskB, 1 || TaskC)")

    def test_conditional_with_missing_branches(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA()")

    def test_conditional_with_non_task_branch(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA(0 -> TaskB, 1 -> 42)")

    def test_conditional_with_non_literal_descriptor(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA(x -> TaskB, 1 -> TaskC)")

    # def test_conditional_with_duplicate_descriptors(self):
    #     with self.assertRaises(SyntaxError):
    #         pointy_parser("TaskA(0 -> TaskB, 0 -> TaskC)")

    def test_conditional_with_non_integer_descriptor(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA(0.5 -> TaskB, 1 -> TaskC)")


if __name__ == "__main__":
    unittest.main()
