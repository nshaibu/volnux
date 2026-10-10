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


class TestGrammarParallel(unittest.TestCase):
    def test_parallel_two_tasks(self):
        program = pointy_parser("TaskA || TaskB")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "||")
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "TaskA")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskB")

    def test_parallel_three_tasks_left_associative(self):
        program = pointy_parser("TaskA || TaskB || TaskC")
        node = program.chain
        # (TaskA || TaskB) || TaskC
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "||")
        self.assertIsInstance(node.left, BinOpNode)
        self.assertEqual(node.left.op, "||")
        self.assertEqual(node.left.left.task, "TaskA")
        self.assertEqual(node.left.right.task, "TaskB")
        self.assertEqual(node.right.task, "TaskC")

    def test_parallel_with_retry_operands(self):
        program = pointy_parser("TaskA * 3 || TaskB * 2")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "||")
        self.assertIsInstance(node.left, RetryNode)
        self.assertEqual(node.left.job.task, "TaskA")
        self.assertEqual(node.left.attempts.value, 3)
        self.assertIsInstance(node.right, RetryNode)
        self.assertEqual(node.right.job.task, "TaskB")
        self.assertEqual(node.right.attempts.value, 2)

    def test_parallel_sequential_mixture(self):
        # Left is a sequential chain, right is a sequential chain
        program = pointy_parser("TaskA -> TaskB || TaskC -> TaskD")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskD")

        node = node.left
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "||")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskC")
        node = node.left
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskB")
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "TaskA")

        # self.assertEqual(node.op, "||")
        # self.assertIsInstance(node.left, BinOpNode)
        # self.assertEqual(node.left.op, "->")
        # self.assertIsInstance(node.right, BinOpNode)
        # self.assertEqual(node.right.op, "->")
        # self.assertEqual(node.left.left.task, "TaskA")
        # self.assertEqual(node.left.right.task, "TaskB")
        # self.assertEqual(node.right.left.task, "TaskC")
        # self.assertEqual(node.right.right.task, "TaskD")

    def test_parallel_task_with_attributes(self):
        program = pointy_parser("Worker[retries = 3] || DoIt")
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

    def test_parallel_namespaced_and_meta_event(self):
        program = pointy_parser("pypi::Run || MAP<FetchUserData>")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertIsInstance(node.left, TaskNode)
        self.assertEqual(node.left.task, "Run")
        self.assertEqual(node.left.namespace, "pypi")
        self.assertIsInstance(node.right, MetaTaskNode)
        self.assertEqual(node.right.mode, "MAP")

    def test_parallel_grouped_with_attributes(self):
        program = pointy_parser("{DoIt}[opt = 1] || {Run}[opt = 2]")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertIsInstance(node.left, PipelineGroupingNode)
        self.assertIsInstance(node.right, PipelineGroupingNode)
        left_names = {a.attr: a for a in node.left.options}
        right_names = {a.attr: a for a in node.right.options}
        self.assertEqual(left_names["opt"].value.value, 1)
        self.assertEqual(right_names["opt"].value.value, 2)

    def test_parallel_task_attribute_value_variants(self):
        # list, map, arithmetic, variable reference
        program = pointy_parser('Worker[params=[1,2]] || Worker[config={"a":1}]')
        node = program.chain
        left = node.left
        right = node.right
        from volnux.parser.ast import ListNode, MapNode

        names_l = {a.attr: a for a in left.options}
        names_r = {a.attr: a for a in right.options}
        self.assertIsInstance(names_l["params"].value, ListNode)
        self.assertIsInstance(names_r["config"].value, MapNode)

    def test_parallel_sequential_complex_mix(self):
        # Left is a sequential chain that includes a task with attributes and retry; right is a sequential chain ending in a meta-event
        program = pointy_parser(
            'TaskA * 2 -> Worker[retries = 3, timeout = 30] || pypi::Run[version = "1.2.3"] -> MAP<FetchUserData>'
        )
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.right, MetaTaskNode)
        self.assertEqual(node.right.mode, "MAP")
        self.assertEqual(node.right.template_task, "FetchUserData")

        node = node.left
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "||")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "Run")
        self.assertEqual(node.right.namespace, "pypi")
        options = {a.attr: a for a in node.right.options}
        self.assertEqual(options["version"].value.value, "1.2.3")

        node = node.left
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "Worker")
        names = {a.attr: a for a in node.right.options}
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)

        self.assertIsInstance(node.left, RetryNode)
        self.assertIsInstance(node.left.job, TaskNode)
        self.assertEqual(node.left.job.task, "TaskA")
        self.assertEqual(node.left.attempts.value, 2)

        # # left side should be a sequential chain
        # self.assertIsInstance(node.left, BinOpNode)
        # self.assertEqual(node.left.op, '||')
        # # left.left should be a RetryNode
        # self.assertIsInstance(node.left.left, RetryNode)
        # self.assertEqual(node.left.left.attempts.value, 2)
        # # left.right should be a TaskNode with attributes
        # left_right = node.left.right
        # self.assertIsInstance(left_right, TaskNode)
        # names = {a.attr: a for a in left_right.options}
        # self.assertEqual(names['retries'].value.value, 3)
        # self.assertEqual(names['timeout'].value.value, 30)
        # # right side should be sequential: namespaced Run -> MAP
        # self.assertIsInstance(node.right, BinOpNode)
        # self.assertIsInstance(node.right.left, TaskNode)
        # self.assertEqual(node.right.left.namespace, 'pypi')
        # self.assertIsInstance(node.right.right, MetaEventNode)

    def test_parallel_complex_task_patterns(self):
        # tasks using lists, maps, arithmetic, env var, ternary, null coalesce
        program = pointy_parser(
            '@v = 5 Worker[params = [1,2], config = {"a": 1}, port = 8000 + 80, path = $env.PATH, pick = (1 ?? 2) ? 3 : 4] || pypi::Deploy[version = "1.2.3", flag = true]'
        )
        node = program.chain
        # self.assertIsInstance(node, BinOpNode)
        left = node.left
        right = node.right
        # check left complex attributes
        names = {a.attr: a for a in left.options}
        from volnux.parser.ast import (
            ListNode,
            MapNode,
            BinOpNode,
            EnvironmentVariableAccessNode,
            TernaryExprNode,
            NullCoalesceExprNode,
        )

        self.assertIsInstance(names["params"].value, ListNode)
        self.assertIsInstance(names["config"].value, MapNode)
        self.assertIsInstance(names["port"].value, BinOpNode)
        self.assertIsInstance(names["path"].value, EnvironmentVariableAccessNode)
        self.assertIsInstance(names["pick"].value, TernaryExprNode)
        # right should be namespaced task with literal attribute
        rnames = {a.attr: a for a in right.options}
        self.assertEqual(rnames["version"].value.value, "1.2.3")
        self.assertEqual(rnames["flag"].value.value, True)

    def test_parallel_grouped_sequences_with_retries(self):
        program = pointy_parser("{Init}[opt=1] -> TaskA || {Start}[opt=2] -> TaskB * 3")
        node = program.chain
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.right, RetryNode)
        self.assertEqual(node.right.attempts.value, 3)
        self.assertIsInstance(node.right.job, TaskNode)
        self.assertEqual(node.right.job.task, "TaskB")

        self.assertIsInstance(node.left, BinOpNode)
        self.assertEqual(node.left.op, "||")
        self.assertIsInstance(node.left.right, PipelineGroupingNode)
        self.assertEqual(len(node.left.right.expressions), 1)
        self.assertIsInstance(node.left.right.expressions[0], TaskNode)
        self.assertEqual(node.left.right.expressions[0].task, "Start")
        options = {a.attr: a for a in node.left.right.options}
        self.assertEqual(options["opt"].value.value, 2)

        node = node.left.left
        self.assertIsInstance(node, BinOpNode)
        self.assertEqual(node.op, "->")
        self.assertIsInstance(node.right, TaskNode)
        self.assertEqual(node.right.task, "TaskA")
        self.assertIsInstance(node.left, PipelineGroupingNode)
        self.assertEqual(len(node.left.expressions), 1)
        self.assertIsInstance(node.left.expressions[0], TaskNode)
        self.assertEqual(node.left.expressions[0].task, "Init")
        options = {a.attr: a for a in node.left.options}
        self.assertEqual(options["opt"].value.value, 1)

    # Negative cases for parallel when using complex/sequential forms
    def test_parallel_missing_attribute_bracket_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("Worker[retries = 3 || TaskB")

    def test_parallel_malformed_map_literal_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("Worker[config = {a: 1}] || TaskB")

    def test_parallel_invalid_namespace_raises(self):
        # unknown namespace should raise ValueError during TaskNode creation
        with self.assertRaises(ValueError):
            pointy_parser("unknown::Task || TaskB")

    def test_parallel_retry_invalid_count_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("TaskA * 1 || TaskB")


if __name__ == "__main__":
    unittest.main()
