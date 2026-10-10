import unittest

from volnux.parser.grammar import pointy_parser
from volnux.parser.ast import (
    TaskNode,
    AttributeNode,
    MetaTaskNode,
    LiteralNode,
    VariableAccessNode,
    RetryNode,
    PipelineGroupingNode,
)


class TestGrammarChainPrimary(unittest.TestCase):
    def test_simple_task(self):
        program = pointy_parser("DoIt")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "DoIt")
        # attribute_list is optional and should be an empty list when absent
        self.assertTrue(node.options == [] or node.options is None)

    def test_namespaced_task(self):
        # use namespace-first syntax 'pypi::Run' and PascalCase task names
        program = pointy_parser("pypi::Run")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "Run")
        self.assertEqual(node.namespace, "pypi")

    def test_task_with_attribute(self):
        program = pointy_parser("Worker[retries = 3]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "Worker")
        # options should be a list with one AttributeNode
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 1)
        attr = node.options[0]
        self.assertIsInstance(attr, AttributeNode)
        self.assertEqual(attr.attr, "retries")
        # attribute value should be a LiteralNode with numeric value 3
        self.assertIsInstance(attr.value, LiteralNode)
        self.assertEqual(attr.value.value, 3)

    def test_meta_event_simple(self):
        program = pointy_parser("MAP<FetchUserData>")
        node = program.chain
        self.assertIsInstance(node, MetaTaskNode)
        self.assertEqual(node.mode, "MAP")
        self.assertEqual(node.template_task, "FetchUserData")
        # options should be present (attribute_list) but empty list by default
        self.assertTrue(node.options == [] or node.options is None)

    def test_meta_event_namespaced(self):
        program = pointy_parser("FILTER<ns::EnrichUserData>")
        node = program.chain
        self.assertIsInstance(node, MetaTaskNode)
        self.assertEqual(node.mode, "FILTER")
        # grammar_v2 uses IDENTIFIER DOUBLE_COLON IDENTIFIER form; namespace and template_event are set
        self.assertEqual(node.template_event_namespace, "ns")
        self.assertEqual(node.template_task, "EnrichUserData")

    def test_task_with_multiple_attributes(self):
        program = pointy_parser("Worker[retries = 3, timeout = 30]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "Worker")
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 2)
        names = {a.attr: a for a in node.options}
        self.assertIn("retries", names)
        self.assertIn("timeout", names)
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)

    def test_meta_event_with_attributes(self):
        program = pointy_parser("MAP<FetchUserData>[concurrency = 4, timeout = 30]")
        node = program.chain
        self.assertIsInstance(node, MetaTaskNode)
        self.assertEqual(node.mode, "MAP")
        self.assertEqual(node.template_task, "FetchUserData")
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 2)
        names = {a.attr: a for a in node.options}
        self.assertIsInstance(names["concurrency"].value, LiteralNode)
        self.assertEqual(names["concurrency"].value.value, 4)
        self.assertIsInstance(names["timeout"].value, LiteralNode)
        self.assertEqual(names["timeout"].value.value, 30)

    def test_meta_event_namespaced_with_attributes(self):
        program = pointy_parser("FILTER<ns::EnrichUserData>[opt = 2, level = 5]")
        node = program.chain
        self.assertIsInstance(node, MetaTaskNode)
        self.assertEqual(node.mode, "FILTER")
        self.assertEqual(node.template_event_namespace, "ns")
        self.assertEqual(node.template_task, "EnrichUserData")
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 2)
        names = {a.attr: a for a in node.options}
        self.assertIsInstance(names["opt"].value, LiteralNode)
        self.assertEqual(names["opt"].value.value, 2)
        self.assertIsInstance(names["level"].value, LiteralNode)
        self.assertEqual(names["level"].value.value, 5)

    def test_task_with_three_attributes(self):
        program = pointy_parser("Worker[retries = 3, timeout = 30, verbose = true]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "Worker")
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 3)
        names = {a.attr: a for a in node.options}
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)
        # boolean literal should be True
        self.assertEqual(names["verbose"].value.value, True)

    def test_namespaced_task_with_one_attribute(self):
        # Use namespace-first 'pypi::Deploy' and PascalCase task
        program = pointy_parser('pypi::Deploy[version = "1.2.3"]')
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "Deploy")
        self.assertEqual(node.namespace, "pypi")
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 1)
        names = {a.attr: a for a in node.options}
        self.assertIsInstance(names["version"].value, LiteralNode)
        self.assertEqual(names["version"].value.value, "1.2.3")

    def test_namespaced_task_with_multiple_attributes(self):
        # Use namespace-first 'pypi::Run' and PascalCase task
        program = pointy_parser("pypi::Run[opt = 1, level = 2, flag = false]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        self.assertEqual(node.task, "Run")
        self.assertEqual(node.namespace, "pypi")
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 3)
        names = {a.attr: a for a in node.options}
        self.assertEqual(names["opt"].value.value, 1)
        self.assertEqual(names["level"].value.value, 2)
        self.assertEqual(names["flag"].value.value, False)

    def test_task_attribute_list_value(self):
        program = pointy_parser("Worker[params = [1, 2, 3]]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        names = {a.attr: a for a in node.options}
        self.assertIn("params", names)
        list_node = names["params"].value
        # should be a ListNode with three LiteralNode items
        from volnux.parser.ast import ListNode

        self.assertIsInstance(list_node, ListNode)
        self.assertEqual(len(list_node.value), 3)
        self.assertEqual(list_node.value[0].value, 1)

    def test_task_attribute_map_value(self):
        program = pointy_parser('Worker[config = {"a": 1}]')
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        names = {a.attr: a for a in node.options}
        self.assertIn("config", names)
        map_node = names["config"].value
        from volnux.parser.ast import MapNode

        self.assertIsInstance(map_node, MapNode)
        # map stores values as AST nodes; check stored value for key "a"
        self.assertIn("a", map_node.value)
        self.assertEqual(map_node.value["a"].value, 1)

    def test_task_attribute_arithmetic_value(self):
        program = pointy_parser("Worker[port = 8000 + 80]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        names = {a.attr: a for a in node.options}
        self.assertIn("port", names)
        port_expr = names["port"].value
        # should be a BinOpNode combining two LiteralNodes
        from volnux.parser.ast import BinOpNode, LiteralNode

        self.assertIsInstance(port_expr, BinOpNode)
        self.assertEqual(port_expr.op, "+")
        self.assertIsInstance(port_expr.left, LiteralNode)
        self.assertEqual(port_expr.left.value, 8000)
        self.assertIsInstance(port_expr.right, LiteralNode)
        self.assertEqual(port_expr.right.value, 80)

    def test_task_attribute_variable_reference(self):
        # variable declaration followed by task using the variable
        program = pointy_parser("@v = 42 Worker[use = $v]")
        # ProgramNode with chain Run
        self.assertIsNotNone(program)
        self.assertTrue(hasattr(program, "chain"))
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        names = {a.attr: a for a in node.options}
        self.assertIn("use", names)
        # value should be a VariableAccessNode
        self.assertIsInstance(names["use"].value, VariableAccessNode)
        self.assertEqual(names["use"].value.name, "v")

    def test_grouped_with_attributes(self):
        program = pointy_parser("{Run}[opt = 1]")
        node = program.chain
        self.assertIsInstance(node, PipelineGroupingNode)
        # options attached to grouping
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 1)
        self.assertEqual(node.options[0].attr, "opt")

    def test_grouped_with_attributes_multiple(self):
        program = pointy_parser("{Run}[opt = 1, level = 2]")
        node = program.chain
        self.assertIsInstance(node, PipelineGroupingNode)
        self.assertIsInstance(node.options, list)
        self.assertEqual(len(node.options), 2)
        names = {a.attr: a for a in node.options}
        self.assertIn("opt", names)
        self.assertIn("level", names)
        self.assertEqual(names["opt"].value.value, 1)
        self.assertEqual(names["level"].value.value, 2)

    def test_task_attribute_null_coalesce(self):
        program = pointy_parser("Worker[opt = 1 ?? 2]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        names = {a.attr: a for a in node.options}
        self.assertIn("opt", names)
        # Null coalesce should produce NullCoalesceExprNode
        from volnux.parser.ast import NullCoalesceExprNode

        self.assertIsInstance(names["opt"].value, NullCoalesceExprNode)
        self.assertEqual(names["opt"].value.left.value, 1)
        self.assertEqual(names["opt"].value.right.value, 2)

    def test_task_attribute_ternary(self):
        program = pointy_parser("Worker[opt = (1 ?? 2) ? 3 : 4]")
        node = program.chain
        self.assertIsInstance(node, TaskNode)
        names = {a.attr: a for a in node.options}
        self.assertIn("opt", names)
        from volnux.parser.ast import TernaryExprNode, NullCoalesceExprNode

        self.assertIsInstance(names["opt"].value, TernaryExprNode)
        # condition should be NullCoalesce
        self.assertIsInstance(names["opt"].value.condition, NullCoalesceExprNode)
        self.assertEqual(names["opt"].value.true_expr.value, 3)
        self.assertEqual(names["opt"].value.false_expr.value, 4)

    def test_task_attribute_env_var(self):
        program = pointy_parser("Worker[path = $env.PATH]")
        node = program.chain
        names = {a.attr: a for a in node.options}
        self.assertIn("path", names)
        from volnux.parser.ast import EnvironmentVariableAccessNode

        self.assertIsInstance(names["path"].value, EnvironmentVariableAccessNode)
        self.assertEqual(names["path"].value.name, "PATH")

    # --- Negative tests for primary/chain ---
    def test_missing_closing_bracket_attribute_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("Worker[retries = 3")

    def test_malformed_map_literal_raises(self):
        # missing quotes around key should be a syntax error for map literal
        with self.assertRaises(SyntaxError):
            pointy_parser("Worker[config = {a: 1}]")

    # def test_empty_task_raises(self):
    #     # nothing where a task identifier is expected
    #     with self.assertRaises(SyntaxError):
    #         pointy_parser('')

    def test_invalid_namespace_raises_value_error(self):
        # TaskNode __post_init__ validates namespace membership and raises ValueError
        with self.assertRaises(ValueError):
            # 'unknown' is not in VALID_NAMESPACES
            pointy_parser("unknown::DoIt")


class TestGrammarRetry(unittest.TestCase):
    def test_retry_valid_task(self):
        program = pointy_parser("DoIt * 3")
        node = program.chain
        self.assertIsInstance(node, RetryNode)
        self.assertIsInstance(node.job, TaskNode)
        self.assertEqual(node.job.task, "DoIt")
        self.assertIsInstance(node.attempts, LiteralNode)
        self.assertEqual(node.attempts.value, 3)

    def test_retry_invalid_count_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("DoIt * 1")

    # def test_retry_on_grouped(self):
    #     program = pointy_parser("{Run} * 2")
    #     node = program.chain
    #     self.assertIsInstance(node, RetryNode)
    #     self.assertIsInstance(node.job, PipelineGroupingNode)
    #     self.assertIsInstance(node.attempts, LiteralNode)
    #     self.assertEqual(node.attempts.value, 2)

    # def test_retry_on_meta_event(self):
    #     program = pointy_parser("MAP<ProcessPayment> * 2")
    #     node = program.chain
    #     self.assertIsInstance(node, RetryNode)
    #     self.assertIsInstance(node.job, MetaTaskNode)
    #     self.assertEqual(node.attempts.value, 2)

    def test_retry_on_task_with_attributes(self):
        program = pointy_parser("Worker[retries = 3] * 4")
        node = program.chain
        self.assertIsInstance(node, RetryNode)
        self.assertIsInstance(node.job, TaskNode)
        self.assertEqual(node.job.task, "Worker")
        self.assertEqual(node.attempts.value, 4)
        names = {a.attr: a for a in node.job.options}
        self.assertEqual(names["retries"].value.value, 3)

    def test_retry_task_two_attributes(self):
        program = pointy_parser("Worker[retries = 3, timeout = 30] * 2")
        node = program.chain
        self.assertIsInstance(node, RetryNode)
        job = node.job
        self.assertIsInstance(job, TaskNode)
        names = {a.attr: a for a in job.options}
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)
        self.assertEqual(node.attempts.value, 2)

    def test_retry_task_three_attributes(self):
        program = pointy_parser("Worker[retries = 3, timeout = 30, verbose = true] * 3")
        node = program.chain
        self.assertIsInstance(node, RetryNode)
        job = node.job
        self.assertIsInstance(job, TaskNode)
        names = {a.attr: a for a in job.options}
        self.assertEqual(names["retries"].value.value, 3)
        self.assertEqual(names["timeout"].value.value, 30)
        self.assertEqual(names["verbose"].value.value, True)
        self.assertEqual(node.attempts.value, 3)

    def test_retry_namespaced_task_with_attributes(self):
        program = pointy_parser("pypi::Run[opt = 1, level = 2] * 2")
        node = program.chain
        self.assertIsInstance(node, RetryNode)
        job = node.job
        self.assertIsInstance(job, TaskNode)
        self.assertEqual(job.task, "Run")
        self.assertEqual(job.namespace, "pypi")
        names = {a.attr: a for a in job.options}
        self.assertEqual(names["opt"].value.value, 1)
        self.assertEqual(names["level"].value.value, 2)

    # def test_retry_meta_event_with_attributes(self):
    #     program = pointy_parser('MAP<FetchUserData>[concurrency = 4] * 2')
    #     node = program.chain
    #     self.assertIsInstance(node, RetryNode)
    #     self.assertIsInstance(node.job, MetaTaskNode)
    #     names = {a.attr: a for a in node.job.options}
    #     self.assertEqual(names["concurrency"].value.value, 4)
    #     self.assertEqual(node.attempts.value, 2)

    def test_retry_task_attribute_list_value(self):
        program = pointy_parser("Worker[params = [1, 2]] * 2")
        node = program.chain
        self.assertIsInstance(node, RetryNode)
        job = node.job
        self.assertIsInstance(job, TaskNode)
        names = {a.attr: a for a in job.options}
        from volnux.parser.ast import ListNode

        self.assertIsInstance(names["params"].value, ListNode)
        self.assertEqual(len(names["params"].value.value), 2)

    def test_retry_task_attribute_map_value(self):
        program = pointy_parser('Worker[config = {"a": 1}] * 2')
        node = program.chain
        job = node.job
        self.assertIsInstance(job, TaskNode)
        names = {a.attr: a for a in job.options}
        from volnux.parser.ast import MapNode

        self.assertIsInstance(names["config"].value, MapNode)
        self.assertIn("a", names["config"].value.value)

    def test_retry_task_attribute_arithmetic_value(self):
        program = pointy_parser("Worker[port = 8000 + 80] * 2")
        node = program.chain
        job = node.job
        names = {a.attr: a for a in job.options}
        from volnux.parser.ast import BinOpNode, LiteralNode

        self.assertIsInstance(names["port"].value, BinOpNode)
        self.assertEqual(names["port"].value.op, "+")

    def test_retry_task_attribute_variable_reference(self):
        program = pointy_parser("@v = 10 Worker[use = $v] * 2")
        node = program.chain
        job = node.job
        names = {a.attr: a for a in job.options}
        self.assertIsInstance(names["use"].value, VariableAccessNode)
        self.assertEqual(names["use"].value.name, "v")

    def test_retry_task_attribute_null_coalesce(self):
        program = pointy_parser("Worker[opt = 1 ?? 2] * 2")
        node = program.chain
        job = node.job
        names = {a.attr: a for a in job.options}
        from volnux.parser.ast import NullCoalesceExprNode

        self.assertIsInstance(names["opt"].value, NullCoalesceExprNode)

    def test_retry_task_attribute_ternary(self):
        program = pointy_parser("Worker[opt = (1 ?? 2) ? 3 : 4] * 2")
        node = program.chain
        job = node.job
        names = {a.attr: a for a in job.options}
        from volnux.parser.ast import TernaryExprNode

        self.assertIsInstance(names["opt"].value, TernaryExprNode)

    def test_retry_task_attribute_env_var(self):
        program = pointy_parser("Worker[path = $env.PATH] * 2")
        node = program.chain
        job = node.job
        names = {a.attr: a for a in job.options}
        from volnux.parser.ast import EnvironmentVariableAccessNode

        self.assertIsInstance(names["path"].value, EnvironmentVariableAccessNode)

    # def test_retry_grouped_with_multiple_attributes(self):
    #     program = pointy_parser('{Run}[opt = 1, level = 2] * 2')
    #     node = program.chain
    #     self.assertIsInstance(node, RetryNode)
    #     self.assertIsInstance(node.job, PipelineGroupingNode)
    #     names = {a.attr: a for a in node.job.options}
    #     self.assertEqual(names['opt'].value.value, 1)
    #     self.assertEqual(names['level'].value.value, 2)

    # def test_retry_meta_event_namespaced_no_attributes(self):
    #     program = pointy_parser('FILTER<ns::EnrichUserData> * 2')
    #     node = program.chain
    #     self.assertIsInstance(node, RetryNode)
    #     self.assertIsInstance(node.job, MetaTaskNode)
    #     self.assertEqual(node.job.template_event_namespace, 'ns')
    #     self.assertEqual(node.job.template_task, 'EnrichUserData')

    # def test_retry_grouped_expression_attribute_value(self):
    #     program = pointy_parser('{Run}[opt = [1,2]] * 2')
    #     node = program.chain
    #     self.assertIsInstance(node, RetryNode)
    #     self.assertIsInstance(node.job, PipelineGroupingNode)
    #     names = {a.attr: a for a in node.job.options}
    #     from volnux.parser.ast import ListNode
    #     self.assertIsInstance(names['opt'].value, ListNode)
    #     self.assertEqual(len(names['opt'].value.value), 2)

    # --- Negative retry tests (added) ---
    def test_retry_zero_count_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("DoIt * 0")

    def test_retry_negative_count_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("DoIt * -3")

    def test_retry_missing_count_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("DoIt *")

    def test_retry_nonint_token_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("DoIt * three")


if __name__ == "__main__":
    unittest.main()
