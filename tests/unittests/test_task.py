import unittest
from collections import deque
from concurrent.futures import Executor

from volnux import EventBase, Pipeline
from volnux.execution.context import ExecutionContext
from volnux.parser.operator import PipeType
from volnux.task import PipelineTask, build_pipeline_flow_from_pointy_code


class TestTask(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class A(EventBase):
            def process(self, *args, **kwargs):
                return True, "hello world"

        class B(EventBase):
            def process(self, name):
                return True, name

        class C(EventBase):
            def process(self, *args, **kwargs):
                return True, self.previous_result

        class S(EventBase):
            def process(self, *args, **kwargs):
                return True, "Sink"

        cls.A = A
        cls.B = B
        cls.C = C
        cls.S = S

    def test_build_event_pipeline_for_line_execution(self):
        p, _ = build_pipeline_flow_from_pointy_code("A->B->C")

        self.assertIsInstance(p, PipelineTask)
        self.assertIsNotNone(p.id)
        self.assertIsInstance(p.condition_node.on_success_event, PipelineTask)
        self.assertIsInstance(
            p.condition_node.on_success_event.condition_node.on_success_event,
            PipelineTask,
        )
        self.assertEqual(p.condition_node.on_success_pipe, PipeType.POINTER)
        self.assertEqual(
            p.condition_node.on_success_event.condition_node.on_success_pipe,
            PipeType.POINTER,
        )
        self.assertEqual(
            p.condition_node.on_success_event.condition_node.on_success_event.condition_node.on_success_pipe,
            None,
        )

    def test_build_event_pipeline_with_result_piping_and_parallel_execution(self):
        p, _ = build_pipeline_flow_from_pointy_code("A||B|->C")

        self.assertIsInstance(p, PipelineTask)
        self.assertEqual(p.condition_node.on_success_pipe, PipeType.PARALLELISM)
        self.assertEqual(
            p.condition_node.on_success_event.condition_node.on_success_pipe,
            PipeType.PIPE_POINTER,
        )

    def test_build_event_pipeline_with_conditional_branching(self):
        p, _ = build_pipeline_flow_from_pointy_code("A(0->B,1->C)->S")

        self.assertIsInstance(p, PipelineTask)
        self.assertTrue(p.is_conditional)
        self.assertTrue(p.condition_node.on_success_event.is_descriptor_task)
        self.assertTrue(p.condition_node.on_failure_event.is_descriptor_task)
        self.assertIsNotNone(p.sink_node)
        self.assertEqual(p.sink_pipe, PipeType.POINTER)
        self.assertTrue(p.sink_node.is_sink)

    def test_resolve_event_name(self):
        self.assertTrue(issubclass(PipelineTask.resolve_event_name("A"), EventBase))
        self.assertEqual(PipelineTask.resolve_event_name("A"), self.A)

    def test_pointer_to_event(self):
        p, _ = build_pipeline_flow_from_pointy_code("A->B")
        self.assertIsNone(p.get_pointer_to_task())
        self.assertEqual(p.condition_node.on_success_event.event, "B")
        self.assertEqual(
            p.condition_node.on_success_event.get_pointer_to_task(), PipeType.POINTER
        )

    def test_count_nodes(self):
        p, _ = build_pipeline_flow_from_pointy_code("A->B->C")
        self.assertEqual(p.get_task_count(), 3)
        self.assertEqual(p.get_event_klass(), self.A)

    def test_get_root(self):
        p, _ = build_pipeline_flow_from_pointy_code("A->B->C")
        self.assertEqual(
            p.condition_node.on_success_event.condition_node.on_success_event.get_root().event,
            "A",
        )

    def test_get_children(self):
        p, _ = build_pipeline_flow_from_pointy_code("A(0->B,1->C)->S")
        p1, _ = build_pipeline_flow_from_pointy_code("A->B->C")
        self.assertEqual(len(p.get_children()), 3)
        self.assertEqual(len(p1.get_children()), 1)

    def test_pipeline_retry_syntax(self):
        p, _ = build_pipeline_flow_from_pointy_code("2 * A -> B * 4 ->C")
        self.assertEqual(p.options.retry_attempts, 2)
        self.assertEqual(p.condition_node.on_success_event.options.retry_attempts, 4)
        self.assertIsNone(
            p.condition_node.on_success_event.condition_node.on_success_event.options
        )

    def test_syntax_error_wrong_descriptor(self):
        with self.assertRaises(SyntaxError):
            build_pipeline_flow_from_pointy_code("A(10->C,40->B)")

    def test_syntax_error_wrong_retry_factor(self):
        with self.assertRaises(SyntaxError):
            build_pipeline_flow_from_pointy_code("1 * A -> B * 0")

        with self.assertRaises(SyntaxError):
            build_pipeline_flow_from_pointy_code("-1 * A")

    def test_multi_condition_conditional_branching(self):
        # No implemented yet
        self.assertTrue(True)

    @classmethod
    def tearDownClass(cls):
        del cls.A
        del cls.B
        del cls.C
        del cls.S

