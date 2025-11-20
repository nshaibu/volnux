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
        p = build_pipeline_flow_from_pointy_code("A->B->C")

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
        p = build_pipeline_flow_from_pointy_code("A||B|->C")

        self.assertIsInstance(p, PipelineTask)
        self.assertEqual(p.condition_node.on_success_pipe, PipeType.PARALLELISM)
        self.assertEqual(
            p.condition_node.on_success_event.condition_node.on_success_pipe,
            PipeType.PIPE_POINTER,
        )

    def test_build_event_pipeline_with_conditional_branching(self):
        p = build_pipeline_flow_from_pointy_code("A(0->B,1->C)->S")

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
        p = build_pipeline_flow_from_pointy_code("A->B")
        self.assertIsNone(p.get_pointer_to_task())
        self.assertEqual(p.condition_node.on_success_event.event, "B")
        self.assertEqual(
            p.condition_node.on_success_event.get_pointer_to_task(), PipeType.POINTER
        )

    def test_count_nodes(self):
        p = build_pipeline_flow_from_pointy_code("A->B->C")
        self.assertEqual(p.get_task_count(), 3)
        self.assertEqual(p.get_event_klass(), self.A)

    def test_get_root(self):
        p = build_pipeline_flow_from_pointy_code("A->B->C")
        self.assertEqual(
            p.condition_node.on_success_event.condition_node.on_success_event.get_root().event,
            "A",
        )

    def test_get_children(self):
        p = build_pipeline_flow_from_pointy_code("A(0->B,1->C)->S")
        p1 = build_pipeline_flow_from_pointy_code("A->B->C")
        self.assertEqual(len(p.get_children()), 3)
        self.assertEqual(len(p1.get_children()), 1)

    def test_pipeline_retry_syntax(self):
        p = build_pipeline_flow_from_pointy_code("2 * A -> B * 4 ->C")
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


"""
class ExecutionContextTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        class AB(EventBase):
            # execution_evaluation_state = (
            #     EventExecutionEvaluationState.SUCCESS_ON_ALL_EVENTS_SUCCESS
            # )

            def process(self, *args, **kwargs):
                return True, "hello world"

        class BC(EventBase):
            # execution_evaluation_state = (
            #     EventExecutionEvaluationState.FAILURE_FOR_PARTIAL_ERROR
            # )

            def process(self, name):
                return True, name

        class CD(EventBase):
            # execution_evaluation_state = (
            #     EventExecutionEvaluationState.SUCCESS_FOR_PARTIAL_SUCCESS
            # )

            def process(self, *args, **kwargs):
                return True, self.previous_result

        class Sink(EventBase):
            # execution_evaluation_state = (
            #     EventExecutionEvaluationState.FAILURE_FOR_ALL_EVENTS_FAILURE
            # )

            def process(self, *args, **kwargs):
                return True, "Sink"

        class Pipe3(Pipeline):
            class Meta:
                pointy = "AB||BC||CD"

        cls.AB = AB
        cls.BC = BC
        cls.CD = CD
        cls.Sink = Sink
        cls.Pipe3 = Pipe3

    def setUp(self):
        self.pipeline3 = self.Pipe3()

    def test__gather_executors_for_parallel_executions(self):
        queue = self.pipeline3._state.start
        tasks = []
        while queue:
            tasks.append(queue)
            queue = queue.on_success_event

        execution_context = ExecutionContext(tasks, self.pipeline3)
        executors_map = execution_context._gather_executors_for_parallel_executions()
        self.assertIsNotNone(executors_map)
        self.assertIsInstance(executors_map, dict)

        for executor, _map in executors_map.items():
            self.assertTrue(issubclass(executor, Executor))
            self.assertIsInstance(_map, dict)
            self.assertIsInstance(_map["context"], dict)

    def test__get_last_task_profile_in_chain(self):
        queue = self.pipeline3._state.start
        tasks = deque()
        while queue:
            tasks.append(queue)
            queue = queue.on_success_event

        execution_context = EventExecutionContext(list(tasks), self.pipeline3)

        self.assertEqual(
            tasks.pop(), execution_context._get_last_task_profile_in_chain()
        )
"""
