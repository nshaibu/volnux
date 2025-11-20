import multiprocessing
import shutil
import unittest

import pytest
from treelib import Tree

from volnux import EventBase, Pipeline
from volnux.constants import PIPELINE_FIELDS, PIPELINE_STATE
from volnux.exceptions import EventDoesNotExist, EventDone
from volnux.fields import FileInputDataField, InputDataField

# fix deadlock in python3.11 for state management
multiprocessing.set_start_method("spawn", force=True)


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class M(EventBase):
            def process(self, *args, **kwargs):
                return True, "hello world"

        class N(EventBase):
            def process(self, name):
                return True, name

        class Pipe(Pipeline):
            name = InputDataField(data_type=str, required=True)
            school = InputDataField(data_type=str, default="knust")
            csv_file = FileInputDataField(required=False)

            class Meta:
                pointy = "M->N"

        cls.M = M
        cls.N = N
        cls.pipeline_klass = Pipe

    def test_get_task_by_id(self):
        pipe = self.pipeline_klass(name="text")
        state_b = pipe._state.start.condition_node.on_success_event

        self.assertIsNotNone(state_b)

        task = pipe.get_task_by_id(state_b.id)
        self.assertIsNotNone(task)
        self.assertEqual(task, state_b)

    def test_get_pipeline_fields(self):
        pipe = self.pipeline_klass(name="text")
        self.assertTrue(
            all(
                [
                    field_name in ["name", "school", "csv_file"]
                    for field_name, _ in pipe.get_fields()
                ]
            )
        )

    def test_pipeline_file_fields(self):
        pipe = self.pipeline_klass(name="name", csv_file="tests/unittests/test_task.py")
        self.assertIsNotNone(pipe.csv_file)

        with pytest.raises(ValueError):
            self.pipeline_klass(name="name", csv_file="none.file")

        with pytest.raises(TypeError):
            self.pipeline_klass()

    def test_pipeline_start(self):
        pipe = self.pipeline_klass(name="test_name")
        try:
            pipe.start()
        except EventDone:
            self.fail("Pipeline raised EventDone unexpectedly!")

    def test_pipeline_shutdown(self):
        pipe = self.pipeline_klass(name="test_name")
        pipe.start()
        try:
            pipe.shutdown()
        except Exception as e:
            self.fail(f"Pipeline shutdown raised an exception: {e}")

    def test_pipeline_stop(self):
        pipe = self.pipeline_klass(name="test_name")
        pipe.start()
        try:
            pipe.stop()
        except Exception as e:
            self.fail(f"Pipeline stop raised an exception: {e}")

    def test_pipeline_get_cache_key(self):
        pipe = self.pipeline_klass(name="test_name")
        cache_key = pipe.get_cache_key()
        self.assertEqual(cache_key, f"pipeline_{pipe.__class__.__name__}_{pipe.id}")

    def test_pipeline_get_task_by_id_invalid(self):
        pipe = self.pipeline_klass(name="test_name")
        with pytest.raises(EventDoesNotExist):
            pipe.get_task_by_id("invalid_id")

    def test_pipeline_get_pipeline_tree(self):
        pipe = self.pipeline_klass(name="test_name")
        tree = pipe.get_pipeline_tree()
        self.assertIsNotNone(tree)
        self.assertTrue(isinstance(tree, Tree))

    def test_pipeline_draw_ascii_graph(self):
        pipe = self.pipeline_klass(name="test_name")
        try:
            pipe.draw_ascii_graph()
        except Exception as e:
            self.fail(f"Drawing ASCII graph raised an exception: {e}")

    @pytest.mark.skip(reason="Not implemented yet")
    def test_pipeline_load_class_by_id(self):
        pipe = self.pipeline_klass(name="test_name")
        cache_key = pipe.get_cache_key()
        loaded_pipe = self.pipeline_klass.load_class_by_id(cache_key)
        self.assertIsNotNone(loaded_pipe)
        self.assertEqual(loaded_pipe.get_cache_key(), cache_key)

    def test_pipeline_equality(self):
        pipe1 = self.pipeline_klass(name="test1")
        pipe2 = self.pipeline_klass(name="test1")
        pipe3 = self.pipeline_klass(name="test2")

        self.assertEqual(pipe1, pipe1)
        self.assertNotEqual(pipe1, pipe2)  # Different IDs
        self.assertNotEqual(pipe1, pipe3)
        self.assertNotEqual(pipe1, "not a pipeline")

    def test_pipeline_hash(self):
        pipe = self.pipeline_klass(name="test")
        try:
            hash(pipe)
        except Exception as e:
            self.fail(f"Pipeline hash raised an exception: {e}")

    def test_pipeline_state_persistence(self):
        pipe = self.pipeline_klass(name="test")
        state = pipe.__getstate__()
        pipe.__setstate__(state)

        self.assertIsNotNone(pipe._state)
        self.assertIsNotNone(pipe._state.pipeline_cache)

    def test_pipeline_rerun(self):
        pipe = self.pipeline_klass(name="test")
        pipe.start()

        with pytest.raises(EventDone):
            pipe.start()  # Should raise without force_rerun

        try:
            pipe.start(force_rerun=True)  # Should work with force_rerun
        except EventDone:
            self.fail("Pipeline start with force_rerun raised EventDone!")

    def test_pipeline_get_non_batch_fields(self):
        fields = list(self.pipeline_klass.get_non_batch_fields())
        self.assertEqual(len(fields), 2)

    @pytest.mark.skipif(
        shutil.which("dot") is None, reason="'dot' command not found in PATH"
    )
    def test_pipeline_draw_graphviz_image(self):
        pipe = self.pipeline_klass(name="test_name")
        try:
            pipe.draw_graphviz_image()
        except Exception as e:
            self.fail(f"Drawing Graphviz image raised an exception: {e}")

    def test_pipeline_tree_structure(self):
        pipe = self.pipeline_klass(name="test_name")
        tree = pipe.get_pipeline_tree()
        self.assertIsNotNone(tree)
        self.assertTrue(tree.contains(pipe._state.start.id))
        self.assertTrue(tree.depth() > 0)

    def test_pipeline_task_execution_order(self):
        pipe = self.pipeline_klass(name="test_name")
        pipe.start()
        task_order = []
        current_task = pipe._state.start
        while current_task:
            task_order.append(current_task.id)
            current_task = current_task.condition_node.on_success_event
        self.assertTrue(len(task_order) > 0)
        self.assertEqual(len(task_order), len(set(task_order)))  # No duplicate tasks

    def test_pipeline_cache_mechanism(self):
        pipe = self.pipeline_klass(name="test_name")
        pipe.start()
        cache = pipe._state.cache(pipe)
        self.assertIn("name", cache)
        self.assertIsInstance(cache["name"], str)

    def test_pipeline_invalid_field_initialization(self):
        with pytest.raises(TypeError):
            self.pipeline_klass(name=123)  # Invalid type for 'name'

    def test_pipeline_meta_class_initialization(self):
        try:

            class TestPipeline(Pipeline):
                test_field = InputDataField(data_type=str, required=True)

                class Meta:
                    pointy = "M->N"

            self.assertTrue(hasattr(TestPipeline, PIPELINE_FIELDS))
            self.assertTrue(hasattr(TestPipeline, PIPELINE_STATE))
        except Exception as e:
            self.fail(f"PipelineMeta initialization raised an exception: {e}")

    def test_pipeline_shutdown_without_start(self):
        pipe = self.pipeline_klass(name="test_name")
        try:
            pipe.shutdown()
        except Exception as e:
            self.fail(f"Pipeline shutdown without start raised an exception: {e}")

    def test_pipeline_stop_without_start(self):
        pipe = self.pipeline_klass(name="test_name")
        try:
            pipe.stop()
        except Exception as e:
            self.fail(f"Pipeline stop without start raised an exception: {e}")

    @classmethod
    def tearDownClass(cls):
        del cls.M
        del cls.N
        del cls.pipeline_klass
