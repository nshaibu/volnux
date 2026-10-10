import itertools
import time
import multiprocessing as mp
import unittest
from typing import Iterator, List
from unittest.mock import Mock, patch

import pytest

from volnux import EventBase
from volnux.exceptions import ImproperlyConfigured
from volnux.fields import InputDataField
from volnux.execution.pipeline import Pipeline
from volnux.execution.pipeline.batch import (
    BatchPipeline,
    BatchPipelineStatus,
    _BatchProcessingMonitor,
)
from volnux.config import VolnuxConfig


class Start(EventBase):
    def process(self, name, data):
        return True, "Start"


class Process(EventBase):
    def process(self, name, data):
        return True, "Process"


class End(EventBase):
    def process(self, name, data):
        return True, "End"


class DummyPipeline(Pipeline):
    data = InputDataField(data_type=list, batch_size=2)
    name = InputDataField(data_type=str, required=False)

    class Meta:
        pointy = "Start -> Process -> End"


class TestBatchPipeline(unittest.TestCase):
    def setUp(self):
        class TestBatch(BatchPipeline):
            pipeline_template = DummyPipeline

        self.batch_cls = TestBatch

    def test_initialization(self):
        """Test basic initialization of BatchPipeline"""
        batch = self.batch_cls(data=[1, 2, 3, 4], name="test")
        self.assertEqual(batch.data, [1, 2, 3, 4])
        self.assertEqual(batch.name, "test")
        self.assertEqual(batch.status, BatchPipelineStatus.PENDING)
        self.assertIsInstance(batch.lock, mp.synchronize.Lock)
        self.assertEqual(batch.results, [])

    def test_invalid_pipeline_template(self):
        """Test initialization with invalid pipeline template"""

        class InvalidBatch(BatchPipeline):
            pipeline_template = str  # Invalid template

        with self.assertRaises(ImproperlyConfigured):
            InvalidBatch(data=[1, 2])

    def test_batch_processing(self):
        """Test batch processing with multiple items"""
        batch = self.batch_cls(data=[1, 2, 3, 4])
        batch.execute()

        # Check if all pipelines were executed
        self.assertEqual(batch._configured_pipelines_count, 2)  # 2 batches of 2 items
        self.assertEqual(batch.status, BatchPipelineStatus.RUNNING)

    def test_single_pipeline_execution(self):
        """Test execution with single pipeline"""
        batch = self.batch_cls(data=[1])
        batch.execute()

        # Should only create one pipeline
        self.assertEqual(batch._configured_pipelines_count, 1)

    @patch("volnux.execution.pipeline.batch.ProcessPoolExecutor")
    def test_parallel_execution(self, mock_executor):
        """Test parallel execution of multiple pipelines"""
        mock_executor.return_value.__enter__.return_value = Mock()

        batch = self.batch_cls(data=[1, 2, 3, 4, 5, 6])
        batch.execute()

        # Verify ProcessPoolExecutor was used
        mock_executor.assert_called_once()

    def test_custom_batch_processor_with_wrong_call_signature(self):
        """Test custom batch processor method"""

        class WrongBatch(BatchPipeline):
            pipeline_template = DummyPipeline

            def data_batch(self, items: List[int], batch_size: int) -> Iterator[int]:
                for i in range(0, len(items), batch_size):
                    yield items[i : i + batch_size]

        batch = WrongBatch(data=[1, 2, 3, 4])
        with self.assertRaises(ImproperlyConfigured):
            batch._gather_and_init_field_batch_iterators()

    def test_custom_batch_processor(self):
        """Test custom batch processor method"""

        class CustomBatch(BatchPipeline):
            pipeline_template = DummyPipeline

            def data_batch(self, values: List[int], batch_size: int) -> Iterator[int]:
                for i in range(0, len(values), batch_size):
                    yield values[i : i + batch_size]

        batch = CustomBatch(data=[1, 2, 3, 4])
        batch._gather_and_init_field_batch_iterators()

        # Verify custom batch processor was used
        self.assertTrue(
            any(
                field in batch._field_batch_op_map
                for field_name, field in batch.get_fields()
            )
        )

    def test_invalid_batch_processor(self):
        """Test invalid batch processor"""

        class InvalidBatch(BatchPipeline):
            pipeline_template = DummyPipeline

            def data_batch(self, items, batch_size):
                return items  # Not an iterator

        batch = InvalidBatch(data=[1, 2, 3])
        with self.assertRaises(ImproperlyConfigured):
            batch._gather_and_init_field_batch_iterators()

        del batch

    def test_empty_batch(self):
        """Test handling of empty batch"""
        batch = self.batch_cls(data=[])
        with self.assertRaises(Exception):
            batch.execute()
        self.assertEqual(batch._configured_pipelines_count, 0)

    @pytest.mark.skip("Hanging fix it later")
    def test_signal_handling(self):
        """Test signal handling during batch processing"""

        class SignalTestBatch(BatchPipeline):
            pipeline_template = DummyPipeline
            listen_to_signals = ["pipeline_execution_start"]

        batch = SignalTestBatch(data=[1, 2, 3, 4])
        with patch("multiprocessing.Manager") as mock_manager:
            mock_queue = Mock()
            mock_manager.return_value.Queue.return_value = mock_queue
            batch.execute()
            # Verify signal queue was created
            self.assertIsNotNone(batch._signals_queue)

    @patch("psutil.Process")
    def test_memory_management(self, mock_process):
        """Test memory management and batch size adjustment"""
        batch = self.batch_cls(data=list(range(100)))
        field = next(batch.get_fields())[1]
        field.batch_size = 20
        batch._field_batch_op_map = {field: iter(range(100))}
        batch.max_memory_percent = 90.0

        # Test normal memory usage scenario
        mock_process.return_value.memory_percent.return_value = 80.0
        batch.check_memory_usage()
        self.assertEqual(field.batch_size, 20)

        # Test high memory usage scenario - should reduce by 20%
        mock_process.return_value.memory_percent.return_value = 95.0
        batch.check_memory_usage()
        self.assertEqual(field.batch_size, 16)

        # Test successive memory pressure - another 20% reduction
        mock_process.return_value.memory_percent.return_value = 95.0
        batch.check_memory_usage()
        self.assertEqual(field.batch_size, 12)

        # Test minimum batch size limit
        for _ in range(10):
            batch.check_memory_usage()
        self.assertEqual(field.batch_size, 1)

    @patch("psutil.Process")
    def test_memory_check_in_executor(self, mock_process):
        """Test memory checking during execution"""
        batch = self.batch_cls(data=list(range(100)))

        mock_process.return_value.memory_percent.return_value = 95.0

        with patch.object(batch, "_adjust_batch_size") as mock_adjust:
            batch.check_memory_usage()
            mock_adjust.assert_called_once()

        # Test no adjustment needed
        mock_process.return_value.memory_percent.return_value = 85.0
        with patch.object(batch, "_adjust_batch_size") as mock_adjust:
            batch.check_memory_usage()
            mock_adjust.assert_not_called()

    @patch("psutil.Process")
    @pytest.mark.skip("Needs to be fixed later. Affecting test_batch_processing test")
    def test_batch_monitor_memory_check(self, mock_process):
        """Test memory monitoring in BatchProcessingMonitor during execution"""
        batch = self.batch_cls(data=list(range(10)))

        mock_process.return_value.memory_percent.side_effect = itertools.cycle(
            [85.0, 95.0, 95.0, 85.0]
        )

        field = next(batch.get_fields())[1]
        original_batch_size = field.batch_size = 10
        batch._field_batch_op_map = {field: iter(range(10))}

        monitor = _BatchProcessingMonitor(batch)
        try:
            monitor.start()

            time.sleep(0.1)

            self.assertLess(field.batch_size, original_batch_size)

        finally:
            monitor._shutdown_flag.set()
            monitor.join(timeout=1.0)

            if monitor.is_alive():
                monitor._cleanup_signal_listeners()

    def test_executor_config(self):
        """Test executor configuration settings"""
        batch = self.batch_cls(data=[1, 2, 3, 4])
        conf = VolnuxConfig.get_instance()

        # Test default config
        config = batch.get_executor_config
        self.assertEqual(config["max_workers"], conf.MAX_BATCH_PROCESSING_WORKERS)
        self.assertEqual(config["max_memory_percent"], 90.00)

        # Test with custom settings
        batch.max_workers = 4
        batch.max_memory_percent = 85.0
        config = batch.get_executor_config

        self.assertEqual(config["max_workers"], 4)
        self.assertEqual(config["max_memory_percent"], 85.0)
        self.assertIsNotNone(config["mp_context"])

    def tearDown(self):
        # Clean up any resources
        pass
