import logging
import time
from multiprocessing.queues import Queue
from unittest.mock import Mock, patch

import pytest

from volnux import EventBase
from volnux.fields import InputDataField
from volnux.execution.pipeline import Pipeline
from volnux.execution.pipeline.wrapper import PipelineExecutionState, PipelineWrapper


class Fetch(EventBase):
    def process(self, name, data):
        return True, "Fetch"


class Process(EventBase):
    def process(self, name, data):
        return True, "Process"


class Save(EventBase):
    def process(self, name, data):
        return True, "Save"


class DummyPipeline(Pipeline):
    numbers = InputDataField()

    class Meta:
        pointy = "Fetch -> Process -> Save"


@pytest.fixture
def mock_logger():
    return Mock(spec=logging.Logger)


@pytest.fixture
def mock_queue():
    return Mock(spec=Queue)


@pytest.fixture
def basic_wrapper(mock_logger, mock_queue):
    pipeline = DummyPipeline(numbers=[1, 2, 3])
    return PipelineWrapper(
        pipeline=pipeline,
        focus_on_signals=[
            "volnux.signal.signals.pipeline_execution_start",
            "volnux.signal.signals.pipeline_execution_end",
        ],
        signals_queue=mock_queue,
        import_string_fn=lambda x: Mock(),
        logger=mock_logger,
        wrapper_id="test_wrapper",
    )


class TestPipelineWrapper:
    def test_initialization(self, basic_wrapper):
        """Test proper initialization of PipelineWrapper"""
        assert basic_wrapper.execution_state == PipelineExecutionState.INITIALIZED
        assert basic_wrapper.start_time is None
        assert basic_wrapper.end_time is None
        assert basic_wrapper.exception is None
        assert basic_wrapper.wrapper_id == "test_wrapper"

    def test_connect_signals(self, basic_wrapper, mock_queue):
        """Test signal connection functionality"""
        basic_wrapper._connect_signals()
        assert len(basic_wrapper._connected_signals) > 0

        # Test signal handler by simulating a signal
        signal_handler = basic_wrapper._connected_signals[0].connect.call_args[1][
            "listener"
        ]
        signal_handler(sender=DummyPipeline, pipeline=basic_wrapper.pipeline)

        # Verify queue message format
        mock_queue.put.assert_called_once()
        message = mock_queue.put.call_args[0][0]
        assert message["message_type"] == "pipeline_signal"
        assert "wrapper_id" in message
        assert "timestamp" in message
        assert message["version"] == "1"

    def test_disconnect_signals(self, basic_wrapper):
        """Test signal disconnection"""
        mock_signal = Mock()
        basic_wrapper._connected_signals = [mock_signal]
        basic_wrapper._disconnect_signals()

        mock_signal.disconnect.assert_called_once()
        assert len(basic_wrapper._connected_signals) == 0

    def test_successful_pipeline_execution(self, basic_wrapper, mock_queue):
        """Test successful pipeline execution flow"""
        mock_context = Mock()
        mock_context.execution_failed.return_value = False
        mock_context.get_latest_execution_context.return_value = mock_context

        with patch.object(basic_wrapper.pipeline, "start") as mock_start:
            mock_start.return_value = mock_context

            pipeline, exception = basic_wrapper.run()

            assert pipeline == basic_wrapper.pipeline
            assert exception is None
            assert basic_wrapper.execution_state == PipelineExecutionState.FINISHED

            mock_start.assert_called_once()

            expected_events = [
                "wrapper_started",
                "pipeline_execution_start",
                "pipeline_execution_end",
                "wrapper_finished",
            ]

            # Verify lifecycle events were sent
            lifecycle_events = [
                call[0][0]["event_type"]
                for call in mock_queue.put.call_args_list
                if call[0][0].get("message_type") == "wrapper_lifecycle"
            ]
            assert all(event in lifecycle_events for event in expected_events)
            # Verify events occurred in correct order
            assert [
                e for e in lifecycle_events if e in expected_events
            ] == expected_events

    def test_failed_pipeline_execution(self, basic_wrapper, mock_queue):
        """Test pipeline execution failure handling"""
        test_exception = ValueError("Test error")

        with patch.object(basic_wrapper.pipeline, "start", side_effect=test_exception):
            pipeline, exception = basic_wrapper.run()

            assert pipeline == basic_wrapper.pipeline
            assert exception == test_exception
            assert basic_wrapper.execution_state == PipelineExecutionState.FINISHED

            # Verify error event was sent
            error_events = [
                call[0][0]
                for call in mock_queue.put.call_args_list
                if call[0][0].get("event_type") == "wrapper_failed"
            ]
            assert len(error_events) == 1
            assert "error_message" in error_events[0]
            assert "Test error" in error_events[0]["error_message"]

    def test_execution_timing(self, basic_wrapper):
        """Test execution timing measurement"""
        with patch.object(basic_wrapper.pipeline, "start"):
            start_time = time.time()
            basic_wrapper.run()

            assert basic_wrapper.start_time >= start_time
            assert basic_wrapper.end_time >= basic_wrapper.start_time

            # Verify execution duration in wrapper_finished event
            finished_event = [
                call[0][0]
                for call in basic_wrapper.signals_queue.put.call_args_list
                if call[0][0].get("event_type") == "wrapper_finished"
            ][0]
            assert "execution_duration" in finished_event
            assert finished_event["execution_duration"] >= 0

    def test_pipeline_execution_state_transitions(self, basic_wrapper):
        """Test state transitions during pipeline execution"""
        states = []

        def record_state_change():
            states.append(basic_wrapper.execution_state)

        def mock_handle_execution():
            basic_wrapper.execution_state = PipelineExecutionState.RUNNING
            states.append(basic_wrapper.execution_state)

        with patch.object(basic_wrapper.pipeline, "start"):
            states.append(basic_wrapper.execution_state)  # Initial state

            with patch.object(
                basic_wrapper, "_connect_signals", side_effect=record_state_change
            ), patch.object(
                basic_wrapper, "_send_wrapper_lifecycle_event", return_value=None
            ), patch.object(
                basic_wrapper,
                "_handle_pipeline_execution",
                side_effect=mock_handle_execution,
            ):
                basic_wrapper.run()
        assert states == [
            PipelineExecutionState.INITIALIZED,
            PipelineExecutionState.INITIALIZING,
            PipelineExecutionState.RUNNING,
        ]
        assert basic_wrapper.execution_state == PipelineExecutionState.FINISHED

    @pytest.mark.parametrize(
        "signal_import_error", [ImportError, AttributeError, TypeError]
    )
    def test_signal_connection_error_handling(self, basic_wrapper, signal_import_error):
        """Test handling of various signal connection errors"""

        def raise_error(x):
            raise signal_import_error("Test error")

        wrapper = PipelineWrapper(
            pipeline=basic_wrapper.pipeline,
            focus_on_signals=["invalid.signal.path"],
            signals_queue=Mock(),
            import_string_fn=raise_error,
            logger=Mock(),
        )

        # Should not raise exception
        wrapper._connect_signals()

        # Verify warning was logged
        wrapper._logger.warning.assert_called_once()
        assert "Signal import failed" in wrapper._logger.warning.call_args[0][0]

    def test_queue_put_timeout_handling(self, basic_wrapper):
        """Test handling of queue.put timeout"""
        mock_queue = Mock(spec=Queue)
        mock_queue.put.side_effect = TimeoutError("Queue full")

        wrapper = PipelineWrapper(
            pipeline=basic_wrapper.pipeline,
            focus_on_signals=["volnux.signal.signals.pipeline_execution_start"],
            signals_queue=mock_queue,
            import_string_fn=lambda x: Mock(),
            logger=Mock(),
        )

        # Should not raise exception
        wrapper._send_wrapper_lifecycle_event("test_event")

        # Verify error was logged
        wrapper._logger.error.assert_called_once()
        assert (
            "Failed to send wrapper lifecycle event"
            in wrapper._logger.error.call_args[0][0]
        )

    def test_pipeline_state_info_collection(self, basic_wrapper):
        """Test collection of pipeline state information"""
        state_info = basic_wrapper._get_pipeline_state_info()

        assert isinstance(state_info, dict)
        assert "pipeline_id" in state_info
        assert "pipeline_class" in state_info
        assert "execution_context_exists" in state_info
        assert "has_cache" in state_info

    def test_exception_traceback_formatting(self, basic_wrapper):
        """Test exception traceback formatting"""
        try:
            raise ValueError("Test error")
        except ValueError as e:
            formatted = basic_wrapper._format_exception_traceback(e)

        assert isinstance(formatted, str)
        assert "ValueError" in formatted
        assert "Test error" in formatted
