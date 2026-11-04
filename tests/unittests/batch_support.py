"""Pipelines and batches for the BatchPipeline tests.

They live in an importable module (not inside a test function) because the
process pool is spawn-based: a worker must be able to import these classes to
unpickle the pipelines it is given.
"""

import time
import typing

from volnux import EventBase
from volnux.exceptions import PipelineExecutionError
from volnux.execution.pipeline import BatchPipeline, Pipeline
from volnux.execution.pipeline.fields import FileInputDataField, InputDataField


class BatchEcho(EventBase):
    def process(self, *args, **kwargs):
        return True, "echo"


class BatchSleepy(EventBase):
    """Holds a worker for a while (seconds come from the environment)."""

    def process(self, *args, **kwargs):
        import os

        time.sleep(float(os.environ.get("VOLNUX_TEST_SLEEP", "0.3")))
        return True, "slept"


class EchoPipeline(Pipeline):
    items = InputDataField(data_type=list, batch_size=2)
    label = InputDataField(data_type=str, default="x")

    class Meta:
        pointy = "BatchEcho"


class TypedPipeline(Pipeline):
    ids = InputDataField(data_type=typing.List[int], batch_size=2)

    class Meta:
        pointy = "BatchEcho"


class SleepyPipeline(Pipeline):
    items = InputDataField(data_type=list, batch_size=1)

    class Meta:
        pointy = "BatchSleepy"


class ScalarPipeline(Pipeline):
    """No batch field: the batch runs it once, in this process."""

    label = InputDataField(data_type=str, default="solo")

    class Meta:
        pointy = "BatchEcho"


class EchoBatch(BatchPipeline):
    pipeline_template = EchoPipeline
    max_workers = 2


class TypedBatch(BatchPipeline):
    pipeline_template = TypedPipeline
    max_workers = 2


class SleepyBatch(BatchPipeline):
    pipeline_template = SleepyPipeline
    max_workers = 2
    max_in_flight = 2

    def _initialise_template(self, **kwargs):
        # what the window looked like each time a pipeline was created
        self.__dict__.setdefault("seen_in_flight", []).append(self._in_flight)
        self.__dict__.setdefault("created_at", []).append(time.monotonic())
        return super()._initialise_template(**kwargs)


class ScalarBatch(BatchPipeline):
    pipeline_template = ScalarPipeline


class RaisingWorkerBatch(EchoBatch):
    """A worker that blows up before the pipeline even runs."""

    @staticmethod
    def _pipeline_executor(pipeline, focus_on_signals, signals_queue):
        raise ValueError("worker exploded")


class FailedPipelineBatch(EchoBatch):
    """A worker whose pipeline ended failed (the wrapper's (pipeline, exc))."""

    @staticmethod
    def _pipeline_executor(pipeline, focus_on_signals, signals_queue):
        return pipeline, PipelineExecutionError("ended failed at task: BatchEcho")


class FilePipeline(Pipeline):
    book = FileInputDataField(chunk_size=10)

    class Meta:
        pointy = "BatchEcho"


class FileBatch(BatchPipeline):
    pipeline_template = FilePipeline
    max_workers = 2
