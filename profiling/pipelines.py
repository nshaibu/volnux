from volnux import Pipeline
from volnux.pipeline import BatchPipeline
from volnux.execution.pipeline.fields import InputDataField

# this is done because for some reason you have to import the events when you want to run it as a batch pipeline
from events import EventOne, EventTwo, EventThree, EventFour


class LinearPipeline(Pipeline):
    name = InputDataField(data_type=list)
    age = InputDataField(data_type=int)


class ParallelPipeline(Pipeline):
    name = InputDataField(data_type=list)
    age = InputDataField(data_type=int)


class DecisionTreePipeline(Pipeline):
    name = InputDataField(data_type=list)
    age = InputDataField(data_type=int)


class BatchPipelineTemplate(Pipeline):
    name = InputDataField(data_type=list, batch_size=1)
    age = InputDataField(data_type=int)


class BatchPipelineType(BatchPipeline):
    pipeline_template = BatchPipelineTemplate

    def start(self):
        self.execute()


class LinearPipelineWithPreviousResult(Pipeline):
    name = InputDataField(data_type=list)
    age = InputDataField(data_type=int)
