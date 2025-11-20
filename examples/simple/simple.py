from volnux.decorators import listener
from volnux.fields import InputDataField
from volnux.pipeline import BatchPipeline, Pipeline
from volnux.signal.signals import pipeline_execution_start


class Simple(Pipeline):
    name = InputDataField(data_type=str, batch_size=5)


class SimpleBatch(BatchPipeline):
    pipeline_template = Simple


@listener(pipeline_execution_start, sender=Simple)
def simple_listener(**kwargs):
    print(kwargs)
