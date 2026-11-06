from volnux import Pipeline
from volnux.execution.pipeline.fields import InputDataField


class BroadcastPipeline(Pipeline):
    name = InputDataField(data_type=str)

    class Meta:
        pointy = "GeneratorEvent * 4 |-> ParallelAEvent || ParallelBEvent || ParallelCEvent |-> PrinterEvent"
