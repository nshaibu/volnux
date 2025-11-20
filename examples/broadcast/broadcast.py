from volnux import Pipeline
from volnux.fields import InputDataField


class BroadcastPipeline(Pipeline):
    name = InputDataField(data_type=str)

    class Meta:
        pointy = "GeneratorEvent * 4 |-> ParallelAEvent || ParallelBEvent || ParallelCEvent |-> PrinterEvent"
