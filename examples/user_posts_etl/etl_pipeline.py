from volnux.pipeline import Pipeline
from volnux.fields import InputDataField
from volnux.signal.signals import pipeline_execution_start
from volnux.decorators import listener
from .events import LoadData, ProcessData, GraphData


class UserPostETLPipeline(Pipeline):
    """
    A pipeline for loading data, processing it, and visualizing it.

    Attributes:
        url (str): The URL of the data to load.

    """

    url = InputDataField(
        data_type=str,
        required=True,
        default="https://jsonplaceholder.typicode.com/posts",
    )

    class Meta:
        pointy = "LoadData |-> ProcessData |-> GraphData"
        # Path to the pointy file, if u choose to execute with it
        # file = "eventpipelines/userspost_ptr.pty"
