import typing

from volnux.parser.protocols import GroupingStrategy, TaskType

from .base import TaskBase


class PipelineTaskGrouping(TaskBase):
    def __init__(self, chains: typing.List[TaskType]) -> None:
        super().__init__()

        self.chains = chains

        self.strategy: GroupingStrategy = (
            GroupingStrategy.MULTIPATH_CHAINS
            if len(chains) > 1
            else GroupingStrategy.SINGLE_CHAIN
        )

    def get_event_name(self) -> str:
        return "TaskGrouping"

    def get_dot_node_data(self) -> str:
        from volnux.translator.dot import draw_subgraph_from_task_state

        return draw_subgraph_from_task_state(self)
