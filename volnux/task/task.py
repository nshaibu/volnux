import typing
from functools import lru_cache

from volnux.base import EventBase
from volnux.exceptions import EventDoesNotExist

from .base import TaskBase


class PipelineTask(TaskBase):
    def __init__(self, event: typing.Union[typing.Type[EventBase], str]) -> None:
        super().__init__()

        self.event = event

    def get_event_name(self) -> str:
        if isinstance(self.event, str):
            return self.event
        return self.event.__name__

    def get_event_klass(self):
        return self.resolve_event_name(self.event)

    def get_event_class(self):
        return self.get_event_klass()

    @classmethod
    @lru_cache()
    def resolve_event_name(
        cls, event_name: typing.Union[str, typing.Type[EventBase]]
    ) -> typing.Type[EventBase]:
        """Resolve event class"""
        if not isinstance(event_name, str):
            return event_name

        for event in cls.get_event_klasses():
            klass_name = event.__name__.lower()
            if klass_name == event_name.lower():
                return event
        raise EventDoesNotExist(f"'{event_name}' was not found.")

    @staticmethod
    def get_event_klasses() -> (
        typing.Generator[typing.Type[EventBase], typing.Any, None]
    ):
        yield from EventBase.get_all_event_classes()

    def get_dot_node_data(self) -> str:
        if self.is_sink:
            return f'\t"{self.id}" [label="{self.get_event_name()}", shape=box, style="filled,rounded", fillcolor=yellow]\n'
        elif self.is_conditional:
            return f'\t"{self.id}" [label="{self.get_event_name()}", shape=diamond, style="filled,rounded", fillcolor=yellow]\n'
        elif self.is_parallel_execution_node:
            nodes = self.get_parallel_nodes()
            node_id = nodes[0].get_id()
            node_label = "{" + "|".join([n.get_event_name() for n in nodes]) + "}"
            return f'\t"{node_id}" [label="{node_label}", shape=record, style="filled,rounded", fillcolor=lightblue]\n'

        return f'\t"{self.id}" [label="{self.get_event_name()}", shape=circle, style="filled,rounded", fillcolor=yellow]\n'
