import typing

from volnux import EventBase


class Process(EventBase):

    def process(self, *args, **kwargs) -> typing.Tuple[bool, typing.Any]:
        print("Executed process event")
        return True, "Executed process event"
