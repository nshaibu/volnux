import typing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from volnux import EventBase


class Execute(EventBase):
    executor = ThreadPoolExecutor

    def process(self, *args, **kwargs) -> typing.Tuple[bool, typing.Any]:
        print("Executed execute event")
        return True, "Executed execute event"
