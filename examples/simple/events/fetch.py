import typing
from concurrent.futures import ThreadPoolExecutor
from volnux import EventBase


class Fetch(EventBase):
    executor = ThreadPoolExecutor

    def process(self, name) -> typing.Tuple[bool, typing.Any]:
        print(f"Executed fetch event: {name}")
        return True, "Executed fetch event"
