import typing

from volnux import EventBase
from volnux.base import RetryPolicy


class Serialise(EventBase):

    def process(self, *args, **kwargs):
        print("Serialization")
        return True, "Serialise"


class Upload(EventBase):

    def process(self, *args, **kwargs):
        print("Upload event")
        return True, "Upload Event"


class Commission(EventBase):
    retry_policy = RetryPolicy(
        max_attempts=10,
        backoff_factor=0.005,
        max_backoff=100,
        retry_on_exceptions=[ValueError],
    )

    def process(self, *args, **kwargs):
        self.stop_on_exception = True
        print("Commission")
        raise ValueError("test error")
        return True, "Commission"


class Pack(EventBase):

    def process(self, *args, **kwargs):
        print("pack")
        return True, "Pack"


class Ship(EventBase):

    def process(self, *args, **kwargs):
        print("Ship")
        return True, "Ship"


class Unpack(EventBase):

    def process(self, *args, **kwargs):
        print("Unpack")
        return True, "Unpack"


class Receive(EventBase):

    def process(self, *args, **kwargs):
        print("Receive")
        return True, "Receive"


class Dispense(EventBase):

    def process(self, *args, **kwargs):
        print("Dispense")
        return True, "Dispense"
