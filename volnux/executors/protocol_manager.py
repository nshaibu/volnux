import typing

from .message import TaskMessage


class ProtocolManager:
    def __init__(self):
        pass

    def deserialize_data(self, data: str) -> typing.Tuple[typing.Any, bool]:
        # todo forward information to base handler
        return TaskMessage.deserialize(data)
