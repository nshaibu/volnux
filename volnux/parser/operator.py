import typing
from enum import Enum, unique


@unique
class PipeType(Enum):
    POINTER = "pointer"
    PIPE_POINTER = "pipe_pointer"
    PARALLELISM = "parallelism"
    RETRY = "retry"

    def token(self) -> typing.Optional[str]:
        if self == self.POINTER:
            return "->"
        elif self == self.PIPE_POINTER:
            return "|->"
        elif self == self.PARALLELISM:
            return "||"
        elif self == self.RETRY:
            return "*"
        return None

    @classmethod
    def get_pipe_type_enum(
        cls, pipe_str: typing.Union[str, "PipeType"]
    ) -> typing.Optional["PipeType"]:
        if isinstance(pipe_str, PipeType):
            return pipe_str
        if pipe_str == cls.PIPE_POINTER.token():
            return cls.PIPE_POINTER
        elif pipe_str == cls.PARALLELISM.token():
            return cls.PARALLELISM
        elif pipe_str == cls.RETRY.token():
            return cls.RETRY
        elif pipe_str == cls.POINTER.token():
            return cls.POINTER
        return None
