import typing
from .options import StrEnum


class ParserMode(StrEnum):
    """Defines parser modes."""

    DAG = "DAG"
    CFG = "CFG"

    @property
    def modes(self) -> typing.List[str]:
        return [self.DAG, self.CFG]
