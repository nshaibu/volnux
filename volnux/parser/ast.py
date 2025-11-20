import importlib
import importlib.util
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from .protocols import GroupingStrategy

if typing.TYPE_CHECKING:
    from .visitor import ASTVisitorInterface as ASTVisitor


STANDARD_EXECUTORS = {
    "threadpoolexecutor": "event_pipeline.executors.ThreadPoolExecutor",
    "processpoolexecutor": "event_pipeline.executors.ProcessPoolExecutor",
    "defaultexecutor": "event_pipeline.executors.DefaultExecutor",
}


class BlockType(Enum):
    ASSIGNMENT = "assignment"
    CONDITIONAL = "conditional"
    GROUP = "group"


class LiteralType(Enum):
    NUMBER = "number"
    STRING = "string"
    IMPORT_STRING = "import_string"
    BOOLEAN = "boolean"

    @classmethod
    def determine_literal_type(cls, value) -> "LiteralType":
        if isinstance(value, bool):
            return cls.BOOLEAN
        elif isinstance(value, (int, float)):
            return cls.NUMBER
        elif isinstance(value, str):
            if cls._is_import_string(value):
                return cls.IMPORT_STRING
            return cls.STRING
        else:
            raise ValueError(f"Unsupported literal type: {type(value).__name__}")

    @classmethod
    def _is_import_string(cls, value: str) -> bool:
        if not cls._looks_like_import_path(value):
            return False

        return cls._is_actually_importable(value)

    @classmethod
    def _looks_like_import_path(cls, value: str) -> bool:
        """Fast syntactic check for potential import paths."""
        if not value:
            return False

        parts = value.split(".")
        return (
            len(parts) >= 2  # At least package.module
            and all(part.isidentifier() for part in parts)
            and not value.startswith(".")
            and not value.endswith(".")
            and all(
                not part.startswith("__") or part.endswith("__") for part in parts
            )  # Allow __init__ etc
        )

    @classmethod
    def _is_actually_importable(cls, value: str) -> bool:
        """Check if the string can actually be imported."""
        try:
            spec = importlib.util.find_spec(value)
            return spec is not None
        except (ImportError, ModuleNotFoundError, ValueError, AttributeError):
            return False


class ASTNode(ABC):
    @abstractmethod
    def accept(self, visitor: "ASTVisitor"):
        pass


@dataclass
class ProgramNode(ASTNode):
    __slots__ = ("chain",)
    chain: ASTNode

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_program(self)


@dataclass
class AssignmentNode(ASTNode):
    __slots__ = ("target", "value")
    target: str
    value: "LiteralNode"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_assignment(self)


@dataclass
class BlockNode(ASTNode):
    __slots__ = ("statements", "type")
    statements: typing.List[ASTNode]
    type: BlockType

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_block(self)


@dataclass
class BinOpNode(ASTNode):
    __slots__ = ("left", "right", "op")
    left: ASTNode
    op: str
    right: ASTNode

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_binop(self)


@dataclass
class ConditionalNode(ASTNode):
    __slots__ = ("task", "branches")
    task: "TaskNode"
    branches: BlockNode

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_conditional(self)


@dataclass
class TaskNode(ASTNode):
    # __slots__ = ("task", "options")
    task: str
    options: typing.Optional[BlockNode] = None

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_task(self)


@dataclass
class DescriptorNode(ASTNode):
    """AST for descriptor nodes."""

    __slots__ = ("value",)
    value: int

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_descriptor(self)


@dataclass
class LiteralNode(ASTNode):
    __slots__ = ("value",)
    value: typing.Any
    type: typing.Optional[LiteralType] = None

    def __post_init__(self):
        if self.value and self.type is None:
            self.type = LiteralType.determine_literal_type(self.value)

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_literal(self)


@dataclass
class ExpressionGroupingNode(ASTNode):
    """AST for expression chain. One expression chain only"""

    expressions: typing.List[ASTNode]
    grouping_strategy: "GroupingStrategy" = None
    options: typing.Optional[BlockNode] = None

    def __post_init__(self):
        if self.grouping_strategy is None:
            self.grouping_strategy = (
                GroupingStrategy.SINGLE_CHAIN
                if len(self.expressions) == 1
                else GroupingStrategy.MULTIPATH_CHAINS
            )

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_expression_grouping(self)
