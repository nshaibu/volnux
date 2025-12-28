import importlib
import importlib.util
import typing
from os import environ
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum

from .__version__ import __version__ as version
from .task_namespace import VALID_NAMESPACES
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
    ASSIGNMENT_REFERENCE = "assignment_reference"
    NULL = "null"

    @classmethod
    def determine_literal_type(cls, value) -> "LiteralType":
        if isinstance(value, bool):
            return cls.BOOLEAN
        elif isinstance(value, (int, float)):
            return cls.NUMBER
        elif isinstance(value, str):
            if value == "null":
                return cls.NULL
            elif cls._is_import_string(value):
                return cls.IMPORT_STRING
            return cls.STRING
        elif isinstance(value, VariableAccessNode):
            return cls.ASSIGNMENT_REFERENCE
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

class ExpressionNode(ASTNode, ABC):
    pass

@dataclass
class ProgramNode(ASTNode):
    chain: ASTNode
    _version: str = field(repr=False, default=version)
    global_variables: typing.Dict[str, ASTNode] = field(default_factory=dict)
    directives: typing.Dict[str, ASTNode] = field(default_factory=dict)

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
class BinOpNode(ExpressionNode):
    __slots__ = ("left", "right", "op")
    left: ExpressionNode
    op: str
    right: ExpressionNode

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_binop(self)

@dataclass
class UnaryOpNode(ExpressionNode):
    __slots__ = ("op", "right")
    op: str
    right: ExpressionNode

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_unaryop(self)

@dataclass
class DirectiveNode(ASTNode):
    name: str
    value: "LiteralNode"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_directive(self)


@dataclass
class VariableAccessNode(ExpressionNode):
    __slots__ = ("name", "value")
    name: str
    value: "LiteralNode"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_variable_access(self)

    def resolve(self) -> typing.Any:
        value = self.value

        while (
            isinstance(value, LiteralNode)
            and value.type == LiteralType.ASSIGNMENT_REFERENCE
        ):
            value = value.value
        return value.value


@dataclass
class EnvironmentVariableAccessNode(ASTNode):
    __slots__ = ("name",)
    name: str

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_access_environment_variable(self)

    def resolve(self) -> typing.Any:
        """
        Resolve environment variables.
        Returns:
            Value
        Raises:
            KeyError if environment variable is not defined.
        """
        return environ.get(self.name)


@dataclass
class VariableDeclNode(ASTNode):
    __slots__ = ("name", "type")
    name: str
    value: typing.Union["LiteralNode", VariableAccessNode]

    def accept(self, visitor: "ASTVisitor"):
        raise NotImplementedError()


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
    namespace: str = field(default="local")

    def __post_init__(self):
        """Validate namespace if provided"""
        if self.namespace and self.namespace not in VALID_NAMESPACES:
            raise ValueError(
                f"Invalid task namespace '{self.namespace}'. "
                f"Valid namespaces: {', '.join(VALID_NAMESPACES)}"
            )

    @property
    def fully_qualified_name(self) -> str:
        """Get fully qualified task name"""
        if self.namespace:
            return f"{self.namespace}::{self.task}"
        return self.task

    @property
    def is_external(self) -> bool:
        """Check if task is from external source"""
        return self.namespace is not None and self.namespace != "local"

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
class LiteralNode(ExpressionNode):
    __slots__ = ("value",)
    value: typing.Any
    type: typing.Optional[LiteralType] = None

    def __post_init__(self):
        if self.value and self.type is None:
            self.type = LiteralType.determine_literal_type(self.value)

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_literal(self)

@dataclass
class ListNode(ASTNode):
    __slots__ = ("value",)
    value: typing.List[typing.Any]

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_list(self)

@dataclass
class MapNode(ASTNode):
    __slots__ = ("value",)
    value: typing.Dict[str, typing.Any]

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_map(self)

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


@dataclass
class MetaEventNode(ASTNode):
    """
    AST node for Meta Events
    Represents control flow patterns like MAP, FILTER, REDUCE, etc.
    """

    mode: typing.Literal["MAP", "FILTER", "REDUCE", "FOREACH", "FLATMAP", "FANOUT"]
    template_event: str  # Name of the Template Event (identifier)
    template_event_namespace: str = "local"
    options: typing.Optional[BlockNode] = None

    def accept(self, visitor):
        """Visitor pattern support"""
        return visitor.visit_meta_event(self)


@dataclass
class NullCoalesceExprNode(ASTNode):
    left: ASTNode
    right: ASTNode

    def __repr__(self):
        return f"NullCoalesce({self.left} ?? {self.right})"

    def accept(self, visitor: "ASTVisitor"):
        pass


@dataclass
class ComparisonExprNode(ASTNode):
    operator: str
    left: ASTNode
    right: ASTNode

    def __repr__(self):
        return f"Comparison({self.left} {self.operator} {self.right})"

    def accept(self, visitor: "ASTVisitor"):
        pass


@dataclass
class TernaryExprNode(ASTNode):
    condition: ASTNode
    true_expr: ASTNode
    false_expr: ASTNode

    def __repr__(self):
        return f"Ternary({self.condition} ? {self.true_expr} : {self.false_expr})"

    def accept(self, visitor: "ASTVisitor"):
        pass
