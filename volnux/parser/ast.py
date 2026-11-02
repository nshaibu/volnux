import typing
from os import environ
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum

from .__version__ import __version__ as version
from .protocols import GroupingStrategy

if typing.TYPE_CHECKING:
    from .visitor import ASTVisitorInterface as ASTVisitor


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
        # Purely syntactic: whether the path can be imported is a runtime
        # concern, and parsing must never import (and so execute) user modules.
        return cls._looks_like_import_path(value)

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


class ASTNode(ABC):
    @abstractmethod
    def accept(self, visitor: "ASTVisitor"):
        pass

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.__class__.__name__})"


class ExpressionNode(ASTNode, ABC):
    pass


@dataclass
class ProgramNode(ASTNode):
    chain: ASTNode
    _version: str = field(repr=False, default=version)
    global_variables: typing.Dict[str, ASTNode] = field(default_factory=dict)
    directives: typing.Dict[str, ASTNode] = field(default_factory=dict)
    # Custom descriptor label -> integer value, assigned by the parser from
    # ``@descriptor`` (built-in ``failure``/``success`` are not included).
    descriptors: typing.Dict[str, int] = field(default_factory=dict)
    author: typing.Optional[str] = None
    description: typing.Optional[str] = None

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_program(self)


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
class EnvironmentVariableAccessNode(ExpressionNode):
    __slots__ = ("name",)
    name: str

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_access_environment_variable(self)

    def resolve(self) -> typing.Any:
        """
        Resolve environment variables.
        Returns:
            Value
        """
        return environ.get(self.name)


@dataclass
class VariableDeclNode(ASTNode):
    __slots__ = ("name", "type")
    name: str
    value: ExpressionNode

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_variable_declaration(self)


@dataclass
class ConditionalNode(ASTNode):
    __slots__ = ("task", "branches")
    # None for a conditional written without a subject task, e.g.
    # ``(failure -> A, success -> B)``; semantic analysis reports it.
    task: typing.Optional["TaskNode"]
    branches: typing.List["BranchNode"]

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_conditional(self)


@dataclass
class TaskNode(ASTNode):
    # __slots__ = ("task", "options")
    task: str
    options: typing.List["AttributeNode"]
    # Namespaces are user-defined and only resolved at runtime, so the parser
    # accepts any identifier here.
    namespace: str = field(default="local")

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


# Descriptors with a fixed meaning; every other descriptor is user-defined.
BUILTIN_DESCRIPTORS: typing.Dict[str, int] = {"failure": 0, "success": 1}


@dataclass
class DescriptorNode(ASTNode):
    """AST for descriptor nodes."""

    __slots__ = ("value",)
    value: int
    # The name used in the source (``failure``, ``success`` or a custom label);
    # None when the descriptor was written as a number.
    label: typing.Optional[str] = None

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

    def __len__(self) -> int:
        return len(self.value)

    def __getitem__(self, index: int) -> typing.Any:
        return self.value[index]

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_list(self)


@dataclass
class MapNode(ASTNode):
    __slots__ = ("value",)
    value: typing.Dict[str, typing.Any]

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_map(self)


@dataclass
class PipelineGroupingNode(ASTNode):
    """AST for expression chain. One expression chain only"""

    expressions: typing.List[ASTNode]
    grouping_strategy: typing.Optional["GroupingStrategy"] = None
    # Options attached to a grouped expression are attribute lists (list[AttributeNode])
    options: typing.Optional[typing.List["AttributeNode"]] = None

    def __post_init__(self):
        if self.grouping_strategy is None:
            self.grouping_strategy = (
                GroupingStrategy.SINGLE_CHAIN
                if len(self.expressions) == 1
                else GroupingStrategy.MULTIPATH_CHAINS
            )

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_pipeline_grouping(self)


@dataclass
class MetaTaskNode(ASTNode):
    """
    AST node for Meta Events
    Represents control flow patterns like MAP, FILTER, REDUCE, etc.
    """

    mode: typing.Literal["MAP", "FILTER", "REDUCE", "FOREACH", "FLATMAP", "FANOUT"]
    template_task: str  # Name of the Template Task (identifier)
    options: typing.List["AttributeNode"]
    template_event_namespace: str = "local"

    def accept(self, visitor: "ASTVisitor"):
        """Visitor pattern support"""
        return visitor.visit_meta_task(self)


@dataclass
class NullCoalesceExprNode(ASTNode):
    left: ASTNode
    right: ASTNode

    def __repr__(self):
        return f"NullCoalesce({self.left} ?? {self.right})"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_null_coalesce(self)


@dataclass
class ComparisonExprNode(ASTNode):
    operator: str
    left: ASTNode
    right: ASTNode

    def __repr__(self):
        return f"Comparison({self.left} {self.operator} {self.right})"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_comparison_expr(self)


@dataclass
class TernaryExprNode(ASTNode):
    condition: ASTNode
    true_expr: ASTNode
    false_expr: ASTNode

    def __repr__(self):
        return f"Ternary({self.condition} ? {self.true_expr} : {self.false_expr})"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_ternary_expr(self)


@dataclass
class AttributeNode(ASTNode):
    value: ExpressionNode
    attr: str

    def __repr__(self):
        return f"Attribute({self.value}.{self.attr})"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_attribute(self)


@dataclass
class RetryNode(ASTNode):
    job: ASTNode
    attempts: LiteralNode

    def __repr__(self):
        return f"Retry(job={self.job}, attempts={self.attempts})"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_retry(self)


@dataclass
class BranchNode(ASTNode):
    condition: DescriptorNode
    operator: str
    task: ASTNode

    def __repr__(self):
        return f"Branch(condition={self.condition}, task={self.task})"

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_branch(self)


@dataclass
class IndexExprNode(ExpressionNode):
    collection: ASTNode
    index: LiteralNode

    def accept(self, visitor: "ASTVisitor"):
        return visitor.visit_index_expr(self)
