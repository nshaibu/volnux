import typing
from abc import ABC, abstractmethod

if typing.TYPE_CHECKING:
    from .ast import (
        AssignmentNode,
        BinOpNode,
        BlockNode,
        ConditionalNode,
        DescriptorNode,
        ExpressionGroupingNode,
        LiteralNode,
        ProgramNode,
        TaskNode,
    )


class ASTVisitorInterface(ABC):
    @abstractmethod
    def visit_program(self, node: "ProgramNode"):
        pass

    @abstractmethod
    def visit_descriptor(self, node: "DescriptorNode"):
        pass

    @abstractmethod
    def visit_task(self, node: "TaskNode"):
        pass

    @abstractmethod
    def visit_assignment(self, node: "AssignmentNode"):
        pass

    @abstractmethod
    def visit_binop(self, node: "BinOpNode"):
        pass

    @abstractmethod
    def visit_literal(self, node: "LiteralNode"):
        pass

    @abstractmethod
    def visit_block(self, node: "BlockNode"):
        pass

    @abstractmethod
    def visit_conditional(self, node: "ConditionalNode"):
        pass

    @abstractmethod
    def visit_expression_grouping(self, node: "ExpressionGroupingNode"):
        pass
