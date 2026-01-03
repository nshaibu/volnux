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
        DirectiveNode,
        VariableAccessNode,
        MetaEventNode,
        EnvironmentVariableAccessNode,
        ListNode,
        MapNode,
    )


class ASTVisitorInterface(ABC):

    def _safe_index(
        self, target: typing.Any, index: typing.Any
    ) -> typing.Optional[typing.Any]:
        """
        Safely index into a collection, returning None on error.

        Args:
            target: Collection to index into
            index: Index or key

        Returns:
            Indexed value or None
        """
        if target is None:
            return None

        try:
            # Dictionary access
            if isinstance(target, dict):
                return target.get(index, None)

            # List/tuple access
            elif isinstance(target, (list, tuple)):
                if isinstance(index, int):
                    if -len(target) <= index < len(target):
                        return target[index]
                return None

            # String access
            elif isinstance(target, str):
                if isinstance(index, int):
                    if -len(target) <= index < len(target):
                        return target[index]
                return None
            else:
                return None

        except (KeyError, IndexError, TypeError):
            return None

    def _compare(self, operator: str, left: typing.Any, right: typing.Any) -> bool:
        """
        Perform comparison operation.

        Args:
            operator: Comparison operator (==, !=, <, >, <=, >=)
            left: Left operand
            right: Right operand

        Returns:
            Boolean result
        """
        # Equality comparisons work with None
        if operator == "==":
            return left == right

        elif operator == "!=":
            return left != right

        if left is None or right is None:
            return False

        try:
            if operator == "<":
                return left < right
            elif operator == ">":
                return left > right
            elif operator == "<=":
                return left <= right
            elif operator == ">=":
                return left >= right

        except TypeError:
            # Incomparable types (e.g., string vs number)
            return False

        return False

    def _is_truthy(self, value: typing.Any) -> bool:
        """
        Determine if a value is truthy.

        Args:
            value: Value to check

        Returns:
            True if truthy, False otherwise
        """
        if value is None:
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return value != 0

        if isinstance(value, str):
            return len(value) > 0

        if isinstance(value, (list, dict, tuple)):
            return len(value) > 0

        # Everything else is truthy
        return True

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

    @abstractmethod
    def visit_directive(self, node: "DirectiveNode"):
        pass

    @abstractmethod
    def visit_variable_access(self, node: "VariableAccessNode"):
        pass

    @abstractmethod
    def visit_access_environment_variable(self, node: "EnvironmentVariableAccessNode"):
        pass

    @abstractmethod
    def visit_meta_event(self, node: "MetaEventNode"):
        pass

    @abstractmethod
    def visit_list(self, node: "ListNode"):
        pass

    @abstractmethod
    def visit_map(self, node: "MapNode"):
        pass