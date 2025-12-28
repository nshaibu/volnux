import logging
import typing

from . import ast
from .conditional import StandardDescriptor
from .exceptions import PointyParseError
from .operator import PipeType
from .options import Options
from .protocols import TaskGroupingProtocol, TaskProtocol
from .visitor import ASTVisitorInterface

logger = logging.getLogger(__name__)


class ExecutableASTGenerator(ASTVisitorInterface):
    def __init__(
        self,
        task_template: typing.Type[TaskProtocol],
        grouping_template: typing.Type[TaskGroupingProtocol],
    ):
        self.task_template = task_template
        self.grouping_template = grouping_template
        self._generated_task_chain: typing.Optional[TaskProtocol] = None
        self._current_task: typing.Optional[TaskProtocol] = None

    def apply_directive(self, name: str, value: typing.Union[str, int]):
        """Apply configuration directive"""
        if name == "recursive-depth":
            from volnux.utils import _extend_recursion_depth

            result = _extend_recursion_depth(value)
            if isinstance(result, Exception):
                logger.warning(f"Failed to set recursive-depth: {result}")

    def _visit_node(self, node: ast.ASTNode):
        """Generic node visitor dispatcher"""
        if isinstance(node, ast.ProgramNode):
            return self.visit_program(node)
        elif isinstance(node, ast.BinOpNode):
            return self.visit_binop(node)
        elif isinstance(node, ast.DescriptorNode):
            return self.visit_descriptor(node)
        elif isinstance(node, ast.TaskNode):
            return self.visit_task(node)
        elif isinstance(node, ast.ExpressionGroupingNode):
            return self.visit_expression_grouping(node)
        elif isinstance(node, ast.ConditionalNode):
            return self.visit_conditional(node)
        elif isinstance(node, ast.AssignmentNode):
            return self.visit_assignment(node)
        elif isinstance(node, ast.BlockNode):
            return self.visit_block(node)
        elif isinstance(node, ast.LiteralNode):
            return self.visit_literal(node)
        elif isinstance(node, ast.EnvironmentVariableAccessNode):
            return self.visit_environment_variable_access(node)
        elif isinstance(node, ast.VariableAccessNode):
            return self.visit_variable_access(node)
        elif isinstance(node, ast.MetaEventNode):
            return self.visit_meta_event(node)
        else:
            raise PointyParseError(f"Unknown node type: {type(node)}")

    def visit_program(self, node: ast.ProgramNode):
        for directive_name, directive_value in node.directives.items():
            self.apply_directive(directive_name, self._visit_node(directive_value))

        chain = node.chain
        if chain is None:
            return
        self._generated_task_chain = None
        self._current_task = None
        self._visit_node(chain)

    def visit_binop(
        self, node: ast.BinOpNode
    ) -> typing.Union[TaskProtocol, TaskGroupingProtocol]:
        left_instance: typing.Union[TaskProtocol, TaskGroupingProtocol] = (
            self._visit_node(node.left)
        )
        right_instance: typing.Union[TaskProtocol, TaskGroupingProtocol] = (
            self._visit_node(node.right)
        )

        if isinstance(
            left_instance, (TaskProtocol, TaskGroupingProtocol)
        ) and isinstance(right_instance, (TaskProtocol, TaskGroupingProtocol)):
            pipe_type = PipeType.get_pipe_type_enum(node.op)
            if pipe_type is None:
                logger.debug("No pipe type for %s", node.op)
                raise PointyParseError(
                    f"AST is malformed {ast}. No pipe type for {node.op} found."
                )

            if left_instance.is_conditional:
                left_instance.sink_node = right_instance
                left_instance.sink_pipe = pipe_type
            else:
                left_instance.condition_node.on_success_event = right_instance
                left_instance.condition_node.on_success_pipe = pipe_type

            right_instance.parent_node = left_instance
            return right_instance
        elif isinstance(left_instance, int) or isinstance(right_instance, int):
            descriptor_value = None
            node_instance = None

            if isinstance(left_instance, int):
                descriptor_value = left_instance
            else:
                node_instance = left_instance

            if isinstance(right_instance, int):
                descriptor_value = right_instance
            else:
                node_instance = right_instance

            if node_instance is None:
                logger.debug("No node instance for %s", left_instance)
                raise PointyParseError(
                    f"AST is malformed {ast}. Descriptor operation must have a valid task node"
                )

            # handle retry syntax
            if node.op == PipeType.RETRY.token():
                if node_instance.options is None:
                    node_instance.options = Options()
                # override the retry_attempts since * has high precedence
                node_instance.options.retry_attempts = descriptor_value
                return node_instance

            node_instance = node_instance.get_root()
            node_instance.descriptor = descriptor_value
            node_instance.descriptor_pipe = node.op
            return node_instance
        else:
            return left_instance or right_instance

    def visit_descriptor(self, node: ast.DescriptorNode):
        return int(node.value)

    def visit_task(self, node: ast.TaskNode):
        instance = self.task_template(event=node.task)
        self._current_task = instance
        if node.options:
            instance.options = Options.from_dict(
                self.visit_assignment_block(node.options)
            )
        return instance

    def visit_block(self, node: ast.BlockNode):
        if node.type == ast.BlockType.ASSIGNMENT:
            return self.visit_assignment_block(node)
        elif node.type == ast.BlockType.CONDITIONAL:
            node = typing.cast(ast.ConditionalNode, typing.cast(ast.ASTNode, node))
            return self.visit_conditional(node)
        elif node.type == ast.BlockType.GROUP:
            return self.visit_group_block(node)
        else:
            raise ValueError(f"Unknown block type: {type(node)}")

    def visit_group_block(self, node: ast.BlockNode):
        raise NotImplementedError("Not Supported yet")

    def visit_literal(self, node: ast.LiteralNode) -> typing.Union[int, str, float]:
        return node.value

    def visit_assignment(self, node: ast.AssignmentNode):
        return {node.target: self._visit_node(node.value)}

    def visit_assignment_block(
        self, node: ast.BlockNode
    ) -> typing.Dict[str, typing.Any]:
        assign = {}
        statements = typing.cast(typing.List[ast.AssignmentNode], node.statements)
        for statement in statements:
            assign.update(self.visit_assignment(statement))
        return assign

    def visit_expression_grouping(
        self, node: ast.ExpressionGroupingNode
    ) -> TaskGroupingProtocol:
        expression_chain_groups = [
            self._visit_node(chain) for chain in node.expressions
        ]

        # create instance of expression group
        instance = self.grouping_template(expression_chain_groups)
        self._current_task = instance
        if node.options:
            instance.options = Options.from_dict(
                self.visit_assignment_block(node.options)
            )
        return instance

    def visit_directive(self, node: ast.DirectiveNode):
        """Visit individual directive node"""
        return self._visit_node(node.value)

    def visit_variable_access(self, node: ast.VariableAccessNode):
        return self._visit_node(node.value)

    def visit_conditional(self, node: ast.ConditionalNode):
        parent = self.visit_task(node.task)

        for statement in node.branches.statements:
            instance: typing.Union[TaskProtocol, TaskGroupingProtocol] = (
                self._visit_node(statement)
            )
            if instance:
                self._current_task = instance

                instance = instance.get_root()
                instance.parent_node = parent

                if instance.descriptor == StandardDescriptor.FAILURE:
                    parent.condition_node.on_failure_event = instance
                    parent.condition_node.on_failure_pipe = PipeType.get_pipe_type_enum(
                        instance.descriptor_pipe
                    )
                elif instance.descriptor == StandardDescriptor.SUCCESS:
                    parent.condition_node.on_success_event = instance
                    parent.condition_node.on_success_pipe = PipeType.get_pipe_type_enum(
                        instance.descriptor_pipe
                    )
                else:
                    is_added = parent.condition_node.add_descriptor(
                        instance.descriptor,
                        PipeType.get_pipe_type_enum(instance.descriptor_pipe),
                        instance,
                    )
                    if not is_added:
                        logger.warning(
                            f"Failed to add descriptor {instance.descriptor} for event {node}"
                        )
            else:
                logger.warning(
                    f"Failed to add descriptor for conditional event {statement}"
                )

        return parent

    def visit_environment_variable_access(
        self, node: ast.EnvironmentVariableAccessNode
    ):
        return node.resolve()

    def visit_variable_access(self, node: ast.VariableAccessNode):
        return node.resolve()

    def visit_meta_event(self, node: ast.MetaEventNode):
        pass

    def generate(self) -> typing.Optional[TaskProtocol]:
        if self._current_task is None:
            return None
        return self._current_task.get_root()
