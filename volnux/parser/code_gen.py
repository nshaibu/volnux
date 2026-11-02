import logging
import operator
import typing
from dataclasses import dataclass

from . import ast
from .exceptions import PointyParseError
from .operator import PipeType
from .options import Options
from .protocols import TaskGroupingProtocol, TaskProtocol, TaskType
from .visitor import ASTVisitorInterface

logger = logging.getLogger(__name__)

# Operators in a chain that connect tasks (as opposed to expression operators).
_SEQUENCE_OPS = frozenset({"->", "|->"})

_BINARY_OPERATORS: typing.Dict[str, typing.Callable[[typing.Any, typing.Any], typing.Any]] = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "%": operator.mod,
    "<<": operator.lshift,
    ">>>": operator.rshift,  # arithmetic shift right
    "&": operator.and_,
    "|": operator.or_,
    "^": operator.xor,
}

_UINT_MASK = (1 << 64) - 1


def _logical_shift_right(value: int, count: int) -> int:
    """Shift a 64-bit unsigned view of ``value`` right, filling with zeros."""
    return (value & _UINT_MASK) >> count


@dataclass
class Fragment:
    """
    A piece of the task graph built from a chain expression.

    ``head`` is where the fragment is entered and ``tail`` is the node that the
    next element of a chain links from. For a single task both are the same node.
    """

    head: TaskType
    tail: TaskType
    # the tail is a task with branches; whatever follows it becomes its sink
    conditional: bool = False
    # a run of tasks linked with ``||`` (``A || B || C``)
    parallel_run: bool = False

    @property
    def is_single(self) -> bool:
        return self.head is self.tail


class ExecutableASTGenerator(ASTVisitorInterface):
    """
    Translates a Pointy AST into a hierarchy of tasks.

    Chain nodes (tasks, groups, conditionals, retries, ``->``/``|->``/``||``)
    are visited into ``Fragment`` objects and wired together using the task
    protocols; expression nodes are evaluated into plain Python values.
    """

    def __init__(
        self,
        task_template: typing.Type[TaskProtocol],
        grouping_template: typing.Type[TaskGroupingProtocol],
    ):
        self.task_template = task_template
        self.grouping_template = grouping_template
        self._root: typing.Optional[TaskType] = None
        self.program: typing.Optional[ast.ProgramNode] = None
        # custom descriptor label -> integer value (from ``@descriptor``)
        self.descriptors: typing.Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply_directive(self, name: str, value: typing.Any) -> None:
        """Hook for directives that influence task generation (none today)."""

    def generate(self) -> typing.Optional[TaskType]:
        """Return the root of the generated task hierarchy, if any."""
        return self._root

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _error(message: str) -> PointyParseError:
        return PointyParseError(message)

    def _value(self, node: ast.ASTNode) -> typing.Any:
        """Evaluate an expression node into a Python value."""
        return node.accept(self)

    def _fragment(self, node: ast.ASTNode) -> Fragment:
        """Visit a chain node and return the graph fragment it produced."""
        result = node.accept(self)
        if not isinstance(result, Fragment):
            raise self._error(f"Expected a task expression but got {node!r}")
        return result

    def _register(self, task: TaskType) -> TaskType:
        """Give a new task its own copy of the program's descriptor names."""
        task.descriptors = dict(self.descriptors)
        return task

    def _build_options(self, attributes: typing.List[ast.AttributeNode]) -> Options:
        values: typing.Dict[str, typing.Any] = {}
        for attribute in attributes or []:
            name, value = self.visit_attribute(attribute)
            values[name] = value
        try:
            return Options.from_dict(values)
        except Exception as exc:
            raise self._error(f"Invalid task options {sorted(values)}: {exc}") from exc

    def _connect(
        self, left: Fragment, head: TaskType, pipe: typing.Optional[PipeType]
    ) -> None:
        """Link ``head`` after the tail of ``left`` with the given pipe."""
        tail = left.tail
        if left.conditional:
            # branches own the success/failure slots; what follows collapses
            # into the sink node once the branches are done
            tail.sink_node = head
            tail.sink_pipe = pipe
        else:
            tail.condition_node.on_success_event = head
            tail.condition_node.on_success_pipe = pipe
        head.parent_node = tail

    def _set_retry(self, task: TaskType, attempts: int) -> None:
        if task.options is None:
            task.options = Options.from_dict({"retry_attempts": attempts})
        else:
            task.options.retry_attempts = attempts

    # ------------------------------------------------------------------
    # Program and directives
    # ------------------------------------------------------------------

    def visit_program(self, node: ast.ProgramNode) -> typing.Optional[TaskType]:
        self._root = None
        self.program = node
        self.descriptors = dict(node.descriptors)

        for name, value in node.directives.items():
            self.apply_directive(name, self._value(value))

        if node.chain is None:
            return None

        self._root = self._fragment(node.chain).head
        return self._root

    def visit_directive(self, node: ast.DirectiveNode) -> typing.Any:
        return self._value(node.value)

    def visit_variable_declaration(self, node: ast.VariableDeclNode) -> typing.Any:
        return self._value(node.value)

    # ------------------------------------------------------------------
    # Tasks and groups
    # ------------------------------------------------------------------

    def visit_task(self, node: ast.TaskNode) -> Fragment:
        task = self._register(self.task_template(event=node.task))
        task.namespace = node.namespace
        if node.options:
            task.options = self._build_options(node.options)
        return Fragment(task, task)

    def visit_meta_task(self, node: ast.MetaTaskNode) -> Fragment:
        # A meta event is a task named after its mode; the template it applies
        # is carried in the options for the runtime to resolve.
        task = self._register(self.task_template(event=node.mode))
        options = self._build_options(node.options)
        options.extras["template_class"] = node.template_task
        options.extras["template_namespace"] = node.template_event_namespace
        task.options = options
        return Fragment(task, task)

    def visit_pipeline_grouping(self, node: ast.PipelineGroupingNode) -> Fragment:
        chains = [self._fragment(expression).head for expression in node.expressions]
        group = self._register(self.grouping_template(chains))
        if node.options:
            group.options = self._build_options(node.options)
        return Fragment(group, group)

    # ------------------------------------------------------------------
    # Chains
    # ------------------------------------------------------------------

    def visit_binop(self, node: ast.BinOpNode) -> typing.Any:
        if node.op in _SEQUENCE_OPS:
            return self._sequence(node)
        if node.op == "||":
            # ``||`` is both the parallel operator and logical-or: the left
            # operand decides which one this is.
            left = node.left.accept(self)
            if isinstance(left, Fragment):
                return self._parallel(left, node)
            return self._truthy(left) or self._truthy(self._value(node.right))
        if node.op == "&&":
            if not self._truthy(self._value(node.left)):
                return False
            return self._truthy(self._value(node.right))
        return self._arithmetic(node)

    def _sequence(self, node: ast.BinOpNode) -> Fragment:
        left = self._fragment(node.left)
        right = self._fragment(node.right)
        pipe = PipeType.get_pipe_type_enum(node.op)
        self._connect(left, right.head, pipe)
        return Fragment(left.head, right.tail, conditional=right.conditional)

    def _parallel(self, left: Fragment, node: ast.BinOpNode) -> Fragment:
        right = self._fragment(node.right)
        if not (left.is_single or left.parallel_run) or left.conditional:
            raise self._error(
                "'||' can only join single tasks or groups; wrap a multi-task "
                "chain in a group, e.g. '{ A -> B } || C'"
            )
        if not right.is_single:
            raise self._error(
                "'||' can only join single tasks or groups; wrap a multi-task "
                "chain in a group, e.g. 'A || { B -> C }'"
            )
        self._connect(left, right.head, PipeType.PARALLELISM)
        return Fragment(
            left.head, right.tail, conditional=right.conditional, parallel_run=True
        )

    def visit_conditional(self, node: ast.ConditionalNode) -> Fragment:
        if node.task is None:
            raise self._error(
                "Conditional expression has no subject task; branches must "
                "follow a task, e.g. 'A (failure -> B, success -> C)'"
            )
        subject = self._fragment(node.task)
        if not subject.is_single:
            raise self._error("Branches can only follow a single task or group")

        for branch in node.branches:
            descriptor, pipe, fragment = self.visit_branch(branch)
            self._attach_branch(subject.head, descriptor, pipe, fragment.head)

        return Fragment(subject.head, subject.head, conditional=True)

    def visit_branch(
        self, node: ast.BranchNode
    ) -> typing.Tuple[int, typing.Optional[PipeType], Fragment]:
        descriptor = self.visit_descriptor(node.condition)
        pipe = PipeType.get_pipe_type_enum(node.operator)
        return descriptor, pipe, self._fragment(node.task)

    def _attach_branch(
        self,
        subject: TaskType,
        descriptor: int,
        pipe: typing.Optional[PipeType],
        head: TaskType,
    ) -> None:
        existing = subject.condition_node.get_descriptor_config(descriptor)
        if existing is not None and existing.task is not None:
            raise self._error(
                f"Duplicate descriptor {descriptor} in conditional on "
                f"'{subject.get_event_name()}'"
            )
        head.descriptor = descriptor
        head.descriptor_pipe = pipe
        head.parent_node = subject
        if not subject.condition_node.add_descriptor(descriptor, pipe, head):
            raise self._error(f"Invalid descriptor {descriptor}")

    def visit_descriptor(self, node: ast.DescriptorNode) -> int:
        if node.value is None:
            raise self._error(f"Descriptor '{node.label}' was never resolved")
        return int(node.value)

    def visit_retry(self, node: ast.RetryNode) -> Fragment:
        if isinstance(node.job, ast.ConditionalNode):
            raise self._error("Retry cannot be applied to a conditional expression")

        attempts = self._value(node.attempts)
        fragment = self._fragment(node.job)

        if not fragment.is_single:
            # retry the whole chain as one unit
            group = self._register(self.grouping_template([fragment.head]))
            fragment = Fragment(group, group)

        self._set_retry(fragment.head, attempts)
        return fragment

    def visit_attribute(self, node: ast.AttributeNode) -> typing.Tuple[str, typing.Any]:
        return node.attr, self._value(node.value)

    # ------------------------------------------------------------------
    # Expressions
    # ------------------------------------------------------------------

    def _truthy(self, value: typing.Any) -> bool:
        return self._is_truthy(value)

    def visit_literal(self, node: ast.LiteralNode) -> typing.Any:
        if node.type == ast.LiteralType.NULL:
            return None
        return node.value

    def visit_list(self, node: ast.ListNode) -> typing.List[typing.Any]:
        return [self._value(item) for item in node.value]

    def visit_map(self, node: ast.MapNode) -> typing.Dict[str, typing.Any]:
        return {key: self._value(value) for key, value in node.value.items()}

    def visit_variable_access(self, node: ast.VariableAccessNode) -> typing.Any:
        return self._value(node.value)

    def visit_access_environment_variable(
        self, node: ast.EnvironmentVariableAccessNode
    ) -> typing.Any:
        return node.resolve()

    def visit_unaryop(self, node: ast.UnaryOpNode) -> typing.Any:
        value = self._value(node.right)
        try:
            if node.op == "!":
                return not self._truthy(value)
            if node.op == "~":
                return ~value
            if node.op == "-":
                return -value
        except TypeError as exc:
            raise self._error(f"Cannot apply '{node.op}' to {value!r}: {exc}") from exc
        raise self._error(f"Unknown unary operator '{node.op}'")

    def _arithmetic(self, node: ast.BinOpNode) -> typing.Any:
        left = self._value(node.left)
        right = self._value(node.right)
        try:
            if node.op == ">>":
                return _logical_shift_right(left, right)
            function = _BINARY_OPERATORS.get(node.op)
            if function is None:
                raise self._error(f"Unknown operator '{node.op}'")
            return function(left, right)
        except (TypeError, ValueError, ZeroDivisionError, OverflowError) as exc:
            raise self._error(
                f"Cannot evaluate {left!r} {node.op} {right!r}: {exc}"
            ) from exc

    def visit_comparison_expr(self, node: ast.ComparisonExprNode) -> bool:
        return self._compare(
            node.operator, self._value(node.left), self._value(node.right)
        )

    def visit_ternary_expr(self, node: ast.TernaryExprNode) -> typing.Any:
        if self._truthy(self._value(node.condition)):
            return self._value(node.true_expr)
        return self._value(node.false_expr)

    def visit_null_coalesce(self, node: ast.NullCoalesceExprNode) -> typing.Any:
        left = self._value(node.left)
        return left if left is not None else self._value(node.right)

    def visit_index_expr(self, node: ast.IndexExprNode) -> typing.Any:
        return self._safe_index(self._value(node.collection), self._value(node.index))
