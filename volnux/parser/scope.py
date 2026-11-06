"""
Scopes of Pointy-Lang variables.

Annotations (``[a=1]``) on a ``{}`` block declare variables for that block. Inner
blocks see the variables of the blocks around them; an inner declaration shadows an
outer one. ``$a`` resolves to the nearest enclosing declaration, then to a global
``@a``, and is an error if neither exists (the environment is never consulted
implicitly: write ``$env.A``).

A ``@component`` runs in a scope on top of its caller's: the component's defaults,
then the invocation's annotations, so the body sees both and the caller's own
variables as well (dynamic scoping, as for a subroutine called in a scope).

Two things live here:

* :class:`Scope`: the run-time chain used while tasks are generated.
* :func:`check_scopes`: the parse-time check that every ``$name`` can be resolved,
  so a mistake fails when the script is parsed, not when it is built.
"""

import typing
from dataclasses import dataclass, field

from . import ast

__all__ = [
    "CASCADING_EXCLUDED",
    "Scope",
    "UnresolvedVariableError",
    "check_scopes",
    "unresolved_message",
]

#: Options that configure a block as a unit. They are variables like any other
#: (``$retry_attempts`` works) but are not passed down to the tasks inside:
#: cascading ``retry_attempts`` would retry every task inside each retry of the
#: group, multiplying the attempts.
CASCADING_EXCLUDED = frozenset(
    {"retry_attempts", "max_concurrency", "result_evaluation_strategy", "extras"}
)


def unresolved_message(name: str, component: typing.Optional[str] = None) -> str:
    by = f" (used by component '{component}')" if component else ""
    return f"Variable '{name}'{by} is not declared in any enclosing scope"


class UnresolvedVariableError(LookupError):
    """A ``$name`` is declared nowhere it can be seen from."""


@dataclass
class Scope:
    """Variables declared by one block, and the scope around it."""

    variables: typing.Dict[str, typing.Any] = field(default_factory=dict)
    parent: typing.Optional["Scope"] = None

    def lookup(self, name: str) -> typing.Tuple[bool, typing.Any]:
        """(found, value) of the nearest enclosing declaration of ``name``."""
        scope: typing.Optional[Scope] = self
        while scope is not None:
            if name in scope.variables:
                return True, scope.variables[name]
            scope = scope.parent
        return False, None

    def resolve(self, name: str) -> typing.Any:
        found, value = self.lookup(name)
        if not found:
            raise UnresolvedVariableError(unresolved_message(name))
        return value

    def child(self, variables: typing.Dict[str, typing.Any]) -> "Scope":
        return Scope(dict(variables), parent=self)

    def chain(self) -> typing.List["Scope"]:
        """This scope and its parents, outermost first."""
        scopes: typing.List[Scope] = []
        scope: typing.Optional[Scope] = self
        while scope is not None:
            scopes.append(scope)
            scope = scope.parent
        return scopes[::-1]

    def cascaded(self) -> typing.Dict[str, typing.Any]:
        """The variables that flow down into the tasks of this scope as options:
        every enclosing declaration, the innermost winning, except the options
        that apply to a block as a unit (:data:`CASCADING_EXCLUDED`)."""
        merged: typing.Dict[str, typing.Any] = {}
        for scope in self.chain():
            merged.update(
                {
                    k: v
                    for k, v in scope.variables.items()
                    if k not in CASCADING_EXCLUDED
                }
            )
        return merged


# ---------------------------------------------------------------------------
# Parse-time check
# ---------------------------------------------------------------------------


class _Checker:
    """Walks the AST with the names declared so far and collects the ``$name``
    that nothing declares."""

    def __init__(self, program: ast.ProgramNode) -> None:
        self.program = program
        self.globals = set(program.global_variables)
        self._free: typing.Dict[str, typing.Set[str]] = {}
        self._computing: typing.Set[str] = set()

    # -- names -------------------------------------------------------------

    def _seen(self, name: str, scopes: typing.List[typing.Set[str]]) -> bool:
        return name in self.globals or any(name in s for s in scopes)

    def free_names(self, component: str) -> typing.Set[str]:
        """Names a component's body (and defaults) read without declaring them:
        the parameters its callers must supply."""
        if component not in self._free:
            node = self.program.components[component]
            missing: typing.Set[str] = set()
            self._walk(node.body, [], missing, None)
            for attribute in node.options:
                self._walk(attribute.value, [], missing, None)
            self._free[component] = missing
        return self._free[component]

    # -- walk --------------------------------------------------------------

    def run(self) -> typing.List[str]:
        missing: typing.Set[str] = set()
        problems: typing.List[str] = []
        self._walk(self.program.chain, [], missing, problems)
        for name in sorted(missing):
            problems.append(unresolved_message(name))
        return problems

    def _options(self, options, scopes, missing, problems) -> typing.Set[str]:
        """Walk the values of an annotation list (evaluated in the scope around
        the node); returns the names it declares."""
        declared: typing.Set[str] = set()
        for attribute in options or []:
            self._walk(attribute.value, scopes, missing, problems)
            declared.add(attribute.attr)
        return declared

    def _walk(self, node, scopes, missing, problems) -> None:  # noqa: C901
        if node is None or isinstance(
            node,
            (
                ast.LiteralNode,
                ast.EnvironmentVariableAccessNode,
                ast.DescriptorNode,
                str,
                int,
                float,
                bool,
            ),
        ):
            return
        walk = lambda n, sc=scopes: self._walk(n, sc, missing, problems)  # noqa: E731

        if isinstance(node, ast.VariableAccessNode):
            if node.value is None and not self._seen(node.name, scopes):
                missing.add(node.name)
        elif isinstance(node, ast.PipelineGroupingNode):
            declared = self._options(node.options, scopes, missing, problems)
            inner = scopes + [declared]
            for expression in node.expressions:
                self._walk(expression, inner, missing, problems)
        elif isinstance(node, (ast.TaskNode, ast.MetaTaskNode)):
            self._options(node.options, scopes, missing, problems)
        elif isinstance(node, ast.ComponentRefNode):
            declared = self._options(node.options, scopes, missing, problems)
            component = self.program.components.get(node.name)
            if component is not None:
                provided = declared | {a.attr for a in component.options}
                for name in sorted(self.free_names(node.name) - provided):
                    if not self._seen(name, scopes):
                        if problems is None:
                            missing.add(name)  # a component calling another
                        else:
                            problems.append(unresolved_message(name, node.name))
        elif isinstance(node, ast.ConditionalNode):
            walk(node.task)
            for branch in node.branches:
                walk(branch)
        elif isinstance(node, ast.BranchNode):
            walk(node.task)
        elif isinstance(node, ast.RetryNode):
            walk(node.job)
            walk(node.attempts)
        elif isinstance(node, (ast.BinOpNode, ast.ComparisonExprNode)):
            walk(node.left)
            walk(node.right)
        elif isinstance(node, ast.NullCoalesceExprNode):
            walk(node.left)
            walk(node.right)
        elif isinstance(node, ast.UnaryOpNode):
            walk(node.right)
        elif isinstance(node, ast.TernaryExprNode):
            walk(node.condition)
            walk(node.true_expr)
            walk(node.false_expr)
        elif isinstance(node, ast.IndexExprNode):
            walk(node.collection)
            walk(node.index)
        elif isinstance(node, ast.ListNode):
            for item in node.value:
                walk(item)
        elif isinstance(node, ast.MapNode):
            for item in node.value.values():
                walk(item)


def check_scopes(program: ast.ProgramNode) -> None:
    """Raise ``SyntaxError`` if a ``$name`` in the program resolves to nothing.

    A name resolves if an enclosing annotation declares it, a global ``@name``
    exists, or (inside a component body) a caller supplies it: each invocation is
    checked against what the caller, its defaults and its own annotations declare.
    """
    problems = _Checker(program).run()
    if problems:
        raise SyntaxError("Parsing error: " + "; ".join(dict.fromkeys(problems)))
