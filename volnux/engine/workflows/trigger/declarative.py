"""
Triggers declared in a workflow's Pointy source (``@trigger: ...``).

The parser keeps each declaration as a ``TriggerNode``; here they become trigger
objects. Nothing is resolved earlier than it has to be:

* ``schedule`` / ``webhook`` are built once, when the workflow is registered, with
  the values known then (``$env`` is read at that moment).
* ``when <condition>`` is stored as an expression and evaluated again on every
  poll of the :class:`ConditionalTrigger`, so a changed environment is seen.
"""

import logging
import os
import typing

from volnux.parser import ast
from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.parser.exceptions import PointyParseError
from volnux.parser.triggers import CONDITION_FUNCTIONS, split_cron

from .triggers import (
    ConditionalTrigger,
    EventTrigger,
    SchedulerTrigger,
    TriggerBase,
    WebhookTrigger,
)

logger = logging.getLogger(__name__)

__all__ = [
    "TriggerDeclarationError",
    "TriggerExpressionEvaluator",
    "build_trigger",
    "build_triggers",
    "condition_functions",
    "register_condition_function",
]

ConditionFunction = typing.Callable[..., bool]

_conditions: typing.Dict[str, ConditionFunction] = {
    "file_exists": lambda path: os.path.isfile(str(path)),
}


class TriggerDeclarationError(ValueError):
    """A ``@trigger:`` declaration cannot be turned into a trigger."""


def register_condition_function(
    name: str, function: ConditionFunction, arity: int = 1
) -> None:
    """Make ``when <name>(...)`` available in Pointy sources.

    Semantic analysis only knows a function once it is registered, so register
    custom functions before the workflow source is analysed.
    """
    _conditions[name] = function
    CONDITION_FUNCTIONS[name] = arity


def condition_functions() -> typing.Dict[str, ConditionFunction]:
    return dict(_conditions)


def _coerce(value: typing.Optional[str]) -> typing.Any:
    """Environment values are strings; one that reads as a number or a boolean is
    that, so ``$env.QUEUE_DEPTH > 100`` compares numbers."""
    if value is None:
        return None
    text = value.strip()
    if text.lower() in ("true", "false"):
        return text.lower() == "true"
    for kind in (int, float):
        try:
            return kind(text)
        except ValueError:
            pass
    return value


class TriggerExpressionEvaluator(ExecutableASTGenerator):
    """Evaluates the expressions of a trigger declaration.

    The parser's expression evaluator, plus calls to condition functions and
    typed environment values.
    """

    def __init__(
        self,
        conditions: typing.Optional[typing.Mapping[str, ConditionFunction]] = None,
    ):
        super().__init__(task_template=None, grouping_template=None)  # type: ignore[arg-type]
        self._conditions = dict(_conditions if conditions is None else conditions)

    def evaluate(self, node: ast.ASTNode) -> typing.Any:
        return self._value(node)

    def visit_access_environment_variable(
        self, node: ast.EnvironmentVariableAccessNode
    ) -> typing.Any:
        return _coerce(node.resolve())

    def visit_call(self, node: ast.CallNode) -> typing.Any:
        function = self._conditions.get(node.name)
        if function is None:
            raise self._error(f"Unknown function '{node.name}'")
        args = [self._value(a) for a in node.args]
        kwargs = {k: self._value(v) for k, v in node.kwargs.items()}
        return function(*args, **kwargs)


def _arguments(
    node: ast.TriggerNode, evaluator: TriggerExpressionEvaluator
) -> typing.Tuple[typing.List[typing.Any], typing.Dict[str, typing.Any]]:
    try:
        return (
            [evaluator.evaluate(a) for a in node.args],
            {k: evaluator.evaluate(v) for k, v in node.kwargs.items()},
        )
    except PointyParseError as exc:
        raise TriggerDeclarationError(f"@trigger: {node.kind}: {exc}") from exc


def build_trigger(
    node: ast.TriggerNode,
    workflow_name: str,
    *,
    celery_app: typing.Any = None,
    web_app: typing.Any = None,
    conditions: typing.Optional[typing.Mapping[str, ConditionFunction]] = None,
    poll_interval: float = 60.0,
) -> TriggerBase:
    """Create the trigger a declaration describes.

    :raises TriggerDeclarationError: the declaration is wrong or lacks something it
        needs (a ``schedule`` without ``celery_app``, an unset ``$env`` value ...).
    """
    evaluator = TriggerExpressionEvaluator(conditions)
    label = f"@trigger: {node.kind}" + (f" (line {node.line})" if node.line else "")

    try:
        if node.kind == "schedule":
            if celery_app is None:
                raise TriggerDeclarationError(
                    f"{label} needs a Celery app: pass celery_app= when registering "
                    f"the workflow's declared triggers"
                )
            args, kwargs = _arguments(node, evaluator)
            if not args or not isinstance(args[0], str):
                raise TriggerDeclarationError(
                    f"{label}: the cron expression resolved to {args[:1]!r}"
                )
            return SchedulerTrigger(
                workflow_name,
                celery_app,
                timezone=kwargs.get("timezone"),
                **split_cron(args[0]),
            )

        if node.kind == "webhook":
            args, kwargs = _arguments(node, evaluator)
            if not args or not isinstance(args[0], str):
                raise TriggerDeclarationError(
                    f"{label}: the path resolved to {args[:1]!r}"
                )
            options = {
                "secret_token": kwargs.get("secret"),
                "token_header": kwargs.get("token_header"),
                "timestamp_header": kwargs.get("timestamp_header"),
                "tolerance_seconds": kwargs.get("tolerance_seconds"),
            }
            return WebhookTrigger(
                workflow_name,
                args[0],
                web_app=web_app,
                **{k: v for k, v in options.items() if v is not None},
            )

        if node.kind == "event":
            args, kwargs = _arguments(node, evaluator)
            if not args or not all(isinstance(a, str) and a for a in args):
                raise TriggerDeclarationError(
                    f"{label}: event types must be non-empty strings, got {args!r}"
                )
            pattern = kwargs.get("pattern")
            if pattern is not None and not isinstance(pattern, dict):
                raise TriggerDeclarationError(
                    f"{label}: pattern must be a map, got {pattern!r}"
                )
            return EventTrigger(
                workflow_name,
                args,
                event_pattern=pattern or None,
                name=kwargs.get("name"),
            )

        if node.kind == "when":
            condition = node.condition
            if condition is None:
                raise TriggerDeclarationError(f"{label} has no condition")

            def condition_fn() -> bool:
                # Evaluated on every poll. A failing condition counts as unmet
                # (and is logged) rather than ending the polling.
                try:
                    return bool(evaluator._truthy(evaluator.evaluate(condition)))
                except Exception:
                    logger.exception("%s: condition could not be evaluated", label)
                    return False

            return ConditionalTrigger(
                workflow_name, condition_fn, poll_interval=poll_interval
            )
    except TriggerDeclarationError:
        raise
    except (ValueError, TypeError) as exc:
        raise TriggerDeclarationError(f"{label}: {exc}") from exc

    raise TriggerDeclarationError(f"Unknown trigger '{node.kind}'")


def build_triggers(
    program: ast.ProgramNode, workflow_name: str, **kwargs: typing.Any
) -> typing.List[TriggerBase]:
    """The triggers declared by a parsed program, in source order."""
    return [build_trigger(node, workflow_name, **kwargs) for node in program.triggers]
