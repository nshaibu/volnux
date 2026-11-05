"""
Event sources declared in a workflow's Pointy script (``@source: ...``).

``@source: hub("trading_events", "trading-events", version="2.1.0")`` becomes the
``WorkflowSource`` that ``ready()`` would build by hand. Values are evaluated when
the sources are built (``$env`` is read at that moment). Loading stays with the
registry: this module only describes the sources.
"""

import typing
from pathlib import Path

from volnux.parser import ast
from volnux.parser.code_gen import PointyParseError
from volnux.parser.sources import SOURCE_KEYWORDS, canonical_kind

from .source import RegistrySource, SourceCredentials, WorkflowSource
from .trigger.declarative import TriggerExpressionEvaluator

__all__ = ["SourceDeclarationError", "build_source", "build_sources"]

# keywords that are fields of WorkflowSource; the others go to its metadata
_FIELDS = ("version", "timeout", "retries", "priority", "events")


class SourceDeclarationError(ValueError):
    """A ``@source:`` declaration cannot be turned into a WorkflowSource."""


def build_source(
    node: ast.SourceNode, base_dir: typing.Optional[Path] = None
) -> WorkflowSource:
    """The ``WorkflowSource`` a declaration describes.

    :param base_dir: A relative ``local`` location is resolved against it (the
        workflow's own directory).
    :raises SourceDeclarationError: the declaration is invalid; the message names
        it and its line.
    """
    label = f"@source: {node.kind}" + (f" (line {node.line})" if node.line else "")
    kind = canonical_kind(node.kind)
    allowed = SOURCE_KEYWORDS.get(kind)
    if allowed is None:
        raise SourceDeclarationError(f"{label}: unknown source '{node.kind}'")

    evaluator = TriggerExpressionEvaluator()
    try:
        args = [evaluator.evaluate(a) for a in node.args]
        kwargs = {k: evaluator.evaluate(v) for k, v in node.kwargs.items()}
    except PointyParseError as exc:
        raise SourceDeclarationError(f"{label}: {exc}") from exc

    unknown = sorted(set(kwargs) - set(allowed))
    if unknown:
        raise SourceDeclarationError(f"{label}: unknown argument(s) {unknown}")

    if not args or not isinstance(args[0], str) or not args[0].isidentifier():
        raise SourceDeclarationError(
            f"{label}: the namespace must be an identifier, got "
            f"{args[0] if args else None!r}"
        )
    namespace = args[0]
    location = args[1] if len(args) > 1 else kwargs.pop("location", None)
    if not isinstance(location, str) or not location.strip():
        raise SourceDeclarationError(f"{label}: a location is required")
    kwargs.pop("location", None)

    if kind == "local":
        path = Path(location).expanduser()
        if not path.is_absolute() and base_dir is not None:
            path = Path(base_dir) / path
        location = str(path)

    token = kwargs.pop("token", None)
    credentials = SourceCredentials(token=token) if token else None
    fields = {k: kwargs.pop(k) for k in _FIELDS if k in kwargs}
    if "events" in fields and not isinstance(fields["events"], list):
        raise SourceDeclarationError(f"{label}: events must be a list of names")

    try:
        return WorkflowSource(
            name=namespace,
            source_type=RegistrySource(kind),
            location=location,
            credentials=credentials,
            metadata=kwargs,
            **fields,
        )
    except (ValueError, TypeError) as exc:
        raise SourceDeclarationError(f"{label}: {exc}") from exc


def build_sources(
    program: ast.ProgramNode, base_dir: typing.Optional[Path] = None
) -> typing.List[WorkflowSource]:
    """The sources a parsed program declares, in source order."""
    return [build_source(node, base_dir) for node in program.sources]
