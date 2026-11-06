import typing
import logging
from .base import ControlFlowEvent, AttributesKwargs, MetaAttributes, _is_collection
from .runner import TemplateCaller

logger = logging.getLogger(__name__)


def flatten(value: typing.Any, depth: int) -> typing.Iterator[typing.Any]:
    """The elements of ``value``, lazily; ``depth`` levels of nesting are opened
    (1: ``value`` itself, -1: all). Strings and mappings are single items."""
    if depth == 0 or not _is_collection(value) or hasattr(value, "__aiter__"):
        yield value
        return
    for element in value:
        if depth == 1:
            yield element
        else:
            yield from flatten(element, depth - 1 if depth > 0 else depth)


class FlatMapEvent(ControlFlowEvent):
    """
    FLATMAP Meta Event: apply a template that returns a collection, and emit the
    elements of each collection as separate items.

    The flattening is lazy (no list of lists is built), duplicates and order are kept.

    Syntax:
        FLATMAP<ExtractLinks>[flatten_depth=1, skip_errors=true]

    Attributes:
        - flatten_depth: Levels to open (default 1; -1: all)
        - skip_errors: Drop an item whose template failed instead of failing
        - batch_size: Items per template call (0 / 1: one); the template then
          receives a list and returns one collection for the whole batch
    """

    name = "FLATMAP"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "flatten_depth": {
            "type": int,
            "default": 1,
            "description": "Number of levels to flatten results (1 = flatten one level, -1 = flatten all)",
            "validators": [lambda x: isinstance(x, int) and (x >= -1)],
        },
        "skip_errors": {
            "type": bool,
            "default": False,
            "description": "Continue processing even if some tasks fail",
        },
        "batch_size": {
            "type": int,
            "default": 0,
            "description": "Batch input items (0 = no batching)",
            "validators": [lambda x: isinstance(x, int) and x >= 0],
        },
    }

    async def transform(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
    ) -> typing.AsyncIterator[typing.Tuple[bool, typing.Any]]:
        depth = attributes.get("flatten_depth", 1)
        skip = attributes.get("skip_errors", False)
        size = attributes.get("batch_size", 0)

        async for outcome in self.outcomes(
            self.batched(items, size), caller, attributes
        ):
            if outcome.ok:
                for element in flatten(outcome.value, depth):
                    yield True, element
            elif skip:
                logger.warning(
                    "FLATMAP: skipping item %d: %s", outcome.index, outcome.error_text
                )
            else:
                yield False, outcome.error_text
