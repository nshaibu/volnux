import typing
import logging
from .base import ControlFlowEvent, AttributesKwargs, MetaAttributes
from .runner import TemplateCaller

logger = logging.getLogger(__name__)


class MapEvent(ControlFlowEvent):
    """
    MAP Meta Event: apply a template to each item of a stream.

    Each output item is the template's result for the input item at the same
    position. Results stream out as they are ready, in input order (``ordered=false``:
    as they finish), while at most ``max_workers`` items are being worked on and at most
    ``max_workers + buffer`` have been read.

    Syntax:
        MAP<TemplateEvent>[batch_size=10, max_workers=8]

    A failed item stops the stream (``fail_fast``) unless ``partial_success`` is set,
    which emits ``None`` for it, or ``skip_errors``, which emits nothing for it.

    With ``batch_size > 1`` the template receives a list of items and returns an
    iterable of results; each result is emitted on its own.

    Attributes:
       - batch_size: Number of items per template call (default: 1)
       - concurrent, concurrency_mode, max_workers, ordered, buffer, timeout:
         see ``ControlFlowEvent``
       - partial_success: Continue if some items fail (default: False)
       - skip_errors: Leave failed items out of the output (implies partial_success)
       - collection: Input items if not passed via message passing
    """

    name = "MAP"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "batch_size": {
            "type": int,
            "default": 1,
            "description": "Number of items to process per batch",
            "validators": [lambda x: isinstance(x, int) and x > 0],
        },
        "partial_success": {
            "type": bool,
            "default": False,
            "description": "Allow returning partial results when some items fail",
        },
        "skip_errors": {
            "type": bool,
            "default": False,
            "description": "Exclude failed items from results (implies partial_success)",
        },
    }

    async def transform(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
    ) -> typing.AsyncIterator[typing.Tuple[bool, typing.Any]]:
        size = attributes.get("batch_size", 1)
        skip = attributes.get("skip_errors", False)
        partial = attributes.get("partial_success", False) or skip

        async for outcome in self.outcomes(
            self.batched(items, size), caller, attributes
        ):
            if outcome.ok:
                if size > 1:
                    for value in outcome.value:
                        yield True, value
                else:
                    yield True, outcome.value
            elif skip:
                logger.warning(
                    "MAP: skipping item %d: %s", outcome.index, outcome.error_text
                )
            elif partial:
                logger.warning(
                    "MAP: item %d failed: %s", outcome.index, outcome.error_text
                )
                yield True, None
            else:
                yield False, outcome.error_text
