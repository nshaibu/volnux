import typing
import logging
from .base import ControlFlowEvent, AttributesKwargs, MetaAttributes
from .runner import TemplateCaller

logger = logging.getLogger(__name__)


class ForeachEvent(ControlFlowEvent):
    """
    FOREACH Meta Event: run the template on each item for its side effects, and pass
    the items through unchanged.

    The template's results are discarded. The output stream is the input stream, so
    ``FOREACH<Log>`` can be put anywhere in a ``|->`` chain.

    Syntax:
        FOREACH<SideEffectEvent>[concurrent=true, continue_on_error=true]

    Attributes:
        - continue_on_error: Pass the item on even if the template failed
          (default: True); otherwise the stream stops with the failure
        - track_failures: Log failed operations (default: True)
        - batch_size: Items per template call (0 / 1: one)
    """

    name = "FOREACH"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "continue_on_error": {
            "type": bool,
            "default": True,
            "description": "Continue processing even if some operations fail",
        },
        "batch_size": {
            "type": int,
            "default": 0,
            "description": "Process items in batches (0 = no batching)",
            "validators": [lambda x: isinstance(x, int) and x >= 0],
        },
        "track_failures": {
            "type": bool,
            "default": True,
            "description": "Log and track failed operations for debugging",
        },
    }

    async def transform(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
    ) -> typing.AsyncIterator[typing.Tuple[bool, typing.Any]]:
        size = attributes.get("batch_size", 0)
        carry_on = attributes.get("continue_on_error", True)
        track = attributes.get("track_failures", True)

        async for outcome in self.outcomes(
            self.batched(items, size), caller, attributes
        ):
            if not outcome.ok:
                if track:
                    logger.warning(
                        "FOREACH: item %d failed: %s", outcome.index, outcome.error_text
                    )
                if not carry_on:
                    yield False, outcome.error_text
                    continue
            for item in outcome.item if size > 1 else [outcome.item]:
                yield True, item
