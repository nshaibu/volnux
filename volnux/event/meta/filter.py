import typing
import logging
from .base import ControlFlowEvent, AttributesKwargs, MetaAttributes
from .runner import TemplateCaller

logger = logging.getLogger(__name__)


class FilterEvent(ControlFlowEvent):
    """
    FILTER Meta Event: keep the items for which the template (a predicate) is truthy.

    The output items are the *original* input items, in input order, as they are
    decided. Nothing is held back beyond the items in flight.

    Syntax:
        FILTER<IsValid>[invert=true, keep_on_error=true]

    With ``batch_size > 1`` the template receives a list of items and returns one flag
    per item.

    Attributes:
        - keep_on_error: Keep an item whose predicate failed (default: False; the
          stream then stops with the failure, as ``fail_fast`` does)
        - invert: Keep the items for which the predicate is falsy
        - batch_size: Items per predicate call (0 / 1: one)
    """

    name = "FILTER"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "keep_on_error": {
            "type": bool,
            "default": False,
            "description": "Include items when predicate evaluation fails with error",
        },
        "invert": {
            "type": bool,
            "default": False,
            "description": "Invert filter logic - keep items where predicate returns False",
        },
        "batch_size": {
            "type": int,
            "default": 0,
            "description": "Process items in batches (0 = no batching)",
            "validators": [lambda x: isinstance(x, int) and x >= 0],
        },
    }

    async def transform(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
    ) -> typing.AsyncIterator[typing.Tuple[bool, typing.Any]]:
        size = attributes.get("batch_size", 0)
        invert = attributes.get("invert", False)
        keep_on_error = attributes.get("keep_on_error", False)

        async for outcome in self.outcomes(
            self.batched(items, size), caller, attributes
        ):
            if not outcome.ok:
                if not keep_on_error:
                    yield False, outcome.error_text
                    continue
                logger.warning(
                    "FILTER: keeping item %d after: %s",
                    outcome.index,
                    outcome.error_text,
                )
                kept = outcome.item if size > 1 else [outcome.item]
                for item in kept:
                    yield True, item
                continue
            if size > 1:
                flags = list(outcome.value)
                if len(flags) != len(outcome.item):
                    yield False, (
                        f"FILTER: the predicate returned {len(flags)} flags for "
                        f"{len(outcome.item)} items"
                    )
                    continue
                for item, flag in zip(outcome.item, flags):
                    if bool(flag) != invert:
                        yield True, item
            elif bool(outcome.value) != invert:
                yield True, outcome.item
