import typing
import logging
from .base import ControlFlowEvent, AttributesKwargs, MetaAttributes
from .runner import TemplateCaller

logger = logging.getLogger(__name__)


class ReduceEvent(ControlFlowEvent):
    """
    REDUCE Meta Event: fold the items into one value.

    ``accumulator = template(accumulator, item)`` for each item, starting from
    ``initial_value``; the final accumulator is the event's single output item. A
    template event receives ``[accumulator, item]`` as its ``previous_result``; a
    function is called ``fn(accumulator, item)``.

    A fold is sequential, so there is no concurrency here, but the input is read as a
    stream and never held. The accumulator is checkpointed with the stream position
    every ``checkpoint_every`` items (``stream_policy``), so a restarted run continues
    from there; it must be JSON-serialisable for that. ``direction='right'`` has to see
    the last item first, so it is the one case that holds the whole input.

    Syntax:
        REDUCE<Sum>[initial_value=0]

    Attributes:
        - initial_value: Starting accumulator (required)
        - direction: 'left' (default) or 'right'
        - skip_errors: Leave out an item whose fold step failed and go on
    """

    name = "REDUCE"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "initial_value": {
            "type": object,
            "required": True,
            "description": "Initial accumulator value (starting point for reduction)",
        },
        "direction": {
            "type": str,
            "default": "left",
            "description": "Reduction direction: 'left' (left-to-right) or 'right' (right-to-left)",
            "validators": [
                lambda x: isinstance(x, str) and x.lower() in ["left", "right"]
            ],
        },
        "skip_errors": {
            "type": bool,
            "default": False,
            "description": "Skip items that cause reduction errors and continue",
        },
        "concurrent": {
            "type": bool,
            "default": False,
            "description": "Reduce is inherently sequential - concurrent execution not supported",
            "validators": [lambda x: x is False or not x],  # Must be False
        },
    }

    _reduce_acc: typing.Any = None
    _reduce_started: bool = False

    async def transform(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
    ) -> typing.AsyncIterator[typing.Tuple[bool, typing.Any]]:
        skip = attributes.get("skip_errors", False)
        every = self.stream_policy.checkpoint_every

        if not self._reduce_started:  # not restored from a checkpoint
            self._reduce_acc = attributes["initial_value"]
            self._reduce_started = True

        source = items
        if str(attributes.get("direction", "left")).lower() == "right":
            source = _reversed(items)

        seen = 0
        async for item in source:
            try:
                self._reduce_acc = await caller.call_with(self._reduce_acc, item)
            except Exception as exc:
                if not skip:
                    yield False, f"{type(exc).__name__}: {exc}"
                    return
                logger.warning("REDUCE: skipping item %d: %s", seen, exc)
            seen += 1
            if every and seen % every == 0:
                await self.enqueue_checkpoint()

        yield True, self._reduce_acc


async def _reversed(
    items: typing.AsyncIterator[typing.Any],
) -> typing.AsyncIterator[typing.Any]:
    collected = [item async for item in items]
    for item in reversed(collected):
        yield item
