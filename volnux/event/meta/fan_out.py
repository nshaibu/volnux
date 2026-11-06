import asyncio
import json
import typing
import logging
from collections import Counter
from .base import ControlFlowEvent, AttributesKwargs, MetaAttributes
from .runner import TemplateCaller

logger = logging.getLogger(__name__)


def _key(value: typing.Any) -> str:
    return json.dumps(value, sort_keys=True, default=repr)


class FanoutEvent(ControlFlowEvent):
    """
    FANOUT Meta Event: run the template ``count`` times on each item, concurrently, and
    combine the answers into one output item per input item.

    For redundancy and hedging: ask several replicas (several model calls, several
    servers) and take their answer by a strategy. Items are handled one after another;
    the replicas of an item run together, at most ``max_workers`` at once.

    Syntax:
        FANOUT<Ask>[count=3, aggregation_strategy='majority']

    Attributes:
        - count: Replicas per item (required)
        - aggregation_strategy:
            ``all``       the list of the successful answers, in replica order;
            ``first``     the first successful answer in replica order (waits for all);
            ``fastest``   the first answer to arrive; the others are cancelled;
            ``majority``  the answer more than half of the replicas gave;
            ``consensus`` the answer, if every successful replica gave the same.
        - min_success_count: Successful replicas needed (default 1)
        - continue_on_error: Tolerate failed replicas as long as ``min_success_count``
          succeed; otherwise any failed replica fails the item (``fastest`` is
          always tolerant)
    """

    name = "FANOUT"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "count": {
            "type": int,
            "required": True,
            "description": "Number of instances to fan out to (must be positive)",
            "validators": [lambda x: isinstance(x, int) and x > 0],
        },
        "aggregation_strategy": {
            "type": str,
            "default": "all",
            "description": "Strategy for aggregating results: all, first, majority, fastest, consensus",
            "validators": [
                lambda x: isinstance(x, str)
                and x.lower() in ["all", "first", "majority", "fastest", "consensus"]
            ],
        },
        "continue_on_error": {
            "type": bool,
            "default": False,
            "description": "Continue processing even if some instances fail",
        },
        "min_success_count": {
            "type": int,
            "default": 1,
            "description": "Minimum number of successful instances required",
            "validators": [lambda x: isinstance(x, int) and x > 0],
        },
    }

    async def transform(
        self,
        items: typing.AsyncIterator[typing.Any],
        caller: TemplateCaller,
        attributes: MetaAttributes,
    ) -> typing.AsyncIterator[typing.Tuple[bool, typing.Any]]:
        count = attributes["count"]
        strategy = attributes.get("aggregation_strategy", "all").lower()
        tolerant = attributes.get("continue_on_error", False) or strategy == "fastest"
        needed = attributes.get("min_success_count", 1)
        gate = asyncio.Semaphore(attributes.get("max_workers", 4))

        async def replica(item: typing.Any) -> typing.Any:
            async with gate:
                return await caller.call(item)

        async for item in items:
            tasks = [asyncio.ensure_future(replica(item)) for _ in range(count)]
            try:
                if strategy == "fastest":
                    answers, errors = await self._first_answer(tasks)
                else:
                    settled = await asyncio.gather(*tasks, return_exceptions=True)
                    errors = [r for r in settled if isinstance(r, Exception)]
                    answers = [r for r in settled if not isinstance(r, Exception)]
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            if errors and not tolerant:
                yield False, f"FANOUT: {len(errors)} of {count} replicas failed: {errors[0]!r}"
                continue
            if len(answers) < needed:
                yield False, (
                    f"FANOUT: {len(answers)} replicas succeeded, {needed} needed"
                    + (f" ({errors[0]!r})" if errors else "")
                )
                continue
            yield self._aggregate(strategy, answers, count)

    @staticmethod
    async def _first_answer(
        tasks: typing.List[asyncio.Future],
    ) -> typing.Tuple[list, list]:
        errors: list = []
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task.exception() is None:
                    return [task.result()], errors
                errors.append(task.exception())
        return [], errors

    @staticmethod
    def _aggregate(
        strategy: str, answers: list, count: int
    ) -> typing.Tuple[bool, typing.Any]:
        if strategy == "all":
            return True, answers
        if strategy in ("first", "fastest"):
            return True, answers[0]
        votes = Counter(_key(a) for a in answers)
        top_key, top_votes = votes.most_common(1)[0]
        winner = next(a for a in answers if _key(a) == top_key)
        if strategy == "majority":
            if top_votes * 2 > count:
                return True, winner
            return False, f"FANOUT: no majority among {count} replicas"
        if len(votes) == 1:  # consensus
            return True, winner
        return False, f"FANOUT: the replicas disagree ({len(votes)} different answers)"
