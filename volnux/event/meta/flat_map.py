import typing
import logging
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs

from volnux.result import EventResult, ResultSet


logger = logging.getLogger(__name__)


class FlatMapEvent(ControlFlowEvent):
    """
    FlatMap Meta Event - Maps a template event over input items and flattens results.

    FlatMap combines the functionality of Map and Flatten:
    1. Applies a template event to each input item (like Map)
    2. Flattens the results by one or more levels

    This is particularly useful when:
    - Template event returns collections that should be merged
    - You want to avoid nested result structures
    - Processing items that expand into multiple sub-items

    Examples:
        # Process documents and flatten paragraphs
        FlatMap<ExtractParagraphs>(collection=[doc1, doc2, doc3])
        # Returns: [para1, para2, para3, ...] instead of [[para1, para2], [para3], ...]

        # Expand categories into products
        FlatMap<GetProducts>(collection=[category1, category2])
        # Returns: [product1, product2, product3, ...] instead of [[product1], [product2, product3]]

    Attributes:
        - concurrent (bool): Execute in parallel
        - concurrency_mode: Mode of concurrent execution (thread, process)
        - max_workers: Maximum number of concurrent executions (thread, process)
        - flatten_depth: Number of levels to flatten (default: 1, use -1 for infinite)
        - batch_size: Optional batching of input items
        - skip_errors: Continue processing if individual tasks fail
        - collection: Input items if not passed via message passing
    """

    name: str = "FLATMAP"

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

    def aggregate_results(
        self, results: ResultSet[EventResult], original_input: typing.Any
    ) -> typing.List:
        """
        Aggregate and flatten results from all tasks.

        Args:
            results: Results from all executed tasks
            original_input: Original input data for context

        Returns:
            EventResult containing flattened collection

        Raises:
            Exception: If skip_errors is False and any task failed
        """
        attributes = self.options.extras
        skip_errors = attributes.get("skip_errors", False)
        flatten_depth = attributes.get("flatten_depth", 1)

        successful_results = []
        failed_results = []

        results = typing.cast(typing.Set[EventResult], results)

        # Separate successful and failed results
        for result in results:
            if result.error:
                failed_results.append(result)
                logger.warning(f"Task {result.task_id} failed: {result.content}")
            else:
                successful_results.append(result)

        # Handle failures
        if failed_results and not skip_errors:
            error_summary = self._get_error_summary(list(results))
            raise Exception(
                f"{self.name} failed: {len(failed_results)} task(s) encountered errors. "
                f"Summary: {error_summary}"
            )

        # Extract content from successful results
        collected_results = []
        for result in successful_results:
            collected_results.append(result.content)

        # Flatten the results
        flattened_results = self._flatten(collected_results, flatten_depth)

        logger.info(
            f"{self.name}: Processed {len(results)} tasks, "
            f"{len(successful_results)} successful, {len(failed_results)} failed. "
            f"Flattened to {len(flattened_results)} items (depth: {flatten_depth})"
        )

        return flattened_results

    def _flatten(
        self, items: typing.List[typing.Any], depth: int
    ) -> typing.List[typing.Any]:
        """
        Recursively flatten a nested list structure.

        Args:
            items: List to flatten (may contain nested lists)
            depth: Number of levels to flatten (-1 for infinite)

        Returns:
            Flattened list

        Examples:
            _flatten([[1, 2], [3, 4]], 1) -> [1, 2, 3, 4]
            _flatten([[[1]], [[2]]], 1) -> [[1], [2]]
            _flatten([[[1]], [[2]]], 2) -> [1, 2]
            _flatten([[[1]], [[2]]], -1) -> [1, 2]
        """
        if depth == 0:
            return items

        flattened = []
        for item in items:
            # Check if item is iterable but not string
            if self._is_flattenable(item):
                if depth == -1:
                    # Infinite depth - recursively flatten
                    flattened.extend(self._flatten(list(item), -1))
                else:
                    # Finite depth - flatten one level and recurse
                    flattened.extend(self._flatten(list(item), depth - 1))
            else:
                flattened.append(item)

        return flattened

    def _is_flattenable(self, item: typing.Any) -> bool:
        """
        Check if an item should be flattened.

        Items are flattenable if they're iterable collections but NOT:
        - Strings (to avoid character-by-character splitting)
        - Dicts (preserve as objects)
        - EventResult objects (preserve as results)

        Args:
            item: Item to check

        Returns:
            True if item should be flattened
        """
        if isinstance(item, (str, dict, EventResult)):
            return False

        # Check if iterable
        try:
            iter(item)
            return True
        except TypeError:
            return False

    def _handle_empty_input(self, input_data: typing.Any) -> typing.List:
        """
        Handle empty input for FlatMap.

        Returns empty list to maintain type consistency.

        Args:
            input_data: The empty input data

        Returns:
            Empty list
        """
        logger.info(f"{self.name}: No input data to process, returning empty list")
        return []
