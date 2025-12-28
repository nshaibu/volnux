import typing
import logging
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs, MetaAttributes
from volnux.result import EventResult, ResultSet

logger = logging.getLogger(__name__)


class FilterEvent(ControlFlowEvent):
    """
    FILTER Meta Event: Filter items based on template event predicate results.

    Filter evaluates a predicate event for each input item and keeps only those
    items where the predicate returns a truthy value or the descriptor 1.

    Predicate Return Values:
        - 1 or True: Include item in results
        - 0 or False: Exclude item from results
        - Tuple (success, value): Use value for filtering decision

    Behavior:
        - Processes each item through predicate independently
        - Maintains original order in filtered results
        - Handles errors based on keep_on_error configuration
        - Returns original input items (not predicate results)

    Syntax:
        FILTER<PredicateEvent>[concurrent=true, keep_on_error=false]

    Examples:
        # Filter valid emails
        FILTER<IsValidEmail>[collection=[email1, email2, email3]]
        # Returns: [email1, email3] if email2 failed validation

        # Filter with error tolerance
        FILTER<CheckAge>[keep_on_error=true](users)
        # Includes users even if age check fails

        # Filter documents by content
        FILTER<HasKeyword>[collection=$documents]
        # Returns: documents containing the keyword

    Attributes:
        - concurrent: Execute predicates in parallel (default: True)
        - concurrency_mode: Mode of concurrent execution (thread, process)
        - max_workers: Maximum number of concurrent executions (thread, process)
        - keep_on_error: Include items when predicate fails with error (default: False)
        - invert: Invert filter logic - keep items where predicate is False (default: False)
        - batch_size: Process items in batches (default: 0, no batching)
        - collection: Input items if not passed via message passing
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

    def aggregate_results(
        self, results: ResultSet[EventResult], original_input: typing.Any
    ) -> typing.List[typing.Any]:
        """
        Filter items based on predicate evaluation results.

        Evaluates each predicate result and keeps only items where:
        - Predicate returned truthy value (1, True, non-zero, etc.)
        - Predicate failed with error AND keep_on_error=True
        - Predicate returned falsy AND invert=True

        Args:
            results: Predicate evaluation results
            original_input: Original items being filtered

        Returns:
            Filtered list of original items (not predicate results)

        Note:
            Returns the ORIGINAL items that passed the filter,
            not the predicate evaluation results
        """
        attributes: MetaAttributes = self.options.extras
        keep_on_error = attributes.get("keep_on_error", False)
        invert = attributes.get("invert", False)

        # Convert to lists and sort by order
        items = list(original_input)
        sorted_results = sorted(results, key=lambda r: getattr(r, "order", 0))

        sorted_results = typing.cast(typing.List[EventResult], sorted_results)

        # Validate we have matching counts
        if len(sorted_results) != len(items):
            logger.warning(
                f"{self.name}: Result count ({len(sorted_results)}) doesn't match "
                f"input count ({len(items)}). This may indicate execution errors."
            )

        filtered = []
        included_count = 0
        excluded_count = 0
        error_count = 0

        for idx, result in enumerate(sorted_results):
            # Safety check for index bounds
            if idx >= len(items):
                logger.error(
                    f"{self.name}: Result index {idx} exceeds input size {len(items)}"
                )
                break

            original_item = items[idx]

            if result.error:
                # Handle predicate evaluation errors
                error_count += 1
                if keep_on_error:
                    filtered.append(original_item)
                    included_count += 1
                    logger.debug(
                        f"Item {idx} included due to predicate error "
                        f"(keep_on_error=True): {result.content}"
                    )
                else:
                    excluded_count += 1
                    logger.debug(
                        f"Item {idx} excluded due to predicate error: {result.content}"
                    )
            else:
                # Evaluate predicate result
                predicate_result = self._evaluate_predicate(result)

                # Apply invert logic if configured
                should_include = (
                    predicate_result if not invert else not predicate_result
                )

                if should_include:
                    filtered.append(original_item)
                    included_count += 1
                else:
                    excluded_count += 1

        logger.info(
            f"{self.name}: Filtered {len(items)} items -> {len(filtered)} results "
            f"(included: {included_count}, excluded: {excluded_count}, "
            f"errors: {error_count}, invert: {invert})"
        )

        return filtered

    def _evaluate_predicate(self, result: EventResult) -> bool:
        """
        Evaluate if a predicate result indicates the item should be included.

        Handles multiple predicate return formats:
        - Direct boolean: True/False
        - Descriptor: 1 (include) / 0 (exclude)
        - Tuple: (success, value) - uses value for decision
        - Numeric: Non-zero = True, Zero = False
        - String: Non-empty = True, Empty = False

        Args:
            result: EventResult from predicate evaluation

        Returns:
            True if item should be included, False otherwise
        """
        content = result.content

        return self._is_truthy(content)

    @staticmethod
    def _is_truthy(value: typing.Any) -> bool:
        """
        Determine if a value should be considered truthy for filtering.

        Special handling for descriptor pattern where 1 = True, 0 = False.

        Args:
            value: Value to evaluate

        Returns:
            True if value is truthy or equals 1, False otherwise
        """
        # Explicit descriptor check: 1 = include
        if value == 1:
            return True

        # Explicit descriptor check: 0 = exclude
        if value == 0:
            return False

        # Standard Python truthiness for other types
        return bool(value)

    def _handle_empty_input(self, input_data: typing.Any) -> typing.List:
        """
        Handle empty input for FILTER.

        Returns empty list since there's nothing to filter.

        Args:
            input_data: The empty input data

        Returns:
            Empty list
        """
        logger.info(f"{self.name}: No items to filter, returning empty list")
        return []
