import typing
import logging
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs
from volnux.result import EventResult, ResultSet


logger = logging.getLogger(__name__)


class MapEvent(ControlFlowEvent):
    """
    MAP Meta Event: Apply a template event to each item in a collection.

    Map is a fundamental control flow pattern that transforms each input item
    independently through the same template event, producing a collection of results.

    Behavior:
        - Processes each input item through the template event
        - Maintains input order in output (unless errors occur)
        - Supports batching for efficiency
        - Can run concurrently for better performance
        - Handles partial failures based on configuration

    Syntax:
        MAP<TemplateEvent>[batch_size=10, concurrent=true]

    Examples:
        # Process each document
        MAP<ExtractText>(collection=[doc1, doc2, doc3])
        # Returns: [text1, text2, text3]

        # Process with batching
        MAP<ValidateEmail>[batch_size=100](emails)
        # Processes 100 emails per batch

        # Allow partial success
        MAP<FetchUserData>[partial_success=true](user_ids)
        # Returns results even if some fetches fail

    Attributes:
       - batch_size: Number of items to process per batch (default: 1)
       - concurrent: Execute tasks in parallel (default: True)
       - concurrency_mode: Mode of concurrent execution (thread, process)
       - max_workers: Maximum number of concurrent executions (thread, process)
       - max_workers: Maximum parallel executions (default: 4)
       - partial_success: Continue if some tasks fail (default: False)
       - skip_errors: Skip failed items in output (default: False)
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
            "description": "Allow returning partial results when some tasks fail",
        },
        "skip_errors": {
            "type": bool,
            "default": False,
            "description": "Exclude failed items from results (only with partial_success)",
        },
    }

    def aggregate_results(
        self, results: ResultSet[EventResult], original_input: typing.Any
    ) -> typing.List[typing.Any]:
        """
        Aggregate MAP results maintaining input order.

        Collects results from all tasks, preserving the original order of inputs.
        Handles errors based on partial_success and skip_errors settings.

        Process:
            1. Sort results by execution order
            2. Check for errors and handle based on configuration
            3. Extract content from successful results
            4. Return ordered list

        Args:
            results: Results from all executed tasks
            original_input: Original input collection (for context)

        Returns:
            Ordered list of results (content only, not EventResult objects)

        Raises:
            Exception: If partial_success=False and any task failed
        """
        attributes = self.options.extras
        partial_success = attributes.get("partial_success", False)
        skip_errors = attributes.get("skip_errors", False)

        sorted_results = sorted(results, key=lambda r: getattr(r, "order", 0))

        sorted_results = typing.cast(typing.List[EventResult], sorted_results)

        # Separate successful and failed results
        successful = []
        failed = []

        for result in sorted_results:
            if result.error:
                failed.append(result)
                logger.warning(
                    f"Task {result.task_id} failed with error: {result.content}"
                )
            else:
                successful.append(result)

        # Handle failures based on configuration
        if failed and not partial_success:
            error_summary = self._get_error_summary(sorted_results)
            raise Exception(
                f"{self.name} failed: {len(failed)} of {len(sorted_results)} tasks failed. "
                f"Set partial_success=true to allow partial results. "
                f"Error summary: {error_summary}"
            )

        # Build result collection
        if skip_errors:
            # Only include successful results
            aggregated = [r.content for r in successful]
            if failed:
                logger.info(
                    f"{self.name}: Returning {len(successful)} successful results, "
                    f"skipping {len(failed)} failed items"
                )
        else:
            # Include all results, maintaining order (errors as None or original)
            aggregated = []
            for result in sorted_results:
                if result.error and partial_success:
                    # Include placeholder for failed item to maintain order
                    aggregated.append(None)
                else:
                    aggregated.append(result.content)

        logger.info(
            f"{self.name}: Processed {len(sorted_results)} tasks -> "
            f"{len(aggregated)} results "
            f"({len(successful)} successful, {len(failed)} failed)"
        )

        return aggregated

    def _handle_empty_input(self, input_data: typing.Any) -> typing.List:
        """
        Handle empty input for MAP.

        Returns empty list to maintain type consistency with non-empty case.

        Args:
            input_data: The empty input data

        Returns:
            Empty list
        """
        logger.info(f"{self.name}: No items to map over, returning empty list")
        return []
