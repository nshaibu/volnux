import typing
import logging
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs
from volnux.result import EventResult, ResultSet


logger = logging.getLogger(__name__)

# class ForeachEvent(ControlFlowEvent):
#     """
#     FOREACH Meta Event: Execute template for side effects, pass through input.
#
#     Syntax: FOREACH<SideEffectEvent>[concurrent=true, continue_on_error=true]
#
#     Template event results are discarded, original input is returned.
#
#     Attributes:
#         - concurrent (bool): Execute in parallel
#         - continue_on_error (bool): Continue even if instances fail
#     """
#
#     name = "FOREACH"
#     attributes: typing.Dict[str, AttributesKwargs] = {
#         **ControlFlowEvent.attributes,
#         "continue_on_error": {
#             "type": bool,
#             "default": False,
#             "description": "Continue even if instances fail",
#         },
#     }
#
#     def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
#         """
#         FOREACH business logic: Create one task per item for side effects
#
#         Args:
#             input_data: Collection of items to iterate over
#
#         Returns:
#             List of TaskDefinition objects
#         """
#         items = self._validate_collection_input(input_data)
#
#         # Create one task per item
#         task_defs = []
#         for index, item in enumerate(items):
#             task_def = TaskDefinition(
#                 template_class=self.get_template_class(),
#                 input_data=item,
#                 order=index,
#                 task_id=self._generate_task_id(index),
#             )
#             task_defs.append(task_def)
#
#         return task_defs
#
#     def _should_allow_partial_success(self) -> bool:
#         """FOREACH allows continuing on error if configured"""
#         if self.options:
#             return self.options.extras.get("continue_on_error", True)
#         return True
#
#     def aggregate_results(
#         self, results: typing.List[EventResult], original_input: typing.Any
#     ) -> typing.Any:
#         """
#         FOREACH aggregation: Return original input unchanged
#
#         Args:
#             results: List of EventResult (ignored for FOREACH)
#             original_input: Original input collection
#
#         Returns:
#             Original input unchanged
#         """
#         return original_input


class ForeachEvent(ControlFlowEvent):
    """
    FOREACH Meta Event: Execute template for side effects while passing through input.

    Foreach executes a template event for each input item purely for its side effects
    (logging, notifications, database writes, API calls, etc.). The template event
    results are discarded and the original input is returned unchanged.

    Key Characteristics:
        - Executes template event for side effects only
        - Discards template event results
        - Returns original input unmodified
        - Useful for operations like logging, notifications, updates
        - Can continue processing even if some operations fail

    Syntax:
        FOREACH<SideEffectEvent>[concurrent=true, continue_on_error=true]

    Examples:
        # Log each processed item
        FOREACH<LogItemProcessed>(collection=[item1, item2, item3])
        # Returns: [item1, item2, item3] (unchanged)

        # Send notifications without blocking pipeline
        FOREACH<SendNotification>[continue_on_error=true](users)
        # Returns: users (notifications sent as side effect)

        # Update metrics for each item
        FOREACH<UpdateMetrics>[concurrent=true](transactions)
        # Returns: transactions (metrics updated)

    Attributes:
        - concurrent: Execute operations in parallel (default: True)
        - continue_on_error: Continue even if some operations fail (default: True)
        - batch_size: Process items in batches (default: 0, no batching)
        - track_failures: Log failed operations (default: True)
        - collection: Input items if not passed via message passing
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

    def _should_allow_partial_success(self) -> bool:
        """
        FOREACH allows continuing on error by default.

        Side effects are often non-critical operations (logging, notifications)
        where partial success is acceptable.

        Returns:
            Value of continue_on_error attribute (default: True)
        """
        if self.options:
            return self.options.extras.get("continue_on_error", True)
        return True

    def aggregate_results(
        self, results: ResultSet[EventResult], original_input: typing.Any
    ) -> typing.Any:
        """
        Return original input unchanged after side-effect execution.

        FOREACH discards template event results and passes through the original
        input unmodified. Optionally tracks and logs failures for debugging.

        Process:
            1. Count successful and failed operations
            2. Log failures if track_failures is enabled
            3. Raise exception if continue_on_error=False and failures occurred
            4. Return original input unchanged

        Args:
            results: Results from side-effect executions (will be discarded)
            original_input: Original input collection

        Returns:
            Original input unchanged (passthrough)

        Raises:
            Exception: If continue_on_error=False and any operation failed
        """
        attributes = self.options.extras
        continue_on_error = attributes.get("continue_on_error", True)
        track_failures = attributes.get("track_failures", True)

        # Analyze execution results for monitoring/debugging
        results_list = list(results)
        total_count = len(results_list)
        successful_count = sum(1 for r in results_list if not r.error)
        failed_count = total_count - successful_count

        # Track and log failures if enabled
        if track_failures and failed_count > 0:
            failed_results = [r for r in results_list if r.error]
            logger.warning(
                f"{self.name}: {failed_count} of {total_count} operations failed"
            )

            # Log individual failures for debugging
            for result in failed_results:
                logger.debug(
                    f"{self.name}: Task {result.task_id} failed: {result.content}"
                )

        # Raise exception if not configured to continue on error
        if not continue_on_error and failed_count > 0:
            error_summary = self._get_error_summary(results_list)
            raise Exception(
                f"{self.name} failed: {failed_count} of {total_count} operations "
                f"encountered errors. Set continue_on_error=true to allow partial success. "
                f"Error summary: {error_summary}"
            )

        # Log success summary
        logger.info(
            f"{self.name}: Completed {total_count} side-effect operations "
            f"(successful: {successful_count}, failed: {failed_count})"
        )

        # Return original input unchanged (passthrough behavior)
        return original_input

    def _handle_empty_input(self, input_data: typing.Any) -> typing.Any:
        """
        Handle empty input for FOREACH.

        Returns the original empty input unchanged, maintaining passthrough behavior.

        Args:
            input_data: The empty input data

        Returns:
            Original empty input unchanged
        """
        logger.info(f"{self.name}: No items to process, passing through empty input")
        return input_data

    def _get_execution_summary(
        self, results: typing.List[EventResult]
    ) -> typing.Dict[str, typing.Any]:
        """
        Generate detailed execution summary for FOREACH operations.

        Provides comprehensive statistics about side-effect execution,
        useful for monitoring and debugging.

        Args:
            results: List of all execution results

        Returns:
            Dictionary with execution statistics
        """
        total = len(results)
        successful = [r for r in results if not r.error]
        failed = [r for r in results if r.error]

        return {
            "total_operations": total,
            "successful_operations": len(successful),
            "failed_operations": len(failed),
            "success_rate": (len(successful) / total * 100) if total > 0 else 0,
            "failed_task_ids": [r.task_id for r in failed],
            "template_event": self.get_template_class().__name__,
        }
