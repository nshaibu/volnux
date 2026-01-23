import typing
import logging
from .base import (
    ControlFlowEvent,
    TaskDefinition,
    MetaEventExecutionError,
    MetaEventConfigurationError,
    AttributesKwargs,
)

from volnux.result import EventResult, ResultSet


logger = logging.getLogger(__name__)


# class FanoutEvent(ControlFlowEvent):
#     """
#     FANOUT Meta Event: Broadcast same input to N instances.
#
#     Syntax: FANOUT<TemplateEvent>[count=3, concurrent=true]
#
#     Same input is sent to all instances.
#
#     Attributes:
#         - count (int, REQUIRED): Number of instances to create
#         - concurrent (bool): Execute in parallel (default: true)
#         - collection (list, tuple): Filter items based on template event predicate.
#                                     Not required if items passed through message passing
#     """
#
#     name = "FANOUT"
#
#     attributes: typing.Dict[str, AttributesKwargs] = {
#         **ControlFlowEvent.attributes,
#         "count": {
#             "type": int,
#             "required": True,
#             "description": "Number of instances to fan out to",
#         },
#     }
#
#     def _should_execute_concurrent(self) -> bool:
#         """FANOUT defaults to concurrent execution"""
#         if self.options:
#             return getattr(self.options, "concurrent", True)
#         return True
#
#     def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
#         """
#         FANOUT business logic: Create N tasks with the same input
#
#         Args:
#             input_data: Data to broadcast to all instances (can be any type)
#
#         Returns:
#             List of TaskDefinition objects
#         """
#         # Get count
#         count = self._get_count()
#
#         if count <= 0:
#             raise MetaEventConfigurationError(
#                 "FANOUT requires 'count' attribute with positive integer"
#             )
#
#         # Create N tasks with the same input
#         task_defs = []
#         for index in range(count):
#             task_def = TaskDefinition(
#                 template_class=self.get_template_class(),
#                 input_data=input_data,  # Same input for all
#                 order=index,
#                 task_id=self._generate_task_id(index),
#             )
#             task_defs.append(task_def)
#
#         return task_defs
#
#     def _get_count(self) -> int:
#         """Get fanout count from options or attribute"""
#         if self.options:
#             opt_count = self.options.extras.get("count", None)
#             if opt_count:
#                 return opt_count
#
#         raise MetaEventConfigurationError("FANOUT requires 'count' attribute")
#
#     def aggregate_results(
#         self, results: typing.List[EventResult], original_input: typing.Any
#     ) -> typing.List:
#         """
#         FANOUT aggregation: Collect all results
#
#         Args:
#             results: List of EventResult from all instances
#             original_input: Original input data
#
#         Returns:
#             List of all results (unordered)
#         """
#         return [r.content for r in results if not r.error]


class FanoutEvent(ControlFlowEvent):
    """
    FANOUT Meta Event: Broadcast same input to multiple concurrent instances.

    Fanout replicates the same input data to N independent instances of the template
    event, executing them concurrently. This is useful for:
    - Load distribution across multiple workers
    - Redundant processing for reliability
    - A/B testing with same input
    - Parallel algorithms requiring multiple independent runs

    Key Characteristics:
        - All instances receive identical input
        - Executes concurrently by default
        - Results can be aggregated in multiple ways
        - Useful for consensus, voting, or redundancy patterns

    Syntax:
        FANOUT<TemplateEvent>[count=3, concurrent=true]

    Examples:
        # Run same query on 3 different servers
        FANOUT<QueryDatabase>[count=3, aggregation_strategy="first"](query)
        # Returns: First successful result

        # Consensus pattern - majority voting
        FANOUT<ClassifyImage>[count=5, aggregation_strategy="majority"](image)
        # Returns: Most common classification

        # Collect all results for comparison
        FANOUT<GenerateResponse>[count=3, aggregation_strategy="all"](prompt)
        # Returns: [response1, response2, response3]

        # Redundancy with fallback
        FANOUT<FetchData>[count=3, aggregation_strategy="first", continue_on_error=true](url)
        # Returns: First successful fetch, others as fallback

    Attributes:
        - count: Number of instances to create (REQUIRED, must be > 0)
        - concurrent: Execute instances in parallel (default: True)
        - aggregation_strategy: How to combine results (default: "all")
            - "all": Return all results
            - "first": Return first successful result
            - "majority": Return most common result (voting)
            - "fastest": Return quickest result
            - "consensus": All must agree, else error
        - continue_on_error: Continue if some instances fail (default: False)
        - min_success_count: Minimum successful instances required (default: 1)
        - collection: Input data if not passed via message passing
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

    def _should_execute_concurrent(self) -> bool:
        """
        FANOUT defaults to concurrent execution.

        Since instances are independent and receive identical input,
        concurrent execution is the natural default.

        Returns:
            Value of concurrent attribute (default: True)
        """
        if self.options:
            return self.options.extras.get("concurrent", True)
        return True

    def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
        """
        Generate N identical tasks that will receive the same input.

        Creates multiple instances of the template event, all receiving
        identical input data for independent processing.

        Args:
            input_data: Data to broadcast to all instances

        Returns:
            List of TaskDefinition objects with identical input

        Raises:
            MetaEventConfigurationError: If count is invalid
        """
        attributes = self.options.extras
        count = attributes.get("count")

        if not count or count <= 0:
            raise MetaEventConfigurationError(
                f"{self.name} requires 'count' attribute with positive integer value. "
                f"Got: {count}"
            )

        # Validate min_success_count
        min_success = attributes.get("min_success_count", 1)
        if min_success > count:
            raise MetaEventConfigurationError(
                f"{self.name}: min_success_count ({min_success}) cannot exceed "
                f"count ({count})"
            )

        # Create N identical tasks
        task_defs = []
        for index in range(count):
            task_def = TaskDefinition(
                template_class=self.get_template_class(),
                input_data=input_data,  # Same input for all instances
                order=index,
                task_id=self._generate_task_id(index),
                options=self.options,
            )
            task_defs.append(task_def)

        logger.debug(
            f"{self.name}: Created {count} identical tasks "
            f"(template: {self.get_template_class().__name__})"
        )

        return task_defs

    def aggregate_results(
        self, results: ResultSet[EventResult], original_input: typing.Any
    ) -> typing.Any:
        """
        Aggregate fanout results based on configured strategy.

        Applies the selected aggregation strategy to combine results from
        multiple instances into a final output.

        Strategies:
            - all: Return all results (default)
            - first: Return first successful result
            - majority: Return most common result (voting)
            - fastest: Return quickest result (by order)
            - consensus: All must match, else error

        Args:
            results: Results from all fanout instances
            original_input: Original input data

        Returns:
            Aggregated result based on strategy

        Raises:
            Exception: If min_success_count not met or consensus fails
        """
        attributes = self._validate_attributes()
        strategy = attributes.get("aggregation_strategy", "all").lower()
        continue_on_error = attributes.get("continue_on_error", False)
        min_success = attributes.get("min_success_count", 1)
        count = attributes.get("count")

        # Separate successful and failed results
        results_list = list(results)
        successful = [r for r in results_list if not r.error]
        failed = [r for r in results_list if r.error]

        # Log execution summary
        logger.info(
            f"{self.name}: {len(successful)}/{count} instances succeeded, "
            f"{len(failed)} failed (strategy: {strategy})"
        )

        # Validate minimum success threshold
        if len(successful) < min_success:
            error_summary = self._get_error_summary(results_list)
            raise Exception(
                f"{self.name} failed: Only {len(successful)} of {count} instances "
                f"succeeded (minimum required: {min_success}). "
                f"Error summary: {error_summary}"
            )

        # Check if we should fail due to errors
        if failed and not continue_on_error:
            # Only fail if we don't have enough successes or strategy requires all
            if strategy == "consensus" or len(successful) < count:
                error_summary = self._get_error_summary(results_list)
                raise Exception(
                    f"{self.name} failed: {len(failed)} instance(s) failed. "
                    f"Set continue_on_error=true to allow partial success. "
                    f"Error summary: {error_summary}"
                )

        # Apply aggregation strategy
        if strategy == "all":
            return self._aggregate_all(successful)
        elif strategy == "first":
            return self._aggregate_first(successful)
        elif strategy == "majority":
            return self._aggregate_majority(successful)
        elif strategy == "fastest":
            return self._aggregate_fastest(successful)
        elif strategy == "consensus":
            return self._aggregate_consensus(successful)
        else:
            logger.warning(f"{self.name}: Unknown strategy '{strategy}', using 'all'")
            return self._aggregate_all(successful)

    def _aggregate_all(
        self, results: typing.List[EventResult]
    ) -> typing.List[typing.Any]:
        """
        Return all successful results.

        Args:
            results: Successful results

        Returns:
            List of all result contents
        """
        aggregated = [r.content for r in results]
        logger.debug(f"{self.name}: Returning all {len(aggregated)} results")
        return aggregated

    def _aggregate_first(self, results: typing.List[EventResult]) -> typing.Any:
        """
        Return the first successful result (by order).

        Useful for failover patterns where first success is sufficient.

        Args:
            results: Successful results

        Returns:
            Content of first successful result

        Raises:
            Exception: If no results available
        """
        if not results:
            raise Exception(f"{self.name}: No successful results for 'first' strategy")

        # Sort by order to get truly first result
        sorted_results = sorted(results, key=lambda r: getattr(r, "order", 0))
        first_result = sorted_results[0].content

        logger.debug(
            f"{self.name}: Returning first result from task {sorted_results[0].task_id}"
        )
        return first_result

    def _aggregate_majority(self, results: typing.List[EventResult]) -> typing.Any:
        """
        Return the most common result (voting/consensus).

        Useful for redundant processing where majority vote determines output.

        Args:
            results: Successful results

        Returns:
            Most common result content

        Raises:
            Exception: If no clear majority or results not comparable
        """
        if not results:
            raise Exception(
                f"{self.name}: No successful results for 'majority' strategy"
            )

        # Count occurrences of each result
        from collections import Counter

        try:
            # Extract contents and convert to hashable types if possible
            contents = []
            for r in results:
                content = r.content
                # Convert unhashable types to strings for comparison
                if isinstance(content, (list, dict)):
                    content = str(content)
                contents.append(content)

            # Find most common
            counter = Counter(contents)
            most_common = counter.most_common(1)[0]
            majority_result = most_common[0]
            vote_count = most_common[1]

            logger.debug(
                f"{self.name}: Majority result received {vote_count}/{len(results)} votes"
            )

            # Return original content (not stringified version)
            for r in results:
                content = r.content
                compare_content = (
                    str(content) if isinstance(content, (list, dict)) else content
                )
                if compare_content == majority_result:
                    return content

            return majority_result

        except Exception as e:
            raise Exception(
                f"{self.name}: Failed to compute majority vote - "
                f"results may not be comparable: {e}"
            )

    def _aggregate_fastest(self, results: typing.List[EventResult]) -> typing.Any:
        """
        Return the result that completed fastest (lowest order).

        Args:
            results: Successful results

        Returns:
            Content from fastest result
        """
        # Same as first for now, but could use timestamps in future
        return self._aggregate_first(results)

    def _aggregate_consensus(self, results: typing.List[EventResult]) -> typing.Any:
        """
        Verify all results match, return consensus value.

        All instances must produce identical output, else error.

        Args:
            results: Successful results

        Returns:
            Consensus result content

        Raises:
            Exception: If results don't all match
        """
        if not results:
            raise Exception(
                f"{self.name}: No successful results for 'consensus' strategy"
            )

        # Compare all results for equality
        first_content = results[0].content

        for r in results[1:]:
            if r.content != first_content:
                raise Exception(
                    f"{self.name}: Consensus failed - results do not match. "
                    f"Expected: {first_content}, Got: {r.content} from {r.task_id}"
                )

        logger.debug(f"{self.name}: Consensus achieved across {len(results)} instances")
        return first_content

    def _handle_empty_input(self, input_data: typing.Any) -> typing.List:
        """
        Handle empty input for FANOUT.

        Returns empty list since there's no data to fan out.

        Args:
            input_data: The empty input data

        Returns:
            Empty list
        """
        logger.info(f"{self.name}: No input data to fanout, returning empty list")
        return []
