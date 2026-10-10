import typing
import logging
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs
from volnux.result import EventResult, ResultSet

logger = logging.getLogger(__name__)


class ReduceEvent(ControlFlowEvent):
    """
    REDUCE Meta Event: Fold/reduce a collection into a single value using accumulator pattern.

    Reduce iteratively combines elements of a collection into a single accumulated value
    by repeatedly applying a template event that takes (accumulator, current_item) and
    returns the new accumulator value.

    This is the classic functional programming reduce/fold operation, useful for:
    - Summing, counting, or aggregating values
    - Building complex objects from parts
    - Combining results sequentially
    - Implementing custom aggregation logic

    Accumulator Pattern:
        Template event receives: (accumulator, current_item)
        Template event returns: new_accumulator

        Result flow:
        acc = initial_value
        for item in collection:
            acc = template_event(acc, item)
        return acc

    Syntax:
        REDUCE<AccumulatorEvent>[initial_value=0, direction="left"]

    Examples:
        # Sum all numbers
        REDUCE<Add>[initial_value=0, collection=[1, 2, 3, 4]]
        # Flow: 0 + 1 = 1, 1 + 2 = 3, 3 + 3 = 6, 6 + 4 = 10
        # Returns: 10

        # Concatenate strings
        REDUCE<Concatenate>[initial_value="", collection=["Hello", " ", "World"]]
        # Returns: "Hello World"

        # Build object from parts
        REDUCE<MergeDict>[initial_value={}, collection=[{a:1}, {b:2}, {c:3}]]
        # Returns: {a:1, b:2, c:3}

        # Right-to-left reduction
        REDUCE<Subtract>[initial_value=100, direction="right", collection=[10, 5, 2]]
        # Flow: 100 - ((10 - (5 - 2))) = 100 - 13 = 87

        # Count items matching condition
        REDUCE<CountIfValid>[initial_value=0]
        # Returns: count of valid items

    Attributes:
        - initial_value: Starting value for accumulator (REQUIRED)
        - direction: Reduction direction - "left" or "right" (default: "left")
        - skip_errors: Skip items that cause errors (default: False)
        - concurrent: Must be False - reduce is inherently sequential (default: False)
        - collection: Input items if not passed via message passing
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

    def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
        """
        Generate sequential reduction tasks with accumulator pattern.

        Creates tasks that will be executed sequentially, where each task
        receives the accumulated value and the next item, returning the
        new accumulated value.

        The task order is critical - items are processed left-to-right
        (or right-to-left if direction="right").

        Args:
            input_data: Collection of items to reduce

        Returns:
            List of TaskDefinition objects in reduction order

        Note:
            Tasks must execute sequentially as each depends on the previous result
        """
        attributes = self._validate_attributes()
        direction = attributes.get("direction", "left").lower()

        # Validate we have initial_value
        if "initial_value" not in attributes:
            raise MetaEventConfigurationError(
                f"{self.name} requires 'initial_value' attribute. "
                f"Example: REDUCE<Add>[initial_value=0]"
            )

        # Convert input to list for ordering
        items = list(input_data)

        # Reverse if right-to-left reduction
        if direction == "right":
            items = list(reversed(items))
            logger.debug(f"{self.name}: Using right-to-left reduction order")

        # Create sequential tasks
        task_defs = []
        for index, item in enumerate(items):
            task_def = TaskDefinition(
                template_class=self.get_template_class(),
                input_data=item,  # Current item to process
                order=index,
                task_id=self._generate_task_id(index),
                options=self.options,
            )
            task_defs.append(task_def)

        logger.debug(
            f"{self.name}: Created {len(task_defs)} sequential reduction tasks "
            f"(direction: {direction})"
        )

        return task_defs

    def aggregate_results(
        self, results: ResultSet[EventResult], original_input: typing.Any
    ) -> typing.Any:
        """
        Perform sequential reduction by folding results with accumulator.

        Executes the reduction pattern:
        1. Start with initial_value as accumulator
        2. For each result in order:
           - Combine accumulator with current result
           - Update accumulator with template event output
        3. Return final accumulator value

        The template event is expected to implement the combining logic that
        takes (accumulator, item) and returns new_accumulator.

        Args:
            results: Results from reduction tasks (in execution order)
            original_input: Original input items

        Returns:
            Final accumulated value

        Raises:
            Exception: If skip_errors=False and any reduction step fails
        """
        attributes = self._validate_attributes()
        initial_value = attributes.get("initial_value")
        skip_errors = attributes.get("skip_errors", False)
        direction = attributes.get("direction", "left").lower()

        # Convert to list and sort by order
        results_list = list(results)
        sorted_results = sorted(results_list, key=lambda r: getattr(r, "order", 0))

        # Initialize accumulator
        accumulator = initial_value
        processed_count = 0
        skipped_count = 0

        logger.debug(
            f"{self.name}: Starting reduction with initial_value={initial_value}"
        )

        # Perform reduction
        for index, result in enumerate(sorted_results):
            if result.error:
                error_msg = f"Reduction step {index} failed: {result.content}"

                if skip_errors:
                    skipped_count += 1
                    logger.warning(f"{self.name}: {error_msg} (skipping)")
                    continue
                else:
                    logger.error(f"{self.name}: {error_msg}")
                    raise Exception(
                        f"{self.name} failed at step {index}/{len(sorted_results)}: "
                        f"{result.content}. Set skip_errors=true to continue on errors."
                    )

            # Extract the new accumulator value from result
            # Template event should return the new accumulator
            new_accumulator = result.content

            # Handle tuple format: (success, accumulator)
            if isinstance(new_accumulator, tuple) and len(new_accumulator) >= 2:
                new_accumulator = new_accumulator[1]

            # Update accumulator
            accumulator = new_accumulator
            processed_count += 1

            logger.debug(
                f"{self.name}: Step {index + 1}/{len(sorted_results)} - "
                f"accumulator updated (processed: {processed_count})"
            )

        logger.info(
            f"{self.name}: Reduction complete - processed {processed_count} items, "
            f"skipped {skipped_count} errors (direction: {direction})"
        )

        return accumulator

    def _handle_empty_input(self, input_data: typing.Any) -> typing.Any:
        """
        Handle empty input for REDUCE.

        Returns the initial_value since there are no items to reduce.
        This matches standard reduce semantics where reduce([]) = initial_value.

        Args:
            input_data: The empty input data

        Returns:
            initial_value attribute
        """
        attributes = self._validate_attributes()
        initial_value = attributes.get("initial_value")

        logger.info(
            f"{self.name}: No items to reduce, returning initial_value={initial_value}"
        )
        return initial_value

    def _should_execute_concurrent(self) -> bool:
        """
        REDUCE must execute sequentially.

        Reduction is inherently sequential as each step depends on
        the previous accumulator value. Concurrent execution would
        break the accumulator pattern.

        Returns:
            Always False
        """
        return False
