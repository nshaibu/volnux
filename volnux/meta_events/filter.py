import typing
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs
from volnux.result import EventResult, ResultSet


class FilterEvent(ControlFlowEvent):
    """
    FILTER Meta Event: Filter items based on template event predicate.

    Syntax: FILTER<PredicateEvent>[concurrent=true, keep_on_error=false]

    Template event should return descriptor 1 (include) or 0 (exclude).

    Attributes:
        - concurrent (bool): Execute predicates in parallel
        - keep_on_error (bool): Keep items that are error (default: false)
        - collection (list, tuple): Filter items based on template event predicate.
                                    Not required if items passed through message passing
    """

    name = "FILTER"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "keep_on_error": {
            "type": bool,
            "default": False,
            "description": "Keep items that are error (default: false)",
        },
    }

    def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
        """
        FILTER business logic: Create one predicate task per item

        Args:
            input_data: Collection of items to filter

        Returns:
            List of TaskDefinition objects
        """
        task_defs = []
        index = 1
        for item in input_data:
            task_def = TaskDefinition(
                template_class=self.get_template_class(),
                input_data=typing.cast(EventResult, item),
                order=index,
                task_id=self._generate_task_id(index),
            )
            task_defs.append(task_def)
            index += 1

        return task_defs

    def aggregate_results(
        self, results: typing.List[EventResult], original_input: typing.Any
    ) -> typing.List:
        """
        FILTER aggregation: Keep items where predicate succeeded

        Args:
            results: List of EventResult from predicate tasks
            original_input: Original input collection

        Returns:
            Filtered list of items
        """
        items = list(original_input)
        filtered = []

        # Sort by order to match original input
        sorted_results = sorted(results, key=lambda r: r.order)

        keep_on_error = False
        if self.options:
            keep_on_error = self.options.extras.get("keep_on_error", False)

        for idx, result in enumerate(sorted_results):
            if result.error:
                # Handle error based on keep_on_error flag
                if keep_on_error:
                    filtered.append(items[idx])
            else:
                # Check if predicate succeeded
                if self._is_predicate_success(result):
                    filtered.append(items[idx])

        return filtered

    @staticmethod
    def _is_predicate_success(result: EventResult) -> bool:
        """Check if a predicate result indicates success (include item)"""
        content = result.content

        # Handle tuple (success, data) format
        if isinstance(content, tuple) and len(content) >= 2:
            success, data = content[0], content[1]
            # Check if data is truthy or equals 1
            return bool(data) or data == 1

        # Handle direct boolean or descriptor
        return bool(content) or content == 1
