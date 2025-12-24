import typing
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs
from volnux.result import EventResult, ResultSet


class MapEvent(ControlFlowEvent):
    """
    MAP Meta Event: Execute template event for each item in input collection.

    Syntax: MAP<TemplateEvent>[batch_size=10, concurrent=true]

    Attributes:
        - batch_size (int): Number of items per batch
        - concurrent (bool): Execute in parallel
        - max_workers (int): Max parallel executions
        - partial_success (bool): Allow partial results
        - collection (list, tuple): Filter items based on template event predicate.
                                    Not required if items passed through message passing
    """

    name = "MAP"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "batch_size": {
            "type": int,
            "default": 1,
            "description": "Number of items per batch",
            "validators": [lambda x: isinstance(x, int) and x > 0],
        },
        "partial_success": {
            "type": bool,
            "default": False,
            "description": "Allow partial results",
        },
    }

    def action(self, input_data: ResultSet) -> typing.List[TaskDefinition]:
        """
        MAP business logic: Create one task per item

        Args:
            input_data: Collection of items to map over

        Returns:
            List of TaskDefinition objects
        """
        batch_size: int = self.options.extras.get("batch_size", 0)
        task_defs = []
        index = 1

        for item in self.batch_input_data(input_data, batch_size):
            task_def = TaskDefinition(
                template_class=self.get_template_class(),
                input_data=item,
                order=index,
                task_id=self._generate_task_id(index),
            )
            index += 1
            task_defs.append(task_def)

        return task_defs

    def aggregate_results(
        self, results: typing.List[EventResult], original_input: typing.Any
    ) -> typing.List:
        """
        MAP aggregation: Collect results in order

        Args:
            results: List of EventResult from tasks
            original_input: Original input collection

        Returns:
            List of results in order
        """
        # Sort by order and extract content
        sorted_results = sorted(results, key=lambda r: r.order)
        return [r.content for r in sorted_results if not r.error]
