import typing
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs

from volnux.result import EventResult, ResultSet


class FlatmapEvent(ControlFlowEvent):
    """
    FLATMAP Meta Event: Map and flatten nested results.

    Syntax: FLATMAP<TemplateEvent>[concurrent=true, flatten_depth=1]

    Template event can return collections, which are flattened.

    Attributes:
        - concurrent (bool): Execute in parallel
        - flatten_depth (int): Levels to flatten (default: 1)
        - collection (list, tuple): Filter items based on template event predicate.
                                    Not required if items passed through message passing
    """

    name = "FLATMAP"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "flatten_depth": {
            "type": int,
            "default": 1,
            "description": "Levels to flatten (default: 1)",
        },
    }

    def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
        """
        FLATMAP business logic: Create one task per item

        Args:
            input_data: Collection of items to flatmap

        Returns:
            List of TaskDefinition objects
        """
        items = self._validate_collection_input(input_data)

        # Create one task per item
        task_defs = []
        for index, item in enumerate(items):
            task_def = TaskDefinition(
                template_class=self.get_template_class(),
                input_data=item,
                order=index,
                task_id=self._generate_task_id(index),
            )
            task_defs.append(task_def)

        return task_defs

    def aggregate_results(
        self, results: typing.List[EventResult], original_input: typing.Any
    ) -> typing.List:
        """
        FLATMAP aggregation: Flatten nested results

        Args:
            results: List of EventResult from tasks
            original_input: Original input collection

        Returns:
            Flattened list
        """
        flatten_depth = 1
        if self.options:
            flatten_depth = getattr(self.options, "flatten_depth", 1)

        # Sort by order
        sorted_results = sorted(results, key=lambda r: r.order)

        # Extract content and flatten
        flattened = []
        for result in sorted_results:
            if not result.error:
                content = result.content

                # Extract from tuple if needed
                if isinstance(content, tuple) and len(content) >= 2:
                    content = content[1]

                # Flatten based on depth
                if flatten_depth > 0 and isinstance(content, (list, tuple)):
                    flattened.extend(content)
                else:
                    flattened.append(content)

        return flattened
