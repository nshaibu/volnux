import typing
from .base import (
    ControlFlowEvent,
    TaskDefinition,
    MetaEventExecutionError,
    MetaEventConfigurationError,
    AttributesKwargs,
)

from volnux.result import EventResult, ResultSet


class FanoutEvent(ControlFlowEvent):
    """
    FANOUT Meta Event: Broadcast same input to N instances.

    Syntax: FANOUT<TemplateEvent>[count=3, concurrent=true]

    Same input is sent to all instances.

    Attributes:
        - count (int, REQUIRED): Number of instances to create
        - concurrent (bool): Execute in parallel (default: true)
        - collection (list, tuple): Filter items based on template event predicate.
                                    Not required if items passed through message passing
    """

    name = "FANOUT"

    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "count": {
            "type": int,
            "required": True,
            "description": "Number of instances to fan out to",
        },
    }

    def _should_execute_concurrent(self) -> bool:
        """FANOUT defaults to concurrent execution"""
        if self.options:
            return getattr(self.options, "concurrent", True)
        return True

    def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
        """
        FANOUT business logic: Create N tasks with the same input

        Args:
            input_data: Data to broadcast to all instances (can be any type)

        Returns:
            List of TaskDefinition objects
        """
        # Get count
        count = self._get_count()

        if count <= 0:
            raise MetaEventConfigurationError(
                "FANOUT requires 'count' attribute with positive integer"
            )

        # Create N tasks with the same input
        task_defs = []
        for index in range(count):
            task_def = TaskDefinition(
                template_class=self.get_template_class(),
                input_data=input_data,  # Same input for all
                order=index,
                task_id=self._generate_task_id(index),
            )
            task_defs.append(task_def)

        return task_defs

    def _get_count(self) -> int:
        """Get fanout count from options or attribute"""
        if self.options:
            opt_count = self.options.extras.get("count", None)
            if opt_count:
                return opt_count

        raise MetaEventConfigurationError("FANOUT requires 'count' attribute")

    def aggregate_results(
        self, results: typing.List[EventResult], original_input: typing.Any
    ) -> typing.List:
        """
        FANOUT aggregation: Collect all results

        Args:
            results: List of EventResult from all instances
            original_input: Original input data

        Returns:
            List of all results (unordered)
        """
        return [r.content for r in results if not r.error]
