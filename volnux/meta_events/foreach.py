import typing
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs

from volnux.result import EventResult, ResultSet


class ForeachEvent(ControlFlowEvent):
    """
    FOREACH Meta Event: Execute template for side effects, pass through input.

    Syntax: FOREACH<SideEffectEvent>[concurrent=true, continue_on_error=true]

    Template event results are discarded, original input is returned.

    Attributes:
        - concurrent (bool): Execute in parallel
        - continue_on_error (bool): Continue even if instances fail
    """

    name = "FOREACH"
    attributes: typing.Dict[str, AttributesKwargs] = {
        **ControlFlowEvent.attributes,
        "continue_on_error": {
            "type": bool,
            "default": False,
            "description": "Continue even if instances fail",
        },
    }

    def action(self, input_data: ResultSet[EventResult]) -> typing.List[TaskDefinition]:
        """
        FOREACH business logic: Create one task per item for side effects

        Args:
            input_data: Collection of items to iterate over

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

    def _should_allow_partial_success(self) -> bool:
        """FOREACH allows continuing on error if configured"""
        if self.options:
            return self.options.extras.get("continue_on_error", True)
        return True

    def aggregate_results(
        self, results: typing.List[EventResult], original_input: typing.Any
    ) -> typing.Any:
        """
        FOREACH aggregation: Return original input unchanged

        Args:
            results: List of EventResult (ignored for FOREACH)
            original_input: Original input collection

        Returns:
            Original input unchanged
        """
        return original_input
