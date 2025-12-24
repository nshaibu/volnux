import typing
from .base import ControlFlowEvent, TaskDefinition, AttributesKwargs
from volnux.result import EventResult


class ReduceEvent(ControlFlowEvent):
    """
    REDUCE Meta Event: Progressively combine items into single result.

    Syntax: REDUCE<CombineEvent>[initial_value=0, direction="left"]

    Template event receives item and reads accumulator from context.
    Accumulator is maintained in context.state.accumulator.

    Attributes:
        - initial_value (any): Starting accumulator value
        - direction (str): "left" or "right" (default: "left")
        - skip_errors (bool): Skip items that error
    """

    name = "REDUCE"

    attributes: typing.Dict[str, AttributesKwargs] = {
        "initial_value": {
            "type": object,
            "required": False,
            "description": "Initial accumulator value",
        },
        "direction": {
            "type": str,
            "default": "left",
            "validators": [lambda x: str(x).lower() in ["left", "right"]],
            "description": "Direction of accumulator",
        },
        "skip_errors": {
            "type": bool,
            "default": False,
            "description": "Skip items that error",
        },
        "collection": {
            "type": list,
            "default": [],
            "description": "Filter items based on template event predicate. Not required if items passed through message passing",
        },
    }

    def _should_execute_concurrent(self) -> bool:
        """REDUCE must always be sequential"""
        return False

    def action(self, input_data: typing.Any) -> typing.List[TaskDefinition]:
        """
        REDUCE business logic: Create sequential accumulator tasks

        Args:
            input_data: Collection of items to reduce

        Returns:
            List of TaskDefinition objects
        """
        items = self._validate_collection_input(input_data)

        if len(items) == 0:
            return []

        # Get initial value and direction
        initial = self._get_initial_value(items)
        direction = self._get_direction()

        # Initialize accumulator in context
        self._execution_context.state.accumulator = initial

        # Reverse for right-to-left reduction
        if direction == "right":
            items = list(reversed(items))

        # Determine starting index
        start_idx = 0 if self.initial_value is not None else 1

        # Create tasks for reduction
        task_defs = []
        for idx in range(start_idx, len(items)):
            task_def = TaskDefinition(
                template_class=self.template_class,
                input_data=items[idx],
                order=idx,
                task_id=self._generate_task_id(idx),
            )
            task_defs.append(task_def)

        return task_defs

    def _get_initial_value(self, items: typing.List) -> typing.Any:
        """Get initial accumulator value"""
        if self.initial_value is not None:
            return self.initial_value

        if self.options:
            opt_initial = getattr(self.options, "initial_value", None)
            if opt_initial is not None:
                return opt_initial

        # Use first item as initial value
        return items[0] if items else None

    def _get_direction(self) -> str:
        """Get reduction direction"""
        if self.options:
            return getattr(self.options, "direction", "left")
        return "left"

    def _execute_sequential(
        self, tasks: typing.List[PipelineTask], task_defs: typing.List[TaskDefinition]
    ) -> typing.List[EventResult]:
        """
        Execute REDUCE tasks sequentially, updating accumulator after each

        Args:
            tasks: List of PipelineTasks
            task_defs: List of TaskDefinitions

        Returns:
            List of EventResult objects
        """
        temp_results = []
        skip_errors = False

        if self.options:
            skip_errors = getattr(self.options, "skip_errors", False)

        for task, task_def in zip(tasks, task_defs):
            try:
                # Initialize event
                event = self._initialize_event(task, task_def)

                # Execute (template reads accumulator from context)
                result = event()
                result.order = task_def.order

                # Update accumulator if successful
                if not result.error:
                    # Extract new accumulator value
                    new_acc = result.content
                    if isinstance(new_acc, tuple) and len(new_acc) >= 2:
                        new_acc = new_acc[1]  # (success, accumulator)

                    self._execution_context.state.accumulator = new_acc
                elif not skip_errors:
                    # Error and not skipping - stop reduction
                    temp_results.append(result)
                    break

                temp_results.append(result)

            except Exception as e:
                logger.error(
                    f"REDUCE task {task_def.task_id} failed: {e}", exc_info=True
                )

                error_result = EventResult(
                    error=True,
                    event_name=task_def.template_class.__name__,
                    content=e,
                    task_id=task_def.task_id,
                    order=task_def.order,
                    init_params=None,
                    call_params=None,
                )
                temp_results.append(error_result)

                if not skip_errors:
                    break

        return temp_results

    def aggregate_results(
        self, results: typing.List[EventResult], original_input: typing.Any
    ) -> typing.Any:
        """
        REDUCE aggregation: Return final accumulator value

        Args:
            results: List of EventResult from reduction
            original_input: Original input collection

        Returns:
            Final accumulator value
        """
        return self._execution_context.state.accumulator

    def _handle_empty_input(self, input_data: typing.Any) -> typing.Any:
        """Handle empty collection for REDUCE"""
        if self.initial_value is not None:
            return self.initial_value

        if self.options:
            opt_initial = getattr(self.options, "initial_value", None)
            if opt_initial is not None:
                return opt_initial

        raise MetaEventExecutionError(
            "REDUCE requires at least one item when no initial_value provided"
        )
