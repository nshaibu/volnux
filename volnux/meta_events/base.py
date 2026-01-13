import typing
import asyncio
import logging
from abc import abstractmethod
from dataclasses import dataclass
from collections import deque

from volnux.typing import TypeAlias
from volnux.task import PipelineTask
from volnux.parser.options import Options
from volnux.base import EventBase, EventType
from volnux.constants import EMPTY
from volnux.result import EventResult, ResultSet
from volnux.signal.handlers.event_initialiser import ExtraEventInitKwargs
from volnux.exceptions import (
    MetaEventConfigurationError,
    MetaEventExecutionError,
    NestedMetaEventError,
    StopProcessingError,
)
from volnux.default_batch_processors import batched
from volnux.flows.meta import MetaFlow
from volnux.execution.result import ResultProcessor
from volnux.result_evaluators import ResultEvaluationStrategies
from volnux.utils import resolve_event_str_to_class


logger = logging.getLogger(__name__)

AttributesKwargs: TypeAlias = ExtraEventInitKwargs


class MetaAttributes(typing.TypedDict, total=False):
    collection: typing.List[typing.Any]
    keep_on_error: bool
    max_workers: int
    count: int
    batch_size: int
    flatten_depth: int
    direction: str
    skip_errors: bool
    initial_value: object
    continue_on_error: bool
    concurrent: bool
    concurrency_mode: typing.Literal["thread", "process"]
    timeout: typing.Optional[float]


def is_control_flow_event(
    event_class: typing.Union[EventBase, typing.Type[EventBase]],
) -> bool:
    """
    Check if an event class is a ControlFlowEvent (Meta Event)
    """
    return (
        event_class.__name__ == "ControlFlowEvent"
        or event_class.event_type == EventType.META
    )


def validate_meta_event(
    nested_event_class: typing.Union[str, typing.Type[EventBase]],
    field_name: str,
    meta_event: "ControlFlowEvent",
) -> None:
    """
    Validate meta-event at semantic analysis phase

    Args:
        nested_event_class: MetaEventNode from AST
        field_name: Dict mapping event names to their classes
        meta_event: MetaEventNode from AST

    Raises:
        NestedMetaEventError: If template is a meta event
        ValueError: If template event doesn't exist
    """
    resolved_event_class = resolve_event_str_to_class(nested_event_class)

    if is_control_flow_event(resolved_event_class):
        raise NestedMetaEventError(
            f"Meta Events cannot be used as Template Events. "
            f"Found: {meta_event.name}<{nested_event_class.__name__}> "
            f"Template Event '{field_name}' is a Meta Event, which is not allowed. "
            f"Use explicit sequential composition instead."
        )


@dataclass
class TaskDefinition:
    template_class: typing.Type[EventBase]
    input_data: typing.Union[ResultSet[EventResult], EventResult]
    order: int
    task_id: str
    options: typing.Optional[Options] = None

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return {
            "template_class": self.template_class.__name__,
            "input_data": self.input_data,
            "order": self.order,
            "task_id": self.task_id,
            "options": self.options,
        }


class ControlFlowEvent(EventBase):
    """
    Base class for Meta Events - reserved system components that manage
    dynamic execution of Template Events based on control flow patterns.

    A Meta Event is a reserved system component in Pointy Language that wraps and manages
    the execution of one or more Template Events.
    Its primary function is to interpret specialized flow instructions (execution modes)
    and generate dynamic execution plans for the Orchestrator.
    """

    # Name of the Meta Event
    name: str = "CONTROL_FLOW"

    # Attributes for the Meta Events. This will be used to fetch data
    # from the Options specific to Meta Events
    attributes: typing.Dict[str, AttributesKwargs] = {
        "concurrent": {
            "type": bool,
            "default": True,
            "description": "Allow parallel executions",
        },
        "concurrency_mode": {
            "type": str,
            "default": "thread",
            "description": "The parallel execution mode",
            "validators": [
                lambda x: isinstance(x, str) and x.lower() in ["thread", "process"]
            ],
        },
        "max_workers": {
            "type": int,
            "default": 4,
            "description": "Max parallel executions",
            "validators": [lambda x: isinstance(x, int) and x > 0],
        },
        "collection": {
            "type": list,
            "default": [],
            "description": "Filter items based on template event predicate. Not required if items passed through message passing",
        },
    }

    event_type = EventType.META

    EXTRA_INIT_PARAMS_SCHEMA: typing.Dict[str, ExtraEventInitKwargs] = {
        "template_class": {
            "type": EventBase,
            "required": True,
            "description": "The Template Event class to instantiate",
            "validators": [validate_meta_event],
        }
    }

    def _validate_attributes(self) -> MetaAttributes:
        """
        Validate meta-event attributes from Options
        Returns:
            typing.Dict[str, typing.Any]: Dictionary of validated attributes
        Raises:
            TypeError: If attributes are not valid
        """
        validate_params: MetaAttributes = {}
        for param_name, config in self.attributes.items():
            is_required = config.get("required", False)
            data_type = config.get("type", object)
            default_value = config.get("default", EMPTY)
            validators: typing.List[typing.Callable[[typing.Any], bool]] = config.get(
                "validators", []
            )

            value_to_set = self.options.extras.get(param_name, default_value)

            if value_to_set == EMPTY and default_value == EMPTY and is_required:
                raise TypeError(
                    f"Event '{self.__class__.__name__}' requires the keyword argument "
                    f"'{param_name}' for initialization."
                )

            if value_to_set == EMPTY:
                continue

            if value_to_set is not None and not isinstance(value_to_set, data_type):
                raise TypeError(
                    f"Event '{self.__class__.__name__}' requires type '{data_type}' for '{param_name}', "
                    f"but received type '{type(value_to_set)}' (value: {value_to_set})."
                )

            for validator in validators:
                if not validator(value_to_set):
                    raise TypeError(
                        f"Validation failed for parameter '{param_name}' in event '{self.__class__.__name__}'"
                    )

            validate_params[param_name] = value_to_set

        return validate_params

    def get_template_class(self) -> typing.Type[EventBase]:
        return getattr(self, "template_class", None)

    async def process(self, *args, **kwargs) -> typing.Tuple[bool, typing.Any]:
        """
        Mini-orchestrator that executes template events and aggregates results.

        This method is the same for all meta events. Business logic is in action().

        Returns:
            Tuple of (success, aggregated_data)
        """
        template_class = self.get_template_class()
        if template_class is None:
            raise StopProcessingError("No template class specified")

        try:
            if isinstance(template_class, str):
                setattr(
                    self, "template_class", resolve_event_str_to_class(template_class)
                )
        except ValueError as e:
            logger.error(e)
            raise StopProcessingError(str(e))

        try:
            attributes = self._validate_attributes()

            timeout: typing.Optional[float] = attributes.get("timeout")

            input_data = self._extract_input_data(
                collection_data=attributes.get("collection")
            )

            task_definitions = self.action(input_data)

            if not task_definitions:
                # Empty input handled by subclass
                return True, self._handle_empty_input(input_data)

            pipeline_tasks = self._create_pipeline_tasks(task_definitions)

            flow = MetaFlow(
                task_profiles=deque(pipeline_tasks),  # type: ignore
                context=self._execution_context,  # type: ignore
                attributes=attributes,  # type: ignore
            )
            run_coro = flow.run()
            future = (
                await asyncio.wait_for(run_coro, timeout=timeout)
                if timeout
                else await run_coro
            )

            if self._should_allow_partial_success():
                # Update on result evaluation strategy
                self.result_evaluation_strategy = (
                    ResultEvaluationStrategies.ANY_MUST_SUCCEED
                )

            temp_results, errors = await ResultProcessor().process_futures([future])

            aggregated = self.aggregate_results(temp_results, input_data)
            if not isinstance(aggregated, EventResult):
                aggregated = EventResult(
                    content=aggregated,  # type: ignore
                    error=False,  # type: ignore
                    event_name=self._generate_meta_event_name(),  # type: ignore
                    task_id=self._task_id,  # type: ignore
                )

            await self._execution_context.update_aggregated_result(aggregated)

            return True, temp_results

        except Exception as e:
            logger.error(f"{self.name} execution failed: {e}", exc_info=True)
            raise MetaEventExecutionError(f"{self.name} failed: {e}") from e

    @abstractmethod
    def action(self, input_data: ResultSet) -> typing.List[TaskDefinition]:
        """
        Define the business logic for this meta-event.

        This method generates task definitions based on input data.
        Each subclass implements its own logic.

        Args:
            input_data: The input data to process

        Returns:
            List of TaskDefinition objects
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement action()")

    @abstractmethod
    def aggregate_results(
        self, results: ResultSet[EventResult], original_input: typing.Any
    ) -> EventResult:
        """
        Aggregate individual task results into final output.

        Each subclass implements its own aggregation strategy.

        Args:
            results: List of EventResult from task execution
            original_input: The original input data

        Returns:
            Aggregated result
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _aggregate_results()"
        )

    def _extract_input_data(
        self, collection_data: typing.Optional[typing.List[typing.Any]] = None
    ) -> ResultSet:
        """
        Extract input data from previous_result and collection from a pointy script
        Args:
            collection_data: Optional collection data from options
        Returns:
            ResultSet: Set of results to process
        Raises:
            MetaEventExecutionError: if both previous_result and collection are empty
            TypeError: if collection data contains unhashable type
        """
        if self.previous_result == EMPTY and not collection_data:
            raise MetaEventExecutionError(
                f"{self.name} requires input data (from a previous event or 'collection' attribute)."
            )

        if self.previous_result == EMPTY:
            self.previous_result = ResultSet()

        if collection_data:
            for content in collection_data:
                if not isinstance(content, EventResult):
                    content = EventResult(
                        content=content,
                        error=False,  # type: ignore
                        task_id=f"meta-{self.__class__.__name__}-{self._task_id}",  # type: ignore
                    )
                self.previous_result.add(content)

        if self.previous_result.is_empty():
            raise MetaEventExecutionError(f"{self.name} received empty result set")

        return self.previous_result

    @staticmethod
    def batch_input_data(
        input_data: ResultSet, batch_size: int
    ) -> typing.Generator[ResultSet, None, None]:
        """
        Batch input data if batch_size is set in Options.

        Args:
            input_data: The full ResultSet from _extract_input_data
            batch_size: The number of items per batch

        Yields:
            A ResultSet containing a subset of the original EventResults
        """
        if batch_size <= 0:
            yield input_data
            return

        for batch_tuple in batched(input_data, batch_size):
            batch_set = ResultSet()
            for item in batch_tuple:
                batch_set.add(item)

            yield batch_set

    def _validate_collection_input(self, input_data: typing.Any) -> typing.List:
        """Validate that input is a collection"""
        if input_data is None:
            raise TypeError(f"Expected collection type for {self.name} mode, got None")

        if not isinstance(input_data, (list, tuple, set)):
            raise TypeError(
                f"Expected collection type for {self.name} mode, "
                f"got {type(input_data).__name__}"
            )

        return list(input_data)

    def _generate_task_id(self, index: int) -> str:
        """Generate unique task ID"""
        return f"meta-{self._task_id}-{self.get_template_class().__name__}-{index}"

    def _generate_meta_event_name(self) -> str:
        """Generate unique meta-event name"""
        return f"{self.name.lower()}-{self.get_template_class().__name__}"

    def _should_allow_partial_success(self) -> bool:
        """Check if partial success is allowed"""
        if self.options:
            return self.options.extras.get("partial_success", False)
        return False

    def _handle_empty_input(self, input_data: typing.Any) -> typing.Any:
        """Handle empty input - can be overridden by subclasses"""
        return []

    def _get_error_summary(self, results: typing.List[EventResult]) -> typing.Dict:
        """Create error summary for failed execution"""
        errors = [r for r in results if r.error]
        return {
            "total_tasks": len(results),
            "successful_tasks": len(results) - len(errors),
            "failed_tasks": len(errors),
            "errors": [
                {"task_id": r.task_id, "error": str(r.content) if r.error else None}
                for r in errors
            ],
        }

    def _create_pipeline_tasks(
        self, task_definitions: typing.List[TaskDefinition]
    ) -> typing.List[PipelineTask]:
        """
        Create PipelineTask objects from task definitions

        Args:
            task_definitions: List of task definitions

        Returns:
            List of PipelineTask objects
        """
        tasks = []
        for task_def in task_definitions:
            task = PipelineTask(event=task_def.template_class)
            setattr(task, "_id", task_def.task_id)
            setattr(task, "sequence_number", task_def.order)

            if task_def.options:
                options = self.options or Options()
                options.merge_with(task_def.options)
                task.options = options
            else:
                task.options = self.options

            tasks.append(task)

        return tasks
