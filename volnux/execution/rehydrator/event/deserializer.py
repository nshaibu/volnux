import logging
from typing import Any, Dict, List

from volnux.import_utils import import_string as import_class
from .event_result_serializer import ExecResultSerializer
from .snapshot import InitArgsTemplate, CallArgsTemplate, ResourceState
from volnux.parser.options import Options, StopCondition


logger = logging.getLogger(__name__)


class StateDeserializer:
    """Handles the deserialization of event state data from checkpoints.

    Mirrors StateSerializer. Each serialize_* method on the serializer
    has a corresponding deserialize_* method here.
    """

    @classmethod
    def deserialize_init_args(cls, init_args: InitArgsTemplate) -> Dict[str, Any]:
        """Reconstruct the init kwargs dict from a checkpoint's init_args.

        This is the inverse of StateSerializer.serialize_init_args.
        """
        from volnux.execution.rehydrator.engine.serializer import (
            StateSerializer as EngineSerializer,
        )
        from volnux.execution.context import ExecutionContext

        kwargs = {}

        # Restore execution context
        execution_context_id = init_args.get("execution_context_id")
        if execution_context_id:
            kwargs["execution_context"] = ExecutionContext.get(execution_context_id)

        # Restore task identity
        task_id = init_args.get("task_id")
        if task_id:
            kwargs["task_id"] = task_id

        # Restore stop conditions
        stop_condition = cls._deserialize_stop_condition(
            init_args.get("stop_condition", [])
        )
        if stop_condition:
            kwargs["stop_condition"] = stop_condition

        # Restore bypass flag
        kwargs["run_bypass_event_checks"] = init_args.get(
            "run_bypass_event_checks", False
        )

        # Restore options
        options_data = init_args.get("options")
        if options_data:
            kwargs["options"] = Options.from_dict(options_data)

        # Restore sequence number
        sequence_number = init_args.get("sequence_number")
        if sequence_number is not None:
            kwargs["sequence_number"] = sequence_number

        # Restore keyword arguments
        kwargs["kwargs"] = cls._restore_keyword_args(init_args.get("kwargs", {}))

        # Restore previous results
        previous_results = init_args.get("previous_result", [])
        if previous_results:
            kwargs["previous_result"] = [
                EngineSerializer.deserialize_result(result)
                for result in previous_results
            ]

        return kwargs

    @classmethod
    def deserialize_call_args(cls, call_args: CallArgsTemplate) -> Dict[str, Any]:
        """Reconstruct the call args dict from a checkpoint's call_args.

        This is the inverse of StateSerializer.serialize_call_args.
        """
        from volnux.execution.rehydrator.engine.serializer import (
            StateSerializer as EngineSerializer,
        )

        return {
            "args": [
                EngineSerializer.deserialize_result(arg)
                for arg in call_args.get("args", [])
            ],
            "kwargs": {
                key: EngineSerializer.deserialize_result(value)
                for key, value in call_args.get("kwargs", {}).items()
            },
        }

    @staticmethod
    def deserialize_exec_result(result: Any) -> Any:
        """Reconstruct the exec_result from its serialized form.

        Inverse of StateSerializer.serialize_exec_result / _ExecResultSerializer.
        """
        return ExecResultSerializer().deserialize_exec_result(result)

    @classmethod
    def deserialize_external_resources(
        cls, external_resources: Dict[str, ResourceState]
    ) -> Dict[str, Any]:
        """Reconstruct registered external resources from checkpoint state.

        Each resource is restored by importing its provider class
        and calling provider.restore(data).
        """
        restored = {}
        for resource_name, resource_state in external_resources.items():
            provider_path = resource_state.get("provider_path")
            resource_data = resource_state.get("data", {})

            if not provider_path:
                logger.warning(
                    "Resource '%s' has no provider_path — skipping restoration",
                    resource_name,
                )
                continue

            try:
                provider_class = import_class(provider_path)
                restored[resource_name] = provider_class.restore(resource_data)
            except Exception as e:
                logger.error(
                    "Failed to restore resource '%s' via provider '%s': %s",
                    resource_name,
                    provider_path,
                    e,
                )
                raise

        return restored

    @classmethod
    def _deserialize_stop_condition(cls, conditions: List[str]) -> List[StopCondition]:
        """Reconstruct StopCondition objects from their string values."""
        result = []
        for condition_str in conditions:
            try:
                result.append(StopCondition(condition_str))
            except ValueError:
                logger.warning("Unknown stop condition '%s' — skipping", condition_str)
        return result if result else [StopCondition.NEVER]

    @classmethod
    def _restore_keyword_args(cls, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively restore keyword arguments.

        Inverse of StateSerializer._build_keyword_args.
        No special transformation is needed since _build_keyword_args
        preserves the structure — but subclasses can override for
        custom deserialization logic.
        """
        return kwargs
