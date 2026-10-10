import logging
from typing import Dict, Any, Optional, Union, List, Hashable, Tuple

from .snapshot import InitArgsTemplate, CallArgsTemplate
from volnux.parser.options import StopCondition
from .event_result_serializer import ExecResultSerializer
from volnux.execution.utils import is_json_serializable
from volnux.execution.constants import EVENT_EXCLUDED_ATTRS


logger = logging.getLogger(__name__)


class StateSerializer:
    """
    Handles the serialization and deserialization of event state data.
    """

    @classmethod
    def serialize_init_args(cls, init_arg_dict: Dict[str, Any]) -> InitArgsTemplate:
        from volnux.execution.rehydrator.engine.serializer import (
            StateSerializer as EngineSerializer,
        )

        execution_context = init_arg_dict.get("execution_context")
        options = init_arg_dict.get("options")
        results = init_arg_dict.get("previous_result", [])
        keyword_args = init_arg_dict.get("kwargs", {})

        return {
            "execution_context_id": (
                execution_context.state_id if execution_context else None
            ),
            "task_id": init_arg_dict.get("task_id"),
            "stop_condition": cls._serialize_stop_condition(
                init_arg_dict.get("stop_condition")
            ),
            "run_bypass_event_checks": init_arg_dict.get(
                "run_bypass_event_checks", False
            ),
            "options": options.as_dict() if options else None,
            "sequence_number": init_arg_dict.get("sequence_number"),
            "kwargs": cls._build_keyword_args(keyword_args),
            "previous_result": [
                EngineSerializer.serialize_result(result) for result in results
            ],
        }

    @classmethod
    def serialize_call_args(cls, call_arg_dict: Dict[str, Any]) -> CallArgsTemplate:
        """Serialize call arguments at the event boundary.

        At this point, args and kwargs have already been through the
        pipeline's serialization via ExecResultSerializer or equivalent.
        We just capture the already-serialized form.
        """

        args = call_arg_dict.get("args", [])
        kwargs = call_arg_dict.get("kwargs", {})

        return {
            "args": [cls.serialize_exec_result(arg) for arg in args],
            "kwargs": {
                key: cls.serialize_exec_result(value) for key, value in kwargs.items()
            },
        }

    @staticmethod
    def serialize_exec_result(result: Hashable):
        return ExecResultSerializer().serialize_exec_result(result)

    @classmethod
    def _serialize_stop_condition(
        cls, stop_condition: Union[StopCondition, List[StopCondition], None]
    ) -> List[str]:
        """
        Serializes a given stop condition or a list of stop conditions into a list of strings suitable
        for internal processing. This method accepts either a single StopCondition object, a list of
        StopCondition objects, or None. The serialized output is a flattened list where each element
        represents the string value of the stop condition.

        :param stop_condition: The stop condition or a list of stop conditions to be serialized.
            Can accept a single StopCondition, a list/tuple of StopCondition, or None.
        :type stop_condition: Union[StopCondition, List[StopCondition], None]

        :return: A list of string values representing the serialized stop conditions.
        :rtype: List[str]
        """
        if stop_condition is None:
            return [StopCondition.NEVER.value]
        if isinstance(stop_condition, (list, tuple)):
            conditions = []
            for cond in stop_condition:
                conditions.extend(cls._serialize_stop_condition(cond))

            return conditions
        else:
            return [stop_condition.value]

    @staticmethod
    def _build_keyword_args(kwargs: Dict[str, Any]) -> Dict[str, Any]:
        data = {}
        for k, v in kwargs.items():
            if isinstance(v, dict):
                data[k] = StateSerializer._build_keyword_args(v)
            elif isinstance(v, list):
                data[k] = [
                    (
                        StateSerializer._build_keyword_args(item)
                        if isinstance(item, dict)
                        else item
                    )
                    for item in v
                ]
            elif isinstance(v, tuple):
                data[k] = tuple(
                    (
                        StateSerializer._build_keyword_args(item)
                        if isinstance(item, dict)
                        else item
                    )
                    for item in v
                )
            else:
                data[k] = v
        return data

    @classmethod
    def serialize_attribs(cls, event_instance) -> dict:
        """Capture user-bound serializable attributes from event.__dict__.

        Excludes:
            1. Framework internal attributes (FRAMEWORK_EXCLUDED_ATTRS)
            2. Keys already tracked in init_args, call_args, or external_resources
            3. Non-JSON-serializable values (warns and skips)
        """
        attribs = {}

        # Gather already-tracked keys from dedicated snapshot fields
        init_arg_keys = (
            set(event_instance.init_args)
            if getattr(event_instance, "init_args", None)
            else set()
        )

        call_args = getattr(event_instance, "call_args", {}) or {}
        call_arg_keys = set(call_args) if isinstance(call_args, dict) else set()

        external_resource_keys = (
            set(event_instance.external_resources)
            if getattr(event_instance, "external_resources", None)
            else set()
        )

        tracked_keys = init_arg_keys | call_arg_keys | external_resource_keys

        for key, value in vars(event_instance).items():
            if key in EVENT_EXCLUDED_ATTRS:
                continue
            if key in tracked_keys:
                continue
            if not is_json_serializable(value):
                logger.warning(
                    "Cannot checkpoint attribute '%s' on '%s': "
                    "value is not JSON-serializable. "
                    "Use acquire_resource() for non-serializable state.",
                    key,
                    getattr(
                        event_instance, "class_path", type(event_instance).__name__
                    ),
                )
                continue
            attribs[key] = value

        return attribs
