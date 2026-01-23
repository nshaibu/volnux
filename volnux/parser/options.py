# from __future__ import annotations

import dataclasses
import typing
from enum import Enum

try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, Enum):
        """An Enum class that inherits from str."""

        pass


from pydantic_mini import Attrib, BaseModel, MiniAnnotated
from pydantic_mini.exceptions import ValidationError

from .executor_config import ExecutorInitializerConfig


class StopCondition(Enum):
    """Defines when task execution should stop."""

    NEVER = "never"
    ON_ERROR = "on_error"
    ON_SUCCESS = "on_success"
    ON_EXCEPTION = "on_exception"
    ON_ANY = "on_any"


class ResultEvaluationStrategy(StrEnum):
    """Defines strategies used to evaluate task results."""

    ALL_MUST_SUCCEED = "ALL_MUST_SUCCEED"
    ANY_MUST_SUCCEED = "ANY_MUST_SUCCEED"
    MAJORITY_MUST_SUCCEED = "MAJORITY_MUST_SUCCEED"
    NO_FAILURES_ALLOWED = "NO_FAILURES_ALLOWED"


def resolve_str_to_enum(
    enum_klass: typing.Type[Enum], value: str, use_lower_case: bool = False
) -> typing.Union[Enum, str]:
    """Resolve enum value to enum class"""
    if not isinstance(value, str):
        return value
    attr_name = value.lower() if use_lower_case else value.upper()
    enum_attr = getattr(enum_klass, attr_name, None)
    if enum_attr is None:
        raise ValidationError(
            f"Invalid enum value {value} for {enum_klass.__name__}", code="invalid_enum"
        )
    return enum_attr


class Options(BaseModel):
    """
    Task execution configuration options that can be passed to a task or
    task groups in pointy scripts, e.g., A[retry_attempts=3], {A->B}[retry_attempts=3].
    """

    # Core execution options with validation
    retry_attempts: MiniAnnotated[int, Attrib(default=0, ge=0)]
    executor: MiniAnnotated[typing.Optional[str], Attrib(default=None)]

    # Configuration dictionaries
    executor_config: MiniAnnotated[
        typing.Union[ExecutorInitializerConfig, dict],
        Attrib(
            default_factory=lambda: ExecutorInitializerConfig(),
            pre_formatter=lambda val: (
                ExecutorInitializerConfig.from_dict(val)
                if isinstance(val, dict)
                else val
            ),
        ),
    ]
    extras: MiniAnnotated[dict, Attrib(default_factory=dict)]

    # Execution state and control
    result_evaluation_strategy: MiniAnnotated[
        ResultEvaluationStrategy,
        Attrib(
            default=ResultEvaluationStrategy.ALL_MUST_SUCCEED,
            pre_formatter=lambda val: resolve_str_to_enum(
                ResultEvaluationStrategy, val, use_lower_case=False
            ),
        ),
    ]
    stop_condition: MiniAnnotated[
        typing.Union[StopCondition, None],
        Attrib(
            default=None,
            pre_formatter=lambda val: val
            and resolve_str_to_enum(StopCondition, val, use_lower_case=False)
            or None,
        ),
    ]
    bypass_event_checks: typing.Optional[bool]

    class Config:
        disable_typecheck = False
        disable_all_validation = False

    @classmethod
    def from_dict(cls, options_dict: typing.Dict[str, typing.Any]) -> "Options":
        """
        Create Options instance from dictionary, placing unknown fields in extras.
        Args:
            options_dict: Dictionary containing option values
        Returns:
            Options instance with known fields populated and unknown fields in extras
        """
        known_fields = {field.name for field in dataclasses.fields(cls)}

        option = {}
        for field_name, value in options_dict.items():
            if field_name in known_fields:
                option[field_name] = value
            else:
                # Place unknown fields in extras
                if "extras" not in option:
                    option["extras"] = {}
                option["extras"][field_name] = value

        return cls.loads(option, _format="dict")

    def has_retry_policy(self) -> bool:
        """Check if retry policy is configured."""
        return self.retry_attempts is not None and self.retry_attempts > 0

    def should_stop_on(self, condition: str) -> bool:
        """
        Check if execution should stop on given condition.
        Args:
            condition: Condition to check ("error", "success", "exception")
        Returns:
            True if should stop on this condition
        """
        if self.stop_condition is None:
            return False

        condition_map = {
            "error": StopCondition.ON_ERROR,
            "success": StopCondition.ON_SUCCESS,
            "exception": StopCondition.ON_EXCEPTION,
        }

        target_condition = condition_map.get(condition.lower())
        if target_condition is None:
            return False

        return self.stop_condition in [target_condition, StopCondition.ON_ANY]

    def merge_with(self, other: typing.Union["Options", dict]) -> "Options":
        """
        Merge this Options with another, with other taking precedence.
        Args:
            other: Other Options instance to merge with
        Returns:
            New Options instance with merged values
        """
        # Convert both to dicts
        self_dict = self.dump(_format="dict")
        other_dict = other.dump(_format="dict") if isinstance(other, Options) else other

        # Merge extras separately to avoid overwriting
        merged_extras = {**self_dict.get("extras", {}), **other_dict.get("extras", {})}

        # Merge main options (other takes precedence for non-None values)
        merged = self_dict.copy()
        for key, value in other_dict.items():
            if key == "extras":
                continue
            if value is not None:
                merged[key] = value

        merged["extras"] = merged_extras
        return self.from_dict(merged)

    def is_configured(self, field_name: str) -> bool:
        """
        Check if a specific option field is configured (not None).
        Args:
            field_name: Name of the field to check
        Returns:
            True if the field is configured, False otherwise
        """
        return getattr(self, field_name, None) is not None

    def to_dict(self):
        return dataclasses.asdict(self)
