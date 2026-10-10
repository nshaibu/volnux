import dataclasses
import typing
from enum import Enum

try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, Enum):
        """An Enum class that inherits from str."""

        pass


from formax import (
    Attrib,
    BaseModel,
    MiniAnnotated,
    ValidationError,
    preformat,
    postformat,
    InitStrategy,
)

from .executor_config import ExecutorInitializerConfig
from volnux.result_evaluators import ResultEvaluationStrategies


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
            f"Invalid enum value {value} for {enum_klass.__name__}",
            params={"code": "invalid_enum"},
        )
    return enum_attr


class Options(BaseModel):
    """
    Task execution configuration options that can be passed to a task or
    task groups in pointy scripts, e.g., A[retry_attempts=3], {A->B}[retry_attempts=3].
    """

    # Core execution options with validation
    retry_attempts: MiniAnnotated[int, Attrib(default=0, ge=0)]
    executor: typing.Optional[str]

    # Configuration dictionaries
    executor_config: ExecutorInitializerConfig
    extras: MiniAnnotated[dict, Attrib(default_factory=dict)]

    # Execution state and control
    stop_condition: typing.Optional[StopCondition]
    bypass_event_checks: bool = False
    result_evaluation_strategy: ResultEvaluationStrategy = (
        ResultEvaluationStrategy.ALL_MUST_SUCCEED
    )

    class Config:
        init_strategy = InitStrategy.DATACLASS

    @preformat(["executor_config"])
    def preformat_executor_config(self, val: typing.Any) -> ExecutorInitializerConfig:
        if isinstance(val, dict):
            return ExecutorInitializerConfig.from_dict(val)
        return val

    @preformat(["result_evaluation_strategy"])
    def preformat_result_evaluation_strategy(self, val: typing.Any) -> typing.Any:
        return (
            resolve_str_to_enum(ResultEvaluationStrategy, val, use_lower_case=False),
        )

    @preformat(["stop_condition"])
    def preformat_stop_condition(self, val: typing.Any) -> typing.Any:
        return (
            val
            and resolve_str_to_enum(StopCondition, val, use_lower_case=False)
            or None
        )

    @postformat(["result_evaluation_strategy"])
    def postformat_result_evaluation_strategy(
        self, value: ResultEvaluationStrategy
    ) -> ResultEvaluationStrategies:
        return getattr(ResultEvaluationStrategies, value.value, None)

    @classmethod
    def from_dict(cls, options_dict: typing.Dict[str, typing.Any]) -> "Options":
        """
        Create Options instance from the dictionary, placing unknown fields in extras.
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

        return cls.loads(option, _format="dict")  # type: ignore

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

    def as_dict(self):
        return dataclasses.asdict(self)
