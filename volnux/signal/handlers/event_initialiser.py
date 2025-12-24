import typing
from typing import Callable, List, Dict, Any

from volnux import EventBase
from volnux.decorators import listener
from volnux.signal.signals import event_init

ValidatorFunc = Callable[[Any, str, EventBase], None]


class ExtraEventInitKwargs(typing.TypedDict, total=False):
    type: typing.Type[typing.Any]
    required: bool
    default: typing.Any
    description: typing.Optional[str]
    default_factory: typing.Callable[[], typing.Any]
    validators: typing.List[ValidatorFunc]


@listener(event_init, sender=EventBase)
def inject_event_initialisation_extra_params(
    event: EventBase, **init_kwargs: Dict[str, Any]
):
    """
    A generic listener connected to event_init that reads the EXTRA_INIT_PARAMS_SCHEMA
    (defined using the ExtraEventInitKwargs TypedDict), handles required parameters,
    default values, default factories, and custom validators, then injects
    the parameters as instance attributes.
    """
    event_class = type(event)

    if not hasattr(event_class, "INIT_PARAMS_SCHEMA"):
        return

    schema: Dict[str, ExtraEventInitKwargs] = event_class.EXTRA_INIT_PARAMS_SCHEMA

    for param_name, config in schema.items():
        is_required = config.get("required", False)
        default_value = config.get("default")
        default_factory = config.get("default_factory")
        validators: List[ValidatorFunc] = config.get("validators", [])

        value_to_set = None

        if param_name in init_kwargs:
            # Value was passed during instantiation
            value_to_set = init_kwargs[param_name]

        elif is_required:
            raise TypeError(
                f"Event '{event_class.__name__}' requires the keyword argument "
                f"'{param_name}' for initialization."
            )

        elif default_factory is not None:
            try:
                value_to_set = default_factory()
            except Exception as e:
                raise RuntimeError(
                    f"Error calling default_factory for parameter '{param_name}': {e}"
                )

        elif default_value is not None:
            value_to_set = default_value

        if value_to_set is None and not is_required:
            continue

        if value_to_set is not None:
            for validator in validators:
                try:
                    # Validator function signature: validator(value, param_name, event_instance)
                    validator(value_to_set, param_name, event)
                except Exception as e:
                    # Re-raise the exception with context
                    raise type(e)(
                        f"Validation failed for parameter '{param_name}' in "
                        f"event '{event_class.__name__}': {e}"
                    )

            setattr(event, param_name, value_to_set)
