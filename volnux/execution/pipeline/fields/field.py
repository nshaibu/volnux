import itertools
import typing
import warnings

from formax.typing import is_type

from volnux import default_batch_processors as batch_defaults
from volnux.constants import EMPTY, UNKNOWN
from volnux.exceptions import ImproperlyConfigured
from volnux.typing import BatchProcessType
from volnux.utils import validate_batch_processor

if typing.TYPE_CHECKING:
    from volnux.execution.pipeline import Pipeline

T = typing.TypeVar("T")

# Sentinel for "field was never set" — distinct from None, which is a valid value.
# Using EMPTY from constants creates an import cycle in some configurations,
# so we define a local sentinel here.
_NOT_SET = object()

# Maximum number of batch elements validated per __set__ call.
# Validates a representative sample without O(n) overhead for large batches.
_MAX_VALIDATION_SAMPLE = 100

__all__ = ["InputDataField"]


def _extract_isinstance_types(
    data_types: tuple,
) -> typing.Optional[tuple]:
    """
    Extract concrete types safe for use in isinstance() from data_types.

    Parameterized generics (List[str], Dict[str, int]) cannot be used
    directly in isinstance(). This function extracts their origin types
    (list, dict) instead.

    Args:
        data_types: Tuple of types from InputDataField.data_type.

    Returns:
        Tuple of concrete types for isinstance(), or None if no
        concrete types could be extracted (all were UNKNOWN or
        un-introspectable).
    """
    concrete = []
    for dtype in data_types:
        if dtype is UNKNOWN:
            continue
        origin = typing.get_origin(dtype)
        if origin is not None:
            # Parameterised generic — use origin (list, dict, tuple, etc.)
            concrete.append(origin)
        elif isinstance(dtype, type):
            concrete.append(dtype)
        # Non-type objects (typing aliases without origin) are skipped
    return tuple(concrete) if concrete else None


def _extract_element_types(
    data_types: tuple,
) -> typing.Optional[tuple]:
    """
    Extract element types from parameterized collection generics.

    For List[str] returns (str,). For list (unparameterised) returns None.
    Used for per-element validation in batch mode.

    Args:
        data_types: Tuple of types from InputDataField.data_type.

    Returns:
        Tuple of element types, or None if not determinable.
    """
    element_types = []
    for dtype in data_types:
        if dtype is UNKNOWN:
            continue
        args = typing.get_args(dtype)
        if args:
            for arg in args:
                if isinstance(arg, type):
                    element_types.append(arg)
    return tuple(element_types) if element_types else None


class CacheInstanceFieldMixin:
    def get_cache_key(self) -> str:
        raise NotImplementedError

    def set_field_cache_value(self, instance: "Pipeline", value: typing.Any) -> None:
        instance.get_pipeline_state().set_cache_for_pipeline_field(
            instance, self.get_cache_key(), value
        )

    def delete_field_cache_value(self, instance: "Pipeline") -> None:
        """Remove this field's cached value from PipelineState."""
        pipeline_state = instance.get_pipeline_state()
        cache = pipeline_state.__dict__.get("pipeline_cache", {})
        instance_key = pipeline_state.get_cache_key(instance)
        if instance_key in cache:
            cache[instance_key].pop(self.name, None)


class InputDataField(CacheInstanceFieldMixin):
    """
    Descriptor for Pipeline input fields with type validation, defaults,
    and batch processing support.
    """

    __slots__ = (
        "name",
        "data_type",
        "default",
        "default_factory",
        "required",
        "batch_processor",
        "batch_size",
        "help_text",
        "_batch_mode",
        # _isinstance_types: concrete types extracted for isinstance()
        "_isinstance_types",
        # _element_types: element types for per-element batch validation
        "_element_types",
    )

    def __init__(
        self,
        name: typing.Optional[str] = None,
        required: bool = False,
        help_text: typing.Optional[str] = None,
        data_type: typing.Union[
            typing.Type[typing.Any],
            typing.Tuple[typing.Type[typing.Any], ...],
            object,
        ] = UNKNOWN,
        default: typing.Any = EMPTY,
        default_factory: typing.Optional[typing.Callable[[], typing.Any]] = None,
        batch_processor: typing.Optional[BatchProcessType] = None,
        batch_size: int = batch_defaults.DEFAULT_BATCH_SIZE,
    ):
        """
        Initialise an InputDataField descriptor.

        Args:
            name: Field name (usually auto-set by ``__set_name__``).
            required:        If True, the field must be set before it is read.
            help_text:       Documentation string for this field.
            data_type:       Expected type(s) for individual elements. Accepts
                             concrete types (``int``, ``str``) and parameterised
                             generics (``List[str]``, ``Dict[str, int]``). For
                             isinstance checks the origin type is used.
            default:         Default value. Mutually exclusive with
                             ``default_factory``.
            default_factory: Callable returning a fresh default. Called on each
                             access when the field has no value. Mutually
                             exclusive with ``default``.
            batch_processor: Callable ``(collection, batch_size) → iterator``.
                             If None and data_type is a collection type, a
                             default list batch processor is inferred.
            batch_size:      Number of items per batch chunk.

        Raises:
            ValueError: If both ``default`` and ``default_factory`` are
                                provided.
            TypeError:          If ``data_type`` contains invalid types.
            ImproperlyConfigured: If ``batch_processor`` is not a valid
                                  batch processor.
        """
        if default is not EMPTY and default_factory is not None:
            raise ValueError(
                "Cannot specify both 'default' and 'default_factory'. "
                "Use one or the other."
            )

        self.name = name
        self.help_text = help_text
        self.required = required
        self.default = default
        self.default_factory = default_factory
        self.batch_size = batch_size
        self._batch_mode = False

        self.data_type: tuple = (
            tuple(data_type) if isinstance(data_type, (list, tuple)) else (data_type,)
        )

        self._validate_data_types()

        # Pre-compute isinstance-safe types to avoid repeated extraction
        self._isinstance_types: typing.Optional[tuple] = (
            _extract_isinstance_types(self.data_type)
            if UNKNOWN not in self.data_type
            else None
        )
        self._element_types: typing.Optional[tuple] = (
            _extract_element_types(self.data_type)
            if UNKNOWN not in self.data_type
            else None
        )

        self.batch_processor = None
        if batch_processor is None:
            batch_processor = self._infer_batch_processor()
        if batch_processor is not None:
            self._set_batch_processor(batch_processor)

    def _validate_data_types(self) -> None:
        """Validate declared data types at field creation time."""
        for dtype in self.data_type:
            if dtype is UNKNOWN:
                continue
            # Parameterised generics (List[str]) pass is_type by their origin
            origin = typing.get_origin(dtype)
            if origin is not None:
                continue  # valid parameterised generic
            if not is_type(dtype):
                raise TypeError(
                    f"data_type {dtype!r} is not a valid type. "
                    "Expected a class, type, or parameterised generic "
                    "(e.g. List[str], Dict[str, int])."
                )

    def _infer_batch_processor(self) -> typing.Optional[BatchProcessType]:
        """
        Infer a batch processor for list/tuple collection types.
        Skips UNKNOWN and parameterised generics (uses their origin).
        """
        for dtype in self.data_type:
            if dtype is UNKNOWN:
                continue
            # For parameterised generics, check the origin. formax's
            # is_collection() only matches parameterised generics, so compare
            # directly; list_batch_processor slices, so sequences only.
            check_type = typing.get_origin(dtype) or dtype
            if check_type in (list, tuple):
                return batch_defaults.list_batch_processor
        return None

    def _set_batch_processor(self, processor: BatchProcessType) -> None:
        """Validate and store a batch processor."""
        if not validate_batch_processor(processor):
            raise ImproperlyConfigured(
                "batch_processor must be a callable that accepts a collection "
                "and an optional batch size, and returns an iterator or generator."
            )
        self.batch_processor = processor

    def enable_batch_mode(self) -> "InputDataField":
        """Enable batch processing mode. Returns self for chaining."""
        self._batch_mode = True
        return self

    def disable_batch_mode(self) -> "InputDataField":
        """Disable batch processing mode. Returns self for chaining."""
        self._batch_mode = False
        return self

    def __set_name__(self, owner: object, name: str) -> None:
        """
        Called when the descriptor is assigned to a class attribute.

        Warns if a user-provided ``name`` does not match the attribute name —
        this causes ``instance.__dict__`` lookups to fail silently.
        """
        if self.name is None:
            self.name = name
        elif self.name != name:
            warnings.warn(
                f"InputDataField declared with name={self.name!r} but assigned "
                f"to attribute {name!r} on {owner!r}. The descriptor will use "
                f"name={name!r} to match the attribute. Remove the explicit "
                f"'name' argument to suppress this warning.",
                UserWarning,
                stacklevel=2,
            )
            self.name = name

    def __get__(
        self,
        instance: typing.Optional[object],
        owner: typing.Optional[typing.Type] = None,
    ) -> typing.Any:
        """
        Return the field value from the instance dict.

        Distinguishes "field was set to None" from "field was never set"
        using the _NOT_SET sentinel (the original code used None as the
        sentinel, making both cases indistinguishable).

        Raises:
            AttributeError: If the field is required and was never set.
        """
        if instance is None:
            return self

        value = instance.__dict__.get(self.name, _NOT_SET)

        if value is not _NOT_SET:
            return value  # explicitly set value, including None

        # Field isn't set — apply default policy
        if self.default is not EMPTY:
            return self.default

        if self.default_factory is not None:
            return self.default_factory()

        if self.required:
            raise AttributeError(
                f"Required field {self.name!r} has not been set. "
                f"Provide a value when constructing the Pipeline."
            )

        return None

    def __set__(self, instance: "Pipeline", value: typing.Any) -> None:
        """
        Set a field value with validation.

        Raises:
            TypeError: If a value is callable or fails type validation.
            ValueError: If required, the field receives None with no default.
        """

        if callable(value) and not isinstance(value, type):
            # Allow type objects (e.g. passing `int` as a data_type value would
            # be unusual but not the mistake we're guarding against).
            raise TypeError(
                f"Field {self.name!r} received a callable {value!r}. "
                "Did you forget to call the function? "
                f"Expected {self._format_types()}."
            )

        if self._isinstance_types is not None and value is not None:
            # Only explicit batch mode means "a collection of data_type". A
            # batch_processor alone just says how the value is chunked: a list
            # field holds a list, a file field holds a single path.
            if self._batch_mode:
                self._validate_batch_value(value)
            else:
                self._validate_single_value(value)

        if value is None:
            value = self._resolve_none_value()

        instance.__dict__[self.name] = value

        try:
            self.set_field_cache_value(instance, value)
        except Exception:
            pass  # Instance dict is already written — field is set correctly

    def __delete__(self, instance: "Pipeline") -> None:
        """
        Delete the field value from instance dict and PipelineState cache.

        Raises:
            ValueError: If the field is required.
        """
        if self.required:
            raise ValueError(
                f"Cannot delete required field {self.name!r}. "
                "Set it to a valid value instead."
            )
        instance.__dict__.pop(self.name, None)

        # Remove from the PipelineState cache so load_class_by_id() does not
        # restore the deleted value on the next reload.
        try:
            self.delete_field_cache_value(instance)
        except Exception:
            pass  # Non-fatal — instance dict already cleaned up

    def _validate_single_value(self, value: typing.Any) -> None:
        """
        Validate a single value against the isinstance-safe types.

        Uses ``_isinstance_types`` (with parameterised generic origins
        extracted) rather than ``data_type`` directly, avoiding the
        TypeError that parameterised generics raise in isinstance().
        """
        assert self._isinstance_types is not None
        if not isinstance(value, self._isinstance_types):
            raise TypeError(
                f"Field {self.name!r} expects {self._format_types()}, "
                f"got {type(value).__qualname__}."
            )

        # For parameterised sequences (List[str]) also sample the elements
        if self._element_types and isinstance(value, (list, tuple, set, frozenset)):
            sample = itertools.islice(enumerate(value), _MAX_VALIDATION_SAMPLE)
            for idx, item in sample:
                if not isinstance(item, self._element_types):
                    raise TypeError(
                        f"Field {self.name!r} element at index {idx} expects "
                        f"{self._format_types()}, got {type(item).__qualname__}."
                    )

    def _validate_batch_value(self, value: typing.Any) -> None:
        """
        Validate a batch value (collection of typed elements).

        Checks collection type first, then validates a sample of up to
        _MAX_VALIDATION_SAMPLE elements to avoid O(n) overhead for large
        batches. Element types are derived from parameterised generics
        (e.g., the str from List[str]).
        """

        if not isinstance(value, (list, tuple, set, frozenset)):
            raise TypeError(
                f"Field {self.name!r} is in batch mode and expects a collection "
                f"(list, tuple, set) of {self._format_types()}, "
                f"got {type(value).__qualname__}."
            )

        element_types = self._element_types or self._isinstance_types
        if element_types:
            sample = itertools.islice(enumerate(value), _MAX_VALIDATION_SAMPLE)
            for idx, item in sample:
                if not isinstance(item, element_types):
                    raise TypeError(
                        f"Field {self.name!r} element at index {idx} expects "
                        f"{self._format_types()}, got {type(item).__qualname__}."
                    )

    def _resolve_none_value(self) -> typing.Any:
        """
        Apply the required / default / factory policy when the value is None.

        Returns the resolved value (default or None for optional fields).

        Raises:
            ValueError: If a field is required with no default or factory.
        """
        if self.required and self.default is EMPTY and self.default_factory is None:
            raise ValueError(
                f"Field {self.name!r} is required but received None. "
                "Provide a non-None value."
            )
        if self.default is not EMPTY:
            return self.default
        if self.default_factory is not None:
            return self.default_factory()
        return None

    @property
    def has_batch_operation(self) -> bool:
        """True if a batch processor is configured for this field."""
        return self.batch_processor is not None

    @property
    def is_batch_mode(self) -> bool:
        """True if batch mode is active (explicit or via batch_processor)."""
        return self._batch_mode or self.batch_processor is not None

    def get_cache_key(self) -> str:
        """Return the field name as its cache key."""
        return typing.cast(str, self.name)

    def _format_types(self) -> str:
        """Format data_type for human-readable error messages."""
        names = []
        for t in self.data_type:
            if t is UNKNOWN:
                continue
            name = getattr(t, "__name__", None) or str(t)
            names.append(name)
        if not names:
            return "any"
        if len(names) == 1:
            return names[0]
        return f"({' | '.join(names)})"

    def __repr__(self) -> str:
        batch_info = f" batch_size={self.batch_size}" if self.is_batch_mode else ""
        has_default = self.default is not EMPTY or self.default_factory is not None
        return (
            f"<InputDataField"
            f" name={self.name!r}"
            f" required={self.required}"
            f" has_default={has_default}"
            f"{batch_info}>"
        )
