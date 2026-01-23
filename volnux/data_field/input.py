import os
import typing
from types import TracebackType

from pydantic_mini.typing import is_type, is_collection

from volnux import default_batch_processors as batch_defaults
from volnux.constants import EMPTY, UNKNOWN
from volnux.exceptions import ImproperlyConfigured
from volnux.typing import BatchProcessType
from volnux.utils import validate_batch_processor

T = typing.TypeVar("T")

if typing.TYPE_CHECKING:
    from volnux.pipeline import Pipeline


class CacheInstanceFieldMixin:
    def get_cache_key(self) -> str:
        raise NotImplementedError

    def set_field_cache_value(self, instance: "Pipeline", value: typing.Any) -> None:
        instance.get_pipeline_state().set_cache_for_pipeline_field(
            instance, self.get_cache_key(), value
        )


def extract_element_type(dtype: typing.Type) -> typing.Optional[typing.Type]:
    """
    Extract the element type from a collection type.

    For List[str] returns str, for list returns None.
    """
    args = typing.get_args(dtype)
    if args:
        return args[0]
    return None


class InputDataField(CacheInstanceFieldMixin):
    """
    A descriptor for managing input data fields with type validation,
    defaults, and batch processing capabilities.

    When batch processing is enabled (batch_processor is set), the field
    automatically expects a collection (list/tuple) of the specified data_type.

    Attributes:
        name: Field name (auto-set via __set_name__)
        data_type: Expected type(s) for individual elements
        default: Default value if not set
        required: Whether field must have a value
        batch_processor: Optional function for batch processing
        batch_size: Size of batches for processing
        help_text: Documentation for the field
        _batch_mode: Whether batch processing context is active
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
        "_frozen",
    )

    def __init__(
        self,
        name: typing.Optional[str] = None,
        required: bool = False,
        help_text: typing.Optional[str] = None,
        data_type: typing.Union[
            typing.Type[typing.Any], typing.Tuple[typing.Type[typing.Any], ...], object
        ] = UNKNOWN,
        default: typing.Any = EMPTY,
        default_factory: typing.Optional[typing.Callable[[], typing.Any]] = None,
        batch_processor: typing.Optional[BatchProcessType] = None,
        batch_size: int = batch_defaults.DEFAULT_BATCH_SIZE,
    ):
        """
        Initialize an InputDataField descriptor.

        Args:
            name: Field name (usually auto-set)
            required: Whether this field must be provided
            help_text: Documentation string
            data_type: Expected type(s) for individual elements (not the collection)
            default: Default value (use EMPTY for no default)
            default_factory: Callable returning default value
            batch_processor: Function for batch processing
            batch_size: Number of items per batch

        Raises:
            TypeError: If data_type contains invalid types
            ValueError: If both default and default_factory are provided
        """
        # Validate mutually exclusive defaults
        if default is not EMPTY and default_factory is not None:
            raise ValueError("Cannot specify both 'default' and 'default_factory'")

        self.name = name
        self.help_text = help_text
        self.required = required
        self.default = default
        self.default_factory = default_factory
        self.batch_size = batch_size
        self._batch_mode = False
        self._frozen = False

        # Normalize data_type to tuple (these are element types)
        self.data_type = (
            data_type if isinstance(data_type, (list, tuple)) else (data_type,)
        )

        # Validate data types
        self._validate_data_types()

        # Setup batch processing
        self.batch_processor = None
        if batch_processor is None:
            batch_processor = self._infer_batch_processor()

        if batch_processor:
            self._set_batch_processor(batch_processor)

    def _validate_data_types(self) -> None:
        """Validate that all data types are valid types."""
        for dtype in self.data_type:
            if dtype is not UNKNOWN and not is_type(dtype):
                raise TypeError(
                    f"Data type '{dtype}' is not a valid type. "
                    f"Expected a class or type object."
                )

    def _infer_batch_processor(self) -> typing.Optional[BatchProcessType]:
        """
        Automatically infer batch processor for list/tuple types.

        Returns:
            Batch processor if applicable, None otherwise
        """
        for dtype in self.data_type:
            status, _ = is_collection(dtype)
            if status:
                return batch_defaults.list_batch_processor
        return None

    def _set_batch_processor(self, processor: BatchProcessType) -> None:
        """
        Set and validate the batch processor.

        Args:
            processor: Batch processing function

        Raises:
            ImproperlyConfigured: If processor is invalid
        """
        if not validate_batch_processor(processor):
            raise ImproperlyConfigured(
                "Batch processor must be a callable that accepts a collection "
                "and an optional batch size, and returns an iterator or generator."
            )
        self.batch_processor = processor

    def enable_batch_mode(self) -> "InputDataField":
        """
        Enable batch processing mode for this field.

        In batch mode, the field expects a collection of data_type elements.

        Returns:
            Self for chaining
        """
        self._batch_mode = True
        return self

    def disable_batch_mode(self) -> "InputDataField":
        """
        Disable batch processing mode for this field.

        Returns:
            Self for chaining
        """
        self._batch_mode = False
        return self

    def __set_name__(self, owner: object, name: str) -> None:
        """
        Called when descriptor is assigned to a class attribute.

        Args:
            owner: The class owning this descriptor
            name: The attribute name assigned to this descriptor
        """
        if self.name is None:
            self.name = name

    def __get__(
        self,
        instance: typing.Optional[object],
        owner: typing.Optional[typing.Type] = None,
    ) -> typing.Any:
        """
        Get field value from instance.

        Args:
            instance: Object instance (None when accessed on class)
            owner: Owner class

        Returns:
            Field value, default value, or descriptor itself (if accessed on class)
        """
        if instance is None:
            return self

        value = instance.__dict__.get(self.name)
        if value is not None:
            return value

        if self.default is not EMPTY:
            return self.default
        elif self.default_factory is not None:
            return self.default_factory()

        return None

    def __set__(self, instance: "Pipeline", value: typing.Any) -> None:
        """
        Set field value on instance with validation.

        In batch mode, validates that value is a collection of data_type elements.
        Otherwise, validates that value is of data_type.

        Args:
            instance: Object instance
            value: Value to set

        Raises:
            TypeError: If value has incorrect type or is callable
            ValueError: If required field is not provided
        """
        # Validate type if specified
        if UNKNOWN not in self.data_type and value is not None:
            if self._batch_mode or self.batch_processor is not None:
                # In batch mode: expect collection of data_type elements
                self._validate_batch_value(value)
            else:
                # Normal mode: expect single data_type value
                self._validate_single_value(value)

        # Prevent callable assignment (common mistake)
        if callable(value):
            raise TypeError(
                f"Field '{self.name}' cannot be assigned a callable. "
                f"Did you forget to call the function?"
            )

        # Handle None with required/defaults
        if value is None:
            value = self._resolve_none_value()

        # Update cache and instance dict
        self.set_field_cache_value(instance, value)
        instance.__dict__[self.name] = value

    def _validate_single_value(self, value: typing.Any) -> None:
        """
        Validate a single value against data_type.

        Args:
            value: Value to validate

        Raises:
            TypeError: If a value type doesn't match
        """
        if not isinstance(value, self.data_type):
            expected = self._format_types()
            actual = type(value).__qualname__
            raise TypeError(
                f"Field '{self.name}' expects type {expected}, " f"got {actual}"
            )

    def _validate_batch_value(self, value: typing.Any) -> None:
        """
        Validate a batch value (collection of data_type elements).

        Args:
            value: Collection to validate

        Raises:
            TypeError: If value is not a collection or elements don't match data_type
        """
        # Check if value is a collection
        if not isinstance(value, (list, tuple, set, frozenset)):
            expected = self._format_types()
            raise TypeError(
                f"Field '{self.name}' is in batch mode and expects a collection "
                f"(list/tuple/set) of {expected}, got {type(value).__qualname__}"
            )

        # Validate each element in the collection
        if UNKNOWN not in self.data_type:
            for i, item in enumerate(value):
                if not isinstance(item, self.data_type):
                    expected = self._format_types()
                    actual = type(item).__qualname__
                    raise TypeError(
                        f"Field '{self.name}' element at index {i} expects type {expected}, "
                        f"got {actual}"
                    )

    def _resolve_none_value(self) -> typing.Any:
        """
        Resolve None value based on field configuration.

        Returns:
            Appropriate value for None input

        Raises:
            ValueError: If field is required with no default
        """
        if self.required and self.default is EMPTY and self.default_factory is None:
            raise ValueError(
                f"Field '{self.name}' is required but no value was provided"
            )

        if self.default is not EMPTY:
            return self.default

        if self.default_factory is not None:
            return self.default_factory()

        return None

    def _format_types(self) -> str:
        """Format data types for error messages."""
        type_names = [
            getattr(t, "__name__", str(t)) for t in self.data_type if t is not UNKNOWN
        ]

        if len(type_names) == 1:
            return type_names[0]
        return f"({' | '.join(type_names)})"

    def get_cache_key(self) -> str:
        """
        Get a cache key for this field.

        Returns:
            Field name as a cache key
        """
        return typing.cast(str, self.name)

    @property
    def has_batch_operation(self) -> bool:
        """Check if the field has batch processing configured."""
        return self.batch_processor is not None

    @property
    def is_batch_mode(self) -> bool:
        """Check if the field is in batch processing mode."""
        return self._batch_mode or self.batch_processor is not None

    def __repr__(self) -> str:
        """String representation for debugging."""
        batch_info = ""
        if self.is_batch_mode:
            batch_info = f" batch_size={self.batch_size}"

        return (
            f"<{self.__class__.__name__} "
            f"name={self.name!r} "
            f"required={self.required} "
            f"has_default={self.default is not EMPTY or self.default_factory is not None}"
            f"{batch_info}>"
        )

    def __delete__(self, instance: object) -> None:
        """
        Delete field value from instance.

        Args:
            instance: Object instance

        Raises:
            ValueError: If trying to delete a required field
        """
        if self.required:
            raise ValueError(f"Cannot delete required field '{self.name}'")

        instance.__dict__.pop(self.name, None)
