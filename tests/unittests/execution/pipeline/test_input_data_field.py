"""InputDataField: declaring, validating, defaulting and batching a pipeline input."""

import typing
import warnings

import pytest

from volnux import default_batch_processors as batch_defaults
from volnux.constants import EMPTY, UNKNOWN
from volnux.exceptions import ImproperlyConfigured
from volnux.execution.pipeline.fields import InputDataField
from volnux.execution.pipeline.fields.field import (
    _MAX_VALIDATION_SAMPLE,
    _extract_element_types,
    _extract_isinstance_types,
)

from .conftest import unique


@pytest.fixture
def pipe(event_class, pipeline_class):
    """``pipe(**fields)`` -> an instance of a pipeline declaring those fields."""

    def build(**fields):
        cls = pipeline_class(event_class().__name__, **fields)
        return cls

    return build


# --- declaring ------------------------------------------------------------------------


def test_defaults_of_a_bare_field():
    f = InputDataField()
    assert f.name is None and f.required is False and f.default is EMPTY
    assert f.default_factory is None and f.data_type == (UNKNOWN,)
    assert f._isinstance_types is None and f._element_types is None
    assert (
        f.batch_processor is None and not f.has_batch_operation and not f.is_batch_mode
    )


def test_default_and_default_factory_are_exclusive():
    with pytest.raises(ValueError, match="both"):
        InputDataField(default=1, default_factory=list)


@pytest.mark.parametrize(
    "given, stored", [(int, (int,)), ((int, str), (int, str)), ([int, str], (int, str))]
)
def test_data_type_is_stored_as_a_tuple(given, stored):
    assert InputDataField(data_type=given).data_type == stored


@pytest.mark.parametrize("bad", ["str", 5, [int, "x"]])
def test_data_types_must_be_types(bad):
    with pytest.raises(TypeError, match="not a valid type"):
        InputDataField(data_type=bad)


def test_parameterised_generics_are_valid_types():
    f = InputDataField(data_type=typing.Dict[str, int])
    assert f._isinstance_types == (dict,)


def test_a_list_or_tuple_field_gets_the_list_batch_processor():
    for dtype in (list, tuple, typing.List[int], typing.Tuple[int, ...]):
        f = InputDataField(data_type=dtype)
        assert f.batch_processor is batch_defaults.list_batch_processor, dtype
        assert f.has_batch_operation and f.is_batch_mode


@pytest.mark.parametrize("dtype", [int, str, dict, set, UNKNOWN])
def test_other_types_get_no_batch_processor(dtype):
    assert InputDataField(data_type=dtype).batch_processor is None


def test_a_given_batch_processor_wins_and_is_validated():
    def custom(values, batch_size=1):
        yield from values

    assert (
        InputDataField(data_type=list, batch_processor=custom).batch_processor is custom
    )
    with pytest.raises(ImproperlyConfigured):
        InputDataField(batch_processor=lambda: None)


def test_batch_size_and_help_text_are_kept():
    f = InputDataField(batch_size=7, help_text="how many")
    assert (f.batch_size, f.help_text) == (7, "how many")
    assert InputDataField().batch_size == batch_defaults.DEFAULT_BATCH_SIZE


def test_batch_mode_switches_chain():
    f = InputDataField()
    assert f.enable_batch_mode() is f and f.is_batch_mode and not f.has_batch_operation
    assert f.disable_batch_mode() is f and not f.is_batch_mode


def test_extracting_types_from_declarations():
    assert _extract_isinstance_types((UNKNOWN,)) is None
    assert _extract_isinstance_types((int, typing.List[str], UNKNOWN)) == (int, list)
    assert _extract_element_types((typing.List[str],)) == (str,)
    assert _extract_element_types((typing.Dict[str, int],)) == (str, int)
    assert _extract_element_types((list, int)) is None


# --- naming ----------------------------------------------------------------------------------


def test_the_attribute_name_becomes_the_field_name(pipe):
    cls = pipe(first=InputDataField(), second=InputDataField())
    assert dict(cls.get_fields())["first"].name == "first"


def test_an_explicit_name_that_does_not_match_warns_and_is_replaced(
    event_class, pipeline_class
):
    with pytest.warns(UserWarning, match="declared with name='other'"):
        cls = pipeline_class(event_class().__name__, mine=InputDataField(name="other"))
    assert dict(cls.get_fields())["mine"].name == "mine"


def test_a_matching_explicit_name_does_not_warn(event_class, pipeline_class):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        pipeline_class(event_class().__name__, same=InputDataField(name="same"))


def test_the_field_on_the_class_is_the_descriptor(pipe):
    cls = pipe(x=InputDataField())
    assert isinstance(cls.x, InputDataField)


# --- reading ---------------------------------------------------------------------------------------


def test_an_unset_optional_field_reads_none(pipe):
    assert pipe(x=InputDataField())().x is None


def test_an_unset_field_reads_its_default(pipe):
    assert pipe(x=InputDataField(default=5))().x == 5


def test_a_default_factory_gives_a_fresh_value_on_each_read(pipe):
    p = pipe(x=InputDataField(default_factory=list))()
    first, second = p.x, p.x
    first.append(1)
    assert second == [] and p.x == []


def test_an_unset_required_field_cannot_be_read(pipe):
    p = pipe(x=InputDataField(required=False))()  # construction needs no value
    f = InputDataField(required=True)
    f.name = "x"
    with pytest.raises(AttributeError, match="has not been set"):
        f.__get__(p, type(p))


def test_none_set_explicitly_is_distinct_from_never_set(pipe):
    p = pipe(x=InputDataField(default=5))(x=None)
    # setting None applies the default policy
    assert p.x == 5


# --- writing and validating ----------------------------------------------------------------------------


def test_a_value_of_the_right_type_is_stored(pipe):
    p = pipe(n=InputDataField(data_type=int))(n=3)
    assert p.n == 3


def test_a_value_of_the_wrong_type_is_rejected_with_a_helpful_message(pipe):
    cls = pipe(n=InputDataField(data_type=int))
    with pytest.raises(TypeError, match=r"Field 'n' expects int, got str"):
        cls(n="three")


def test_several_accepted_types_are_listed(pipe):
    cls = pipe(v=InputDataField(data_type=(int, float)))
    cls(v=1), cls(v=1.5)
    with pytest.raises(TypeError, match=r"\(int \| float\)"):
        cls(v="x")


def test_an_untyped_field_accepts_anything(pipe):
    cls = pipe(v=InputDataField())
    for value in (1, "a", [1], {"a": 1}, object()):
        assert cls(v=value).v is value


def test_a_callable_is_refused_as_a_forgotten_call(pipe):
    cls = pipe(v=InputDataField())
    with pytest.raises(TypeError, match="Did you forget to call"):
        cls(v=lambda: 1)


def test_a_class_is_not_mistaken_for_a_callable(pipe):
    cls = pipe(v=InputDataField())
    assert cls(v=int).v is int


def test_elements_of_a_parameterised_collection_are_checked(pipe):
    cls = pipe(ids=InputDataField(data_type=typing.List[int]))
    cls(ids=[1, 2, 3])
    with pytest.raises(
        TypeError, match=r"element at index 1 expects List|element at index 1"
    ):
        cls(ids=[1, "2"])


def test_only_a_sample_of_a_big_collection_is_checked(pipe):
    cls = pipe(ids=InputDataField(data_type=typing.List[int]))
    big = [1] * _MAX_VALIDATION_SAMPLE + ["bad"]  # the bad one is past the sample

    assert cls(ids=big).ids is big


def test_an_unparameterised_list_does_not_check_elements(pipe):
    cls = pipe(items=InputDataField(data_type=list))
    assert cls(items=[1, "a", None]).items == [1, "a", None]


def test_explicit_batch_mode_wants_a_collection_of_the_data_type(
    event_class, pipeline_class
):
    field = InputDataField(data_type=int).enable_batch_mode()
    cls = pipeline_class(event_class().__name__, n=field)

    assert cls(n=[1, 2]).n == [1, 2]
    with pytest.raises(TypeError, match="batch mode and expects a collection"):
        cls(n=5)
    with pytest.raises(TypeError, match="element at index 1"):
        cls(n=[1, "x"])


def test_none_resolves_to_the_default_or_factory_or_stays_none(pipe):
    assert pipe(v=InputDataField())(v=None).v is None
    assert pipe(v=InputDataField(default=9))(v=None).v == 9
    assert pipe(v=InputDataField(default_factory=lambda: ["f"]))(v=None).v == ["f"]


def test_none_for_a_required_field_without_a_default_is_an_error(pipe):
    cls = pipe(v=InputDataField(required=True))
    with pytest.raises(ValueError, match="required but received None"):
        cls(v=None)


def test_a_required_field_with_a_default_accepts_none(pipe):
    assert pipe(v=InputDataField(required=True, default=3))(v=None).v == 3


def test_values_are_per_instance(pipe):
    cls = pipe(v=InputDataField())
    a, b = cls(v=1), cls(v=2)
    assert (a.v, b.v) == (1, 2)


def test_a_value_is_written_to_the_pipeline_cache(pipe):
    cls = pipe(v=InputDataField())
    p = cls(v="cached")

    assert dict(cls.get_pipeline_state().cache(p))["v"] == "cached"


def test_a_failing_cache_write_does_not_lose_the_value(pipe, monkeypatch):
    cls = pipe(v=InputDataField())
    monkeypatch.setattr(
        InputDataField,
        "set_field_cache_value",
        lambda self, i, v: (_ for _ in ()).throw(RuntimeError("cache down")),
    )

    assert cls(v="kept").v == "kept"


# --- deleting ----------------------------------------------------------------------------------------------


def test_deleting_an_optional_field_clears_it_and_its_cache_entry(pipe):
    cls = pipe(v=InputDataField(default="d"))
    p = cls(v="x")

    del p.v

    assert p.v == "d"
    assert "v" not in dict(cls.get_pipeline_state().cache(p))


def test_a_required_field_cannot_be_deleted(pipe):
    cls = pipe(v=InputDataField(required=True))
    p = cls(v="x")
    with pytest.raises(ValueError, match="Cannot delete required field"):
        del p.v


def test_deleting_a_field_that_was_never_set_is_harmless(pipe):
    p = pipe(v=InputDataField())()
    del p.v


# --- representation -------------------------------------------------------------------------------------------


def test_repr_shows_the_name_requirement_default_and_batching():
    f = InputDataField(data_type=list, required=True, default=[1], batch_size=4)
    f.name = "rows"
    text = repr(f)
    assert "name='rows'" in text and "required=True" in text
    assert "has_default=True" in text and "batch_size=4" in text
    assert "batch_size" not in repr(InputDataField())
