"""PipelineState and its per-instance cache, the pointy file lookup, and pickling."""

import pickle
from collections import OrderedDict

import pytest

from volnux.constants import EMPTY
from volnux.execution.pipeline.fields import InputDataField
from volnux.execution.pipeline.pipeline import (
    CacheFieldDescriptor,
    PipelineState,
    TreeExtraData,
    _directory_walk,
    _find_pointy_file_cached,
)
from volnux.parser.operator import PipeType


class Holder:
    cache = CacheFieldDescriptor()


# --- the descriptor ------------------------------------------------------------------


def test_the_descriptor_creates_an_empty_ordered_dict_on_first_use():
    holder = Holder()
    assert holder.cache == OrderedDict()
    assert holder.cache is holder.cache  # the same one afterwards


def test_each_instance_has_its_own_dict():
    a, b = Holder(), Holder()
    a.cache["k"] = 1
    assert "k" not in b.cache


def test_the_descriptor_stores_the_value_it_is_given():
    """It used to drop the value for an empty OrderedDict."""
    holder = Holder()
    holder.cache = {"only": "this"}
    assert holder.cache == {"only": "this"}


def test_the_descriptor_on_the_class_returns_itself():
    assert isinstance(Holder.cache, CacheFieldDescriptor)


# --- the state's cache ----------------------------------------------------------------


class FakeInstance:
    def __init__(self, key):
        self.key = key

    def get_cache_key(self):
        return self.key


@pytest.fixture
def state():
    return PipelineState(pipeline=object())


def test_a_cache_key_is_the_instances_or_the_string_itself():
    assert PipelineState.get_cache_key("raw-key") == "raw-key"
    assert PipelineState.get_cache_key(FakeInstance("from-instance")) == "from-instance"


def test_fields_set_for_one_instance_accumulate_and_do_not_wipe_each_other(state):
    inst = FakeInstance("p1")
    state.set_cache_for_pipeline_field(inst, "a", 1)
    state.set_cache_for_pipeline_field(inst, "b", 2)

    assert dict(state.cache(inst)) == {"a": 1, "b": 2}


def test_instances_are_cached_apart(state):
    one, two = FakeInstance("p1"), FakeInstance("p2")
    state.set_cache_for_pipeline_field(one, "a", 1)
    state.set_cache_for_pipeline_field(two, "a", 2)

    assert dict(state.cache(one)) == {"a": 1}
    assert dict(state.cache(two)) == {"a": 2}


def test_the_empty_marker_is_not_cached(state):
    inst = FakeInstance("p1")
    state.set_cache(inst, "pipeline_cache", field_name="a", value=EMPTY)

    assert state.check_cache_exists(inst) == set()
    assert dict(state.cache(inst)) == {}


def test_falsy_values_are_cached(state):
    inst = FakeInstance("p1")
    for name, value in (("zero", 0), ("empty", ""), ("none", None), ("no", False)):
        state.set_cache_for_pipeline_field(inst, name, value)

    assert dict(state.cache(inst)) == {
        "zero": 0,
        "empty": "",
        "none": None,
        "no": False,
    }


def test_check_cache_exists_names_the_cache_fields_holding_the_instance(state):
    inst = FakeInstance("p1")
    assert state.check_cache_exists(inst) == set()
    state.set_cache_for_pipeline_field(inst, "a", 1)
    state.set_cache(inst, "other_cache", field_name="x", value=9)

    assert state.check_cache_exists(inst) == {"pipeline_cache", "other_cache"}
    assert dict(state.cache("p1")) == {"a": 1, "x": 9}


def test_clear_forgets_one_instance_only(state):
    one, two = FakeInstance("p1"), FakeInstance("p2")
    state.set_cache_for_pipeline_field(one, "a", 1)
    state.set_cache_for_pipeline_field(two, "a", 2)

    state.clear(one)

    assert dict(state.cache(one)) == {}
    assert dict(state.cache(two)) == {"a": 2}


def test_clearing_an_unknown_instance_is_harmless(state):
    state.clear("never-cached")


def test_the_state_keeps_the_start_of_the_graph():
    start = object()
    assert PipelineState(start).start is start


def test_tree_extra_data_keeps_the_pipe():
    assert TreeExtraData(PipeType.POINTER).pipe_type is PipeType.POINTER


# --- pointy file lookup -------------------------------------------------------------------


def test_a_file_path_is_returned_as_it_is(tmp_path):
    f = tmp_path / "w.pty"
    f.write_text("A")
    assert _find_pointy_file_cached(str(f), "Anything") == str(f)


def test_a_directory_is_searched_recursively_by_class_name_ignoring_case(tmp_path):
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    f = nested / "mypipe.PTY"
    f.write_text("A")

    assert _find_pointy_file_cached(str(tmp_path), "MyPipe") == str(f)


def test_nothing_found_is_none(tmp_path):
    assert _find_pointy_file_cached(str(tmp_path), "Missing") is None
    assert _find_pointy_file_cached(str(tmp_path / "no-such-dir"), "Missing") is None


def test_directory_walk_returns_the_first_match_or_none(tmp_path):
    (tmp_path / "x.pty").write_text("A")
    assert _directory_walk(str(tmp_path), "x.pty").endswith("x.pty")
    assert _directory_walk(str(tmp_path), "y.pty") is None


# --- pickling ------------------------------------------------------------------------------


def test_a_pipeline_pickled_for_a_worker_carries_only_its_own_cache(
    pipeline_class, event_class
):
    """__getstate__ meant to narrow the cache to this instance; the descriptor ignored
    the narrowed dict, so every instance's fields travelled with each pipeline."""
    ev = event_class()
    cls = pipeline_class(
        ev.__name__, label=InputDataField(data_type=str, required=False)
    )
    mine, other = cls(label="mine"), cls(label="other")

    cache = mine.__getstate__()["_state"].__dict__["pipeline_cache"]

    assert list(cache) == [mine.get_cache_key()]
    assert other.get_cache_key() not in cache
