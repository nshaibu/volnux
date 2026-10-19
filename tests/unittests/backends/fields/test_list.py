from typing import ForwardRef

import pytest
from formax import BaseModel

from volnux.backends.fields import ListConfig, ListField
from volnux.backends.fields.foreign_key import ForeignKey
from volnux.backends.fields.list import (
    ManagedList,
    _find_field_name,
    _identity_key,
    _try_resolve_model,
)
from volnux.utils import get_obj_klass_import_str


class Tag:
    def __init__(self, id, name=""):
        self.id = id
        self.name = name


class OtherModel:
    def __init__(self, id):
        self.id = id


def _attrib(annotation):
    return annotation.__metadata__[0]


def _pre(annotation):
    return _attrib(annotation).pre_formatter


def _post(annotation):
    return _attrib(annotation).post_formatter


def _ref(object_id, model_path="tests.Tag", backend_alias="default"):
    return {
        "object_id": object_id,
        "model_path": model_path,
        "backend_alias": backend_alias,
    }


class TestListConfig:
    def test_defaults(self):
        config = ListConfig()
        assert config.max_length is None
        assert config.min_length == 0
        assert config.unique_items is False
        assert config.nullable is True
        assert config.item_nullable is False

    def test_is_frozen(self):
        config = ListConfig()
        with pytest.raises(Exception):
            config.max_length = 10


class TestManagedListBasics:
    def test_empty_by_default(self):
        ml = ManagedList()
        assert ml.len() == 0
        assert len(ml) == 0
        assert ml.is_empty()
        assert not ml
        assert list(ml) == []

    def test_wraps_list_by_reference(self):
        stored = [1, 2]
        ml = ManagedList(items=stored)
        ml.add(3)
        assert stored == [1, 2, 3]

    def test_copies_non_list_iterables(self):
        stored = (1, 2)
        ml = ManagedList(items=stored)
        ml.add(3)
        assert stored == (1, 2)
        assert list(ml) == [1, 2, 3]

    def test_add_without_item_model_accepts_anything(self):
        ml = ManagedList()
        ml.add("a")
        ml.add(1)
        assert list(ml) == ["a", 1]
        assert ml.is_dirty

    def test_add_validates_item_model(self):
        ml = ManagedList(item_model=Tag)
        ml.add(Tag(1))
        with pytest.raises(TypeError, match=r"Cannot add str to ListField\[Tag\]"):
            ml.add("bad")
        assert ml.len() == 1

    def test_add_accepts_foreign_key_for_item_model(self):
        ml = ManagedList(item_model=Tag)
        fk = ForeignKey(object_id="1", model_path="tests.Tag")
        ml.add(fk)
        assert ml[0] is fk

    def test_add_accepts_none(self):
        ml = ManagedList(item_model=Tag)
        ml.add(None)
        assert ml[0] is None

    def test_add_skips_check_for_unresolvable_forward_ref(self):
        ml = ManagedList(item_model=ForwardRef("DoesNotExist"))
        ml.add("anything")
        assert ml.len() == 1

    def test_append_is_alias_of_add(self):
        ml = ManagedList(item_model=Tag)
        ml.append(Tag(1))
        assert ml.len() == 1
        with pytest.raises(TypeError):
            ml.append("bad")

    def test_extend(self):
        ml = ManagedList(item_model=Tag)
        ml.extend([Tag(1), Tag(2)])
        assert ml.len() == 2

    def test_extend_stops_at_invalid_item(self):
        ml = ManagedList(item_model=Tag)
        with pytest.raises(TypeError):
            ml.extend([Tag(1), "bad", Tag(2)])
        assert ml.len() == 1

    def test_remove_existing(self):
        tag = Tag(1)
        ml = ManagedList(items=[tag, Tag(2)])
        assert ml.remove(tag) is True
        assert ml.len() == 1
        assert ml.is_dirty

    def test_remove_missing(self):
        ml = ManagedList(items=[1])
        assert ml.remove(2) is False
        assert not ml.is_dirty

    def test_remove_only_first_occurrence(self):
        ml = ManagedList(items=[1, 2, 1])
        ml.remove(1)
        assert list(ml) == [2, 1]

    def test_remove_serialized_reference_by_foreign_key(self):
        ml = ManagedList(items=[_ref("1"), _ref("2")])
        assert ml.remove(ForeignKey(object_id="1", model_path="tests.Tag")) is True
        assert [fk.object_id for fk in ml] == ["2"]

    def test_clear(self):
        ml = ManagedList(items=[1, 2])
        ml.clear()
        assert ml.is_empty()
        assert ml.is_dirty

    def test_clear_empty_is_not_dirty(self):
        ml = ManagedList()
        ml.clear()
        assert not ml.is_dirty

    def test_contains(self):
        ml = ManagedList(items=["a", "b"])
        assert ml.contains("a")
        assert "b" in ml
        assert "c" not in ml

    def test_contains_serialized_reference(self):
        ml = ManagedList(items=[_ref("1")])
        assert ForeignKey(object_id="1", model_path="tests.Tag") in ml

    def test_not_dirty_initially(self):
        assert not ManagedList(items=[1]).is_dirty


class TestManagedListAccess:
    def test_getitem_int(self):
        ml = ManagedList(items=["a", "b", "c"])
        assert ml[0] == "a"
        assert ml[-1] == "c"

    def test_getitem_out_of_range(self):
        ml = ManagedList(items=["a"])
        with pytest.raises(IndexError):
            ml[5]
        with pytest.raises(IndexError):
            ml[-5]

    def test_getitem_slice(self):
        ml = ManagedList(items=[1, 2, 3, 4])
        assert ml[1:3] == [2, 3]
        assert ml[::-1] == [4, 3, 2, 1]

    def test_iteration_resolves_reference_dicts(self):
        ml = ManagedList(items=[_ref("1", backend_alias="secondary"), "plain"])
        items = list(ml)
        assert isinstance(items[0], ForeignKey)
        assert items[0].object_id == "1"
        assert items[0].model_path == "tests.Tag"
        assert items[0].backend_alias == "secondary"
        assert items[1] == "plain"

    def test_resolved_reference_is_cached(self):
        stored = [_ref("1")]
        ml = ManagedList(items=stored)
        first = ml[0]
        assert ml[0] is first
        assert stored[0] is first

    def test_reference_missing_backend_alias_defaults(self):
        ml = ManagedList(items=[{"object_id": "1", "model_path": "tests.Tag"}])
        assert ml[0].backend_alias == "default"

    def test_non_reference_dicts_left_alone(self):
        ml = ManagedList(items=[{"key": "value"}])
        assert ml[0] == {"key": "value"}

    def test_slice_resolves_references(self):
        ml = ManagedList(items=[_ref("1"), _ref("2")])
        assert all(isinstance(item, ForeignKey) for item in ml[:])


class TestManagedListRepr:
    def test_repr_without_model(self):
        assert repr(ManagedList(items=[1])) == "<ManagedList[Any] len=1>"

    def test_repr_with_model(self):
        assert repr(ManagedList(item_model=Tag)) == "<ManagedList[Tag] len=0>"

    def test_repr_with_forward_ref(self):
        ml = ManagedList(item_model=ForwardRef("Pending"))
        assert repr(ml) == "<ManagedList[Pending] len=0>"


class TestManagedListSerialization:
    def test_model_instance(self):
        tag = Tag(42)
        ml = ManagedList(items=[tag])
        assert ml.to_serializable() == [
            {
                "object_id": "42",
                "model_path": get_obj_klass_import_str(tag),
                "backend_alias": "default",
            }
        ]

    def test_model_instance_backend_alias(self):
        tag = Tag(1)
        tag._backend_alias = "replica"
        ml = ManagedList(items=[tag])
        assert ml.to_serializable()[0]["backend_alias"] == "replica"

    def test_foreign_key_keeps_its_model_path(self):
        fk = ForeignKey(object_id="7", model_path="app.models.Tag", backend_alias="b")
        ml = ManagedList(items=[fk])
        assert ml.to_serializable() == [
            {"object_id": "7", "model_path": "app.models.Tag", "backend_alias": "b"}
        ]

    def test_dicts_and_primitives_pass_through(self):
        ref = _ref("1")
        ml = ManagedList(items=[ref, "a", 1, None])
        assert ml.to_serializable() == [ref, "a", 1, None]

    def test_round_trip(self):
        ml = ManagedList(items=[Tag(1), "x"])
        restored = ManagedList.from_serializable(ml.to_serializable())
        assert restored[0].object_id == "1"
        assert restored.to_serializable() == ml.to_serializable()

    def test_from_serializable(self):
        parent = object()
        ml = ManagedList.from_serializable(
            [1, 2], item_model=Tag, parent_instance=parent, field_name="tags"
        )
        assert ml.len() == 2
        assert ml._item_model is Tag
        assert ml._parent_instance is parent
        assert ml._field_name == "tags"

    def test_from_serializable_none(self):
        assert ManagedList.from_serializable(None).is_empty()


class TestListFieldDeclaration:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError, match="cannot be instantiated"):
            ListField()

    def test_cannot_subclass(self):
        with pytest.raises(TypeError, match="Cannot subclass"):

            class Sub(ListField):
                pass

    def test_single_param_uses_default_config(self):
        metadata = _attrib(ListField[Tag]).metadata
        assert metadata["type"] == "list"
        assert metadata["item_model"] is Tag
        assert metadata["max_length"] is None
        assert metadata["min_length"] == 0
        assert metadata["unique_items"] is False
        assert metadata["nullable"] is True
        assert metadata["item_nullable"] is False

    def test_config_reflected_in_metadata(self):
        config = ListConfig(
            max_length=5,
            min_length=1,
            unique_items=True,
            nullable=False,
            item_nullable=True,
        )
        metadata = _attrib(ListField[Tag, config]).metadata
        assert metadata["max_length"] == 5
        assert metadata["min_length"] == 1
        assert metadata["unique_items"] is True
        assert metadata["nullable"] is False
        assert metadata["item_nullable"] is True

    def test_string_model_becomes_forward_ref(self):
        item_model = _attrib(ListField["Tag"]).metadata["item_model"]
        assert isinstance(item_model, ForwardRef)
        assert item_model.__forward_arg__ == "Tag"

    def test_dotted_string_model_keeps_module(self):
        item_model = _attrib(ListField["app.models.Tag"]).metadata["item_model"]
        assert item_model.__forward_arg__ == "Tag"
        assert item_model.__forward_module__ == "app.models"

    def test_invalid_config_type(self):
        with pytest.raises(TypeError, match="Second argument must be ListConfig"):
            ListField[Tag, {"max_length": 1}]

    def test_too_many_params(self):
        with pytest.raises(TypeError, match="expects 1 or 2 arguments"):
            ListField[Tag, ListConfig(), "extra"]

    def test_negative_min_length(self):
        with pytest.raises(ValueError, match="min_length cannot be negative"):
            ListField[Tag, ListConfig(min_length=-1)]

    def test_max_length_below_min_length(self):
        with pytest.raises(ValueError, match="max_length cannot be less"):
            ListField[Tag, ListConfig(min_length=3, max_length=2)]

    def test_non_nullable_adds_null_validator(self):
        assert _attrib(ListField[Tag, ListConfig(nullable=False)]).validators
        assert not _attrib(ListField[Tag]).validators


class TestListFieldPreFormatter:
    def test_none_when_nullable(self):
        assert _pre(ListField[Tag])(None, None) is None

    def test_none_when_not_nullable(self):
        pre = _pre(ListField[Tag, ListConfig(nullable=False)])
        with pytest.raises(ValueError, match="cannot be None"):
            pre(None, None)

    def test_accepts_list_and_tuple(self):
        pre = _pre(ListField[Tag])
        tags = [Tag(1), Tag(2)]
        assert pre(None, tags) == tags
        assert pre(None, tuple(tags)) == tags

    def test_managed_list_is_serialized(self):
        pre = _pre(ListField[Tag])
        result = pre(None, ManagedList(items=[Tag(1)]))
        assert result[0]["object_id"] == "1"

    def test_rejects_non_list(self):
        pre = _pre(ListField[Tag])
        with pytest.raises(TypeError, match="Expected list or ManagedList"):
            pre(None, "not-a-list")

    def test_min_length(self):
        pre = _pre(ListField[Tag, ListConfig(min_length=2)])
        with pytest.raises(ValueError, match="minimum is 2"):
            pre(None, [Tag(1)])
        assert len(pre(None, [Tag(1), Tag(2)])) == 2

    def test_max_length(self):
        pre = _pre(ListField[Tag, ListConfig(max_length=1)])
        with pytest.raises(ValueError, match="maximum is 1"):
            pre(None, [Tag(1), Tag(2)])
        assert len(pre(None, [Tag(1)])) == 1

    def test_item_type_mismatch(self):
        pre = _pre(ListField[Tag])
        with pytest.raises(TypeError, match=r"List item \[1\]: expected Tag"):
            pre(None, [Tag(1), OtherModel(2)])

    def test_dicts_and_foreign_keys_skip_type_check(self):
        pre = _pre(ListField[Tag])
        fk = ForeignKey(object_id="1", model_path="tests.Tag")
        assert pre(None, [_ref("1"), fk]) == [_ref("1"), fk]

    def test_none_item_not_allowed_by_default(self):
        pre = _pre(ListField[Tag])
        with pytest.raises(ValueError, match=r"List item \[0\] cannot be None"):
            pre(None, [None])

    def test_none_item_allowed_when_item_nullable(self):
        pre = _pre(ListField[Tag, ListConfig(item_nullable=True)])
        assert pre(None, [None, Tag(1)])[0] is None

    def test_unresolvable_forward_ref_skips_type_check(self):
        pre = _pre(ListField["DoesNotExist"])
        assert pre(None, ["anything", 1]) == ["anything", 1]

    def test_unique_items_rejects_duplicate_model_ids(self):
        pre = _pre(ListField[Tag, ListConfig(unique_items=True)])
        with pytest.raises(ValueError, match=r"List item \[1\] is a duplicate"):
            pre(None, [Tag(1), Tag(1)])

    def test_unique_items_detects_instance_vs_reference_duplicate(self):
        pre = _pre(ListField[Tag, ListConfig(unique_items=True)])
        tag = Tag(1)
        ref = _ref("1", model_path=get_obj_klass_import_str(tag))
        with pytest.raises(ValueError, match="duplicate"):
            pre(None, [tag, ref])

    def test_unique_items_allows_distinct(self):
        pre = _pre(ListField[Tag, ListConfig(unique_items=True)])
        assert len(pre(None, [Tag(1), Tag(2)])) == 2

    def test_unique_items_primitives(self):
        pre = _pre(ListField["DoesNotExist", ListConfig(unique_items=True)])
        with pytest.raises(ValueError, match="duplicate"):
            pre(None, ["a", "a"])
        assert pre(None, ["a", "b"]) == ["a", "b"]

    def test_duplicates_allowed_without_unique_items(self):
        pre = _pre(ListField[Tag])
        tag = Tag(1)
        assert pre(None, [tag, tag]) == [tag, tag]


class TestListFieldPostFormatter:
    def test_none_passthrough(self):
        assert _post(ListField[Tag])(None, None) is None

    def test_wraps_list_in_managed_list(self):
        annotation = ListField[Tag]

        class Host:
            __annotations__ = {"tags": annotation}

        host = Host()
        stored = [_ref("1")]
        ml = _post(annotation)(host, stored)
        assert isinstance(ml, ManagedList)
        assert ml._item_model is Tag
        assert ml._parent_instance is host
        assert ml._field_name == "tags"
        ml.add(Tag(2))
        assert len(stored) == 2

    def test_managed_list_returned_as_is(self):
        annotation = ListField[Tag]
        ml = ManagedList()
        assert _post(annotation)(object(), ml) is ml

    def test_field_name_found_on_base_class(self):
        annotation = ListField[Tag]

        class Base:
            __annotations__ = {"tags": annotation}

        class Child(Base):
            __annotations__ = {"other": int}

        assert _post(annotation)(Child(), [])._field_name == "tags"


class TestHelpers:
    def test_try_resolve_model(self):
        assert _try_resolve_model(None) is None
        assert _try_resolve_model(Tag) is Tag
        assert _try_resolve_model(ForwardRef("DoesNotExist")) is None

    def test_identity_key(self):
        tag = Tag(1)
        path = get_obj_klass_import_str(tag)
        fk = ForeignKey(object_id="1", model_path=path)
        assert _identity_key(tag) == _identity_key(fk) == _identity_key(_ref("1", path))
        assert _identity_key("a") == ("val", "a")
        assert _identity_key([1]) == ("val", "[1]")

    def test_find_field_name(self):
        first = ListField[Tag]
        second = ListField[Tag]

        class Host:
            __annotations__ = {"a": first, "b": second, "c": int}

        assert _find_field_name(Host, _attrib(first)) == "a"
        assert _find_field_name(Host, _attrib(second)) == "b"
        assert _find_field_name(Host, _attrib(ListField[Tag])) == ""


class TestListFieldOnModel:
    def test_assignment_runs_pre_formatter(self):
        class Holder(BaseModel):
            tags: ListField[Tag, ListConfig(max_length=2)]

        holder = Holder(tags=[Tag(1)])
        assert len(holder.tags) == 1

        with pytest.raises(RuntimeError) as exc_info:
            holder.tags = [Tag(1), Tag(2), Tag(3)]
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_assignment_rejects_wrong_item_type(self):
        class Holder(BaseModel):
            tags: ListField[Tag]

        with pytest.raises(RuntimeError) as exc_info:
            Holder(tags=["bad"])
        assert isinstance(exc_info.value.__cause__, TypeError)
