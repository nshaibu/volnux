import collections
import dataclasses
from unittest.mock import MagicMock

import pytest

from volnux.exceptions import MultiValueError
from volnux.result import ResultSet
from volnux.result.result import EntityContentType


class MockResult:
    def __init__(self, id, **attributes):
        self.id = id
        for key, value in attributes.items():
            setattr(self, key, value)

    def __hash__(self):
        return hash(self.id)

    def __object_import_str__(self):
        return f"{self.__class__.__module__}.{self.__class__.__qualname__}"


@pytest.fixture
def mock_results():
    return [
        MockResult(
            id="1",
            content={"name": "Alice", "age": 30, "tags": ["urgent", "important"]},
        ),
        MockResult(id="2", content={"name": "Bob", "age": 25, "tags": ["important"]}),
        MockResult(id="3", content={"name": "Charlie", "age": 35, "tags": ["urgent"]}),
    ]


@pytest.fixture
def result_set(mock_results):
    return ResultSet(mock_results)


def test_result_set_initialization(result_set, mock_results):
    assert len(result_set) == len(mock_results)
    for result in mock_results:
        assert result in result_set


def test_result_set_add(result_set):
    new_result = MockResult(id="4", name="Diana", age=28)
    result_set.add(new_result)
    assert len(result_set) == 4
    assert new_result in result_set


def test_result_set_discard(result_set):
    result_to_remove = MockResult(id="1")
    result_set.discard(result_to_remove)
    assert len(result_set) == 2
    assert result_to_remove not in result_set


def test_result_set_clear(result_set):
    result_set.clear()
    assert len(result_set) == 0


def test_result_set_get(result_set):
    result = result_set.get(id="1")
    assert result.id == "1"


def test_result_set_get_no_match(result_set):
    with pytest.raises(KeyError):
        result_set.get(id="999")


def test_result_set_get_multiple_matches(result_set):
    result_set.add(
        MockResult(id="4", content={"name": "Alice", "age": 30, "tags": ["important"]})
    )
    with pytest.raises(MultiValueError):
        result_set.get(content__name="Alice")


def test_result_set_filter(result_set):
    filtered = result_set.filter(id="2")
    assert len(filtered) == 1


def test_result_set_filter_nested(result_set):
    filtered = result_set.filter(content__tags__contains="urgent")
    assert len(filtered) == 2
    assert all("urgent" in result.content["tags"] for result in filtered)


def test_result_set_copy(result_set):
    copied = result_set.copy()
    assert len(copied) == len(result_set)
    assert copied is not result_set
    assert all(result in copied for result in result_set)


def test_result_set_first(result_set):
    first_result = result_set.first()
    assert first_result.id == "1"


def test_result_set_first_empty():
    empty_set = ResultSet([])
    assert empty_set.first() is None


def test_result_set_str_repr(result_set):
    assert str(result_set) == str(list(result_set))
    # All mock results share one class, so one content type is tracked
    assert repr(result_set) == "<ResultSet: 3 items, 1 content types>"


def test_result_set_identity_uses_primary_key(result_set):
    # A distinct object with the same id is the same entry
    assert MockResult(id="1") in result_set

    result_set.add(MockResult(id="1", content={"name": "Replaced"}))

    assert len(result_set) == 3
    assert result_set.get(id="1").content == {"name": "Replaced"}


def test_result_set_identity_falls_back_to_hash_without_id():
    class NoId:
        def __init__(self, value):
            self.value = value

        def __hash__(self):
            return hash(self.value)

    results = ResultSet([NoId("a"), NoId("a"), NoId("b")])

    assert len(results) == 2


def test_result_set_add_merges_result_set(result_set):
    other = ResultSet([MockResult(id="3"), MockResult(id="4")])

    result_set.add(other)

    assert sorted(r.id for r in result_set) == ["1", "2", "3", "4"]


def test_result_set_discard_result_set(result_set):
    result_set.discard(ResultSet([MockResult(id="1"), MockResult(id="2")]))

    assert [r.id for r in result_set] == ["3"]


def test_result_set_extend(result_set):
    result_set.extend([MockResult(id="4"), MockResult(id="5")])

    assert len(result_set) == 5


def test_result_set_indexing(result_set):
    assert result_set[0].id == "1"
    assert result_set[-1].id == "3"

    with pytest.raises(IndexError):
        result_set[3]
    with pytest.raises(IndexError):
        result_set[-4]


def test_result_set_is_empty_and_count(result_set):
    assert result_set.is_empty() is False
    assert result_set.count() == 3

    result_set.clear()

    assert result_set.is_empty() is True
    assert repr(result_set) == "<ResultSet: 0 items, 0 content types>"


def test_result_set_get_entry_by_identity(result_set):
    assert result_set.get_entry_by_identity("2").id == "2"
    assert result_set.get_entry_by_identity("pk:2").id == "2"

    with pytest.raises(KeyError):
        result_set.get_entry_by_identity("999")


def test_result_set_exclude(result_set):
    remaining = result_set.exclude(content__tags__contains="urgent")

    assert [r.id for r in remaining] == ["2"]


def test_result_set_filter_with_multiple_conditions_is_and(result_set):
    filtered = result_set.filter(content__tags__contains="urgent", content__age__gt=30)

    assert [r.id for r in filtered] == ["3"]


@pytest.mark.parametrize(
    "lookup, expected_ids",
    [
        ({"content__name": "Bob"}, ["2"]),
        ({"content__name__exact": "Bob"}, ["2"]),
        ({"content__name__startswith": "Ch"}, ["3"]),
        ({"content__name__endswith": "ce"}, ["1"]),
        ({"content__name__icontains": "LI"}, ["1", "3"]),
        ({"content__age__gte": 30}, ["1", "3"]),
        ({"content__age__lt": 30}, ["2"]),
        ({"content__age__lte": 25}, ["2"]),
        ({"content__age__in": [25, 35]}, ["2", "3"]),
        ({"content__missing__isnull": True}, ["1", "2", "3"]),
        ({"content__name__isnull": False}, ["1", "2", "3"]),
        ({"content__missing__gt": 1}, []),
    ],
)
def test_result_set_filter_operators(result_set, lookup, expected_ids):
    assert [r.id for r in result_set.filter(**lookup)] == expected_ids


def test_result_set_operators_treat_incomparable_types_as_no_match(result_set):
    result_set.add(MockResult(id="4", content={"name": "Dee", "age": "unknown"}))

    assert sorted(r.id for r in result_set.filter(content__age__gt=26)) == ["1", "3"]
    assert len(result_set.filter(content__age__in=30)) == 0


def test_result_set_string_operators_ignore_non_strings(result_set):
    assert len(result_set.filter(content__age__startswith="3")) == 0


def test_result_set_copy_is_independent(result_set):
    copied = result_set.copy()
    copied.add(MockResult(id="4"))

    assert len(result_set) == 3
    assert repr(copied) == "<ResultSet: 4 items, 1 content types>"


def test_result_set_filter_keeps_content_types(result_set):
    filtered = result_set.filter(id="999")

    assert len(filtered) == 0
    assert repr(filtered) == "<ResultSet: 0 items, 1 content types>"


def test_entity_content_type_equality():
    entity1 = EntityContentType(model_path="pkg.Model", backend_alias="default")
    entity2 = EntityContentType(model_path="pkg.Model")
    entity3 = EntityContentType(model_path="pkg.Other", backend_alias="replica")

    assert entity1 == entity2
    assert entity1 != entity3
    assert hash(entity1) == hash(entity2)
    assert len({entity1, entity2, entity3}) == 2


def test_entity_content_type_is_immutable():
    entity = EntityContentType(model_path="pkg.Model")

    with pytest.raises(dataclasses.FrozenInstanceError):
        entity.model_path = "pkg.Other"


def test_entity_content_type_from_entity():
    content_type = EntityContentType.from_entity(MockResult(id="1"))

    assert content_type == EntityContentType(
        model_path=f"{MockResult.__module__}.MockResult"
    )


def test_entity_content_type_from_entity_uses_backend_alias():
    result = MockResult(id="1", _backend_alias="replica")

    assert EntityContentType.from_entity(result).backend_alias == "replica"


@pytest.mark.parametrize("entity", [None, MockResult(id=None), MockResult(id="")])
def test_entity_content_type_from_entity_requires_id(entity):
    assert EntityContentType.from_entity(entity) is None


def test_entity_content_type_resolve_model():
    content_type = EntityContentType(model_path="collections.OrderedDict")

    assert content_type.resolve_model() is collections.OrderedDict


def test_entity_content_type_repr():
    entity = EntityContentType(model_path="pkg.Model", backend_alias="replica")

    assert repr(entity) == "<EntityContentType: pkg.Model (backend=replica)>"
