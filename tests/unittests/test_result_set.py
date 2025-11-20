from unittest.mock import MagicMock

import pytest

from volnux.exceptions import MultiValueError
from volnux.result import EntityContentType, ResultSet


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
    assert repr(result_set) == f"<ResultSet: {len(result_set)}>"


def test_entity_content_type_equality():
    entity1 = EntityContentType(
        backend_import_str="backend1", entity_content_type="type1"
    )
    entity2 = EntityContentType(
        backend_import_str="backend1", entity_content_type="type1"
    )
    entity3 = EntityContentType(
        backend_import_str="backend2", entity_content_type="type2"
    )

    assert entity1 == entity2
    assert entity1 != entity3


def test_entity_content_type_hash():
    entity = EntityContentType(
        backend_import_str="backend1", entity_content_type="type1"
    )
    assert hash(entity) == hash(("backend1", "type1"))
