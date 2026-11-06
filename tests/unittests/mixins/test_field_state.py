"""The default get_state/set_state of KeyValueStoreIntegrationMixin: every
formax field serializes itself through the codecs in its Attrib metadata."""

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from formax import BaseModel, InitStrategy

from volnux.backends.fields import (
    FKConfig,
    ForeignKeyField,
    ListField,
    ObjectConfig,
    ObjectField,
    OnDelete,
)
from volnux.backends.fields.foreign_key import ForeignKey
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


class Phase(Enum):
    NEW = "new"
    DONE = "done"


@dataclass
class Counter:
    hits: int = 0


class Author(KeyValueStoreIntegrationMixin, BaseModel):
    name: str


class Tag(KeyValueStoreIntegrationMixin, BaseModel):
    label: str


class Article(KeyValueStoreIntegrationMixin, BaseModel):
    title: str
    author: ForeignKeyField[Optional[Author], FKConfig(on_delete=OnDelete.SET_NULL)]
    tags: ListField[Tag]
    phase: ObjectField[Phase, ObjectConfig(default=Phase.NEW)]
    counter: ObjectField[Counter, ObjectConfig(default_factory=Counter)]
    _scratch: list = field(default_factory=list)

    class Config:
        init_strategy = InitStrategy.DATACLASS


def make_article(**kwargs):
    return Article(title="hello", **kwargs)


def roundtrip(article):
    state = json.loads(json.dumps(article.__getstate__()))
    record_id = state.pop("id")
    loaded = Article.hydrate_formax_model(state)
    loaded.change_object_id(record_id)
    return loaded


class TestGetState:
    def test_fields_serialize_themselves(self):
        author = Author(name="Ada")
        tag = Tag(label="python")
        article = make_article(author=author, tags=[tag])
        article.phase = Phase.DONE
        article.counter.hits = 3

        state = article.get_state()

        assert state["title"] == "hello"
        assert state["author"] == {"type": "foreignkey", "object_id": author.id}
        assert [ref["object_id"] for ref in state["tags"]] == [tag.id]
        assert state["phase"] == "done"
        assert state["counter"] == {"hits": 3}

    def test_state_is_json_serializable(self):
        article = make_article(author=Author(name="Ada"), tags=[Tag(label="x")])
        json.dumps(article.__getstate__())

    def test_defaults_are_serialized(self):
        state = make_article().get_state()
        assert state["author"] is None
        assert state["tags"] == []
        assert state["phase"] == "new"
        assert state["counter"] == {"hits": 0}

    def test_underscore_fields_are_never_persisted(self):
        article = make_article()
        article._scratch.append("temp")
        state = article.get_state()
        assert not [key for key in state if key.startswith("_")]

    def test_getstate_adds_the_record_id(self):
        article = make_article()
        assert article.__getstate__()["id"] == article.id


class TestSetState:
    def test_round_trip(self):
        author = Author(name="Ada")
        tag = Tag(label="python")
        article = make_article(author=author, tags=[tag])
        article.phase = Phase.DONE
        article.counter.hits = 7

        loaded = roundtrip(article)

        assert loaded.id == article.id
        assert loaded.title == "hello"
        assert loaded.phase is Phase.DONE
        assert loaded.counter == Counter(hits=7)

    def test_references_come_back_lazy(self):
        author = Author(name="Ada")
        tag = Tag(label="python")
        loaded = roundtrip(make_article(author=author, tags=[tag]))

        assert isinstance(loaded.author, ForeignKey)
        assert loaded.author.id == author.id
        assert not loaded.author.is_resolved
        assert [reference.id for reference in loaded.tags] == [tag.id]

    def test_empty_references(self):
        loaded = roundtrip(make_article())
        assert loaded.author is None
        assert len(loaded.tags) == 0

    def test_runtime_fields_get_their_defaults(self):
        loaded = roundtrip(make_article())
        assert loaded._scratch == []
