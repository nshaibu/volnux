import logging

import pytest

from volnux import EventBase
from volnux.event.base import (
    EventCategory,
    EventMeta,
    EventType,
    get_event_registry,
)
from volnux.exceptions import ImproperlyConfigured

from .conftest import make_event_class, unique_name


async def _process(self):
    return True, 1


class TestRegistration:
    def test_subclasses_are_registered_by_name(self):
        event_class = make_event_class()
        registry = get_event_registry()
        assert registry.is_registered(event_class.__name__)
        assert event_class in registry.list_all_classes()

    def test_the_base_class_itself_is_not_registered(self):
        assert EventBase not in get_event_registry().list_all_classes()
        assert not get_event_registry().is_registered("EventBase")

    def test_metaclass_is_applied_to_subclasses(self):
        assert type(make_event_class()) is EventMeta

    def test_intermediate_base_classes_register_too(self):
        parent = make_event_class()
        child = type(unique_name("Child"), (parent,), {})
        assert child in get_event_registry().list_all_classes()

    def test_same_name_is_a_reload_and_warns(self):
        name = unique_name("Reloaded")
        type(name, (EventBase,), {"process": _process})
        with pytest.warns(RuntimeWarning, match="already registered"):
            second = type(name, (EventBase,), {"process": _process})
        # The class is still created and usable; it simply is not re-registered.
        assert second.__name__ == name

    def test_registry_conflict_is_logged_not_raised(self, caplog):
        name = unique_name("Conflict")
        first = type(name, (EventBase,), {"process": _process})
        first.__module__ = "somewhere.else"
        registry = get_event_registry()
        entry = registry._handler_registry.filter(name=name).first()
        entry.module_path = "somewhere.else"
        with caplog.at_level(logging.WARNING, logger="volnux.event.base"):
            clash = type(name, (EventBase,), {"process": _process})
        assert "Conflicting" in caplog.text
        assert clash.__name__ == name


class TestCategories:
    def test_default(self):
        assert make_event_class().categories == frozenset({EventCategory.OTHER})

    def test_frozenset_is_kept(self):
        categories = frozenset({EventCategory.EXTRACT, EventCategory.HTTP})
        assert make_event_class(categories=categories).categories == categories

    def test_bare_category_is_normalised_on_the_class(self):
        """The class attribute used to stay a bare enum member while the registry got a frozenset."""
        event_class = make_event_class(categories=EventCategory.DATABASE)
        assert event_class.categories == frozenset({EventCategory.DATABASE})

    @pytest.mark.parametrize(
        "bad",
        [[EventCategory.OTHER], {EventCategory.OTHER}, frozenset({"EXTRACT"}), "OTHER"],
    )
    def test_invalid_categories_are_rejected(self, bad):
        with pytest.raises(ImproperlyConfigured, match="frozenset of EventCategory"):
            make_event_class(categories=bad)

    def test_category_is_stored_in_the_registry_entry(self):
        event_class = make_event_class(categories=frozenset({EventCategory.AI}))
        entry = (
            get_event_registry()
            ._handler_registry.filter(name=event_class.__name__)
            .first()
        )
        assert entry.extra["categories"] == frozenset({EventCategory.AI})


class TestEnums:
    def test_event_type_values(self):
        assert {t.name: t.value for t in EventType} == {
            "SYSTEM": "system",
            "META": "meta",
            "OTHER": "other",
        }

    def test_category_values_match_names(self):
        for category in EventCategory:
            assert category.value == category.name
            assert isinstance(category, str)

    def test_category_lookup(self):
        assert EventCategory("HTTP") is EventCategory.HTTP
        with pytest.raises(ValueError):
            EventCategory("NOPE")

    def test_expected_categories_exist(self):
        names = {c.name for c in EventCategory}
        assert {"EXTRACT", "TRANSFORM", "LOAD", "AI", "AGENT", "OTHER"} <= names

    def test_event_type_is_a_class_attribute(self):
        assert make_event_class(event_type=EventType.META).event_type is EventType.META
