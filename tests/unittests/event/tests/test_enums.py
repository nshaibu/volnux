"""
Tests for the EventType and EventCategory enums defined in the EventBase
module.

These are the smallest unit in the event package — pure data classes with no
behavioural dependencies — so they make a good starting point and a sanity
check that the conftest successfully imported the real module.
"""

from __future__ import annotations

import pytest


# Lazy import so a bad module path surfaces here, not at collection time.
def _import_enums():
    try:
        from volnux.event.base import EventType, EventCategory
    except ImportError:
        from volnux.event import EventType, EventCategory  # type: ignore
    return EventType, EventCategory


class TestEventType:
    def test_members_match_documented_set(self):
        EventType, _ = _import_enums()
        names = {m.name for m in EventType}
        assert names == {"SYSTEM", "META", "OTHER"}

    def test_values(self):
        EventType, _ = _import_enums()
        assert EventType.SYSTEM.value == "system"
        assert EventType.META.value == "meta"
        assert EventType.OTHER.value == "other"

    def test_lookup_by_value(self):
        EventType, _ = _import_enums()
        assert EventType("system") is EventType.SYSTEM
        assert EventType("meta") is EventType.META
        assert EventType("other") is EventType.OTHER

    def test_invalid_value_raises(self):
        EventType, _ = _import_enums()
        with pytest.raises(ValueError):
            EventType("not-a-real-event-type")


class TestEventCategory:
    def test_is_str_subclass(self):
        """EventCategory inherits from `str, Enum` so members behave as strings."""
        _, EventCategory = _import_enums()
        assert issubclass(EventCategory, str)
        # str() of a member returns its value, not "EventCategory.X"
        assert str(EventCategory.EXTRACT) == "EXTRACT"

    def test_documented_members_present(self):
        _, EventCategory = _import_enums()
        documented = {
            "EXTRACT",
            "TRANSFORM",
            "LOAD",
            "INGEST",
            "DATABASE",
            "HTTP",
            "MESSAGING",
            "FILE",
            "CACHE",
            "VALIDATE",
            "ANALYZE",
            "MONITOR",
            "AUTOMATION",
            "NOTIFICATION",
            "CLEANUP",
            "AI",
            "AGENT",
            "OTHER",
        }
        actual = {m.name for m in EventCategory}
        assert documented.issubset(
            actual
        ), f"Missing documented categories: {documented - actual}"

    def test_each_member_value_equals_name(self):
        """Every EventCategory member has value == name (uppercase string)."""
        _, EventCategory = _import_enums()
        for member in EventCategory:
            assert member.value == member.name
            assert isinstance(member.value, str)

    def test_lookup_by_value(self):
        _, EventCategory = _import_enums()
        assert EventCategory("DATABASE") is EventCategory.DATABASE
        assert EventCategory("AI") is EventCategory.AI

    def test_member_hashable_and_unique(self):
        """Members must be usable in frozenset (used by EventBase.categories)."""
        _, EventCategory = _import_enums()
        fs = frozenset(
            {EventCategory.EXTRACT, EventCategory.LOAD, EventCategory.EXTRACT}
        )
        assert len(fs) == 2

    def test_other_is_default_for_event_base(self):
        """Sanity: EventBase.categories defaults to frozenset({OTHER})."""
        try:
            from volnux.event.base import EventBase, EventCategory
        except ImportError:
            from volnux.event import EventBase, EventCategory  # type: ignore
        assert EventBase.categories == frozenset({EventCategory.OTHER})
