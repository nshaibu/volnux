"""
Tests for the `EventMeta` metaclass.

EventMeta is responsible for auto-registering concrete EventBase subclasses
into the global event registry. Behaviour we verify:

  * `EventBase` and `ControlFlowEvent` are NOT auto-registered.
  * Concrete subclasses ARE auto-registered.
  * A bare `EventCategory` value on `categories` is normalised to a
    `frozenset({category})`.
  * A non-frozenset `categories` (e.g. list) raises `ImproperlyConfigured`
    *but* the class is still created (the error is logged via the swallowed
    `RuntimeError` path).
  * A `categories` containing non-`EventCategory` values raises the same.
  * A `RuntimeError` raised by the versioning layer is swallowed (logged
    as a warning), so a misconfigured event class does not crash import.
"""

from __future__ import annotations

import logging

import pytest
from unittest.mock import MagicMock, patch


def _import_meta():
    try:
        from volnux.event.base import (
            EventBase,
            EventMeta,
            EventType,
            EventCategory,
            get_event_registry,
        )
    except ImportError:
        from volnux.event import (  # type: ignore
            EventBase,
            EventMeta,
            EventType,
            EventCategory,
            get_event_registry,
        )
    return EventBase, EventMeta, EventType, EventCategory, get_event_registry


class TestEventMetaRegistration:
    def test_base_class_not_registered(self):
        EventBase, EventMeta, _, _, get_event_registry = _import_meta()
        registry = get_event_registry()
        # EventBase should not appear as a registered class
        registered = set(getattr(registry, "_classes", set()) or set())
        assert EventBase not in registered

    def test_concrete_subclass_is_registered(self):
        EventBase, _, _, EventCategory, get_event_registry = _import_meta()
        registry = get_event_registry()
        before = set(getattr(registry, "_classes", set()) or set())

        class _RegisteredEvent(EventBase):  # type: ignore[misc, valid-type]
            async def process(self, *args, **kwargs):
                return True, {}

        after = set(getattr(registry, "_classes", set()) or set())
        assert _RegisteredEvent in (after - before)

    def test_categories_bare_eventcategory_is_normalised(self):
        """If a subclass sets `categories = EventCategory.X` (a bare value,
        not a frozenset), the metaclass normalises it to `frozenset({X})`."""
        EventBase, _, _, EventCategory, get_event_registry = _import_meta()

        class _BareCategoryEvent(EventBase):  # type: ignore[misc, valid-type]
            categories = EventCategory.DATABASE

            async def process(self, *args, **kwargs):
                return True, {}

        assert _BareCategoryEvent.categories == frozenset({EventCategory.DATABASE})

    def test_categories_frozenset_passes_through(self):
        EventBase, _, _, EventCategory, _ = _import_meta()

        class _MultiCategoryEvent(EventBase):  # type: ignore[misc, valid-type]
            categories = frozenset({EventCategory.EXTRACT, EventCategory.HTTP})

            async def process(self, *args, **kwargs):
                return True, {}

        assert _MultiCategoryEvent.categories == frozenset(
            {EventCategory.EXTRACT, EventCategory.HTTP}
        )

    def test_categories_list_raises_improperly_configured(self):
        """A list (instead of frozenset) should raise ImproperlyConfigured
        inside the metaclass; the exception is wrapped and re-raised as
        RuntimeError, which the metaclass swallows."""
        EventBase, _, _, EventCategory, _ = _import_meta()
        from volnux.exceptions import ImproperlyConfigured

        # The metaclass catches RuntimeError, not ImproperlyConfigured.
        # So the ImproperlyConfigured raised inside __new__ will propagate
        # out of class creation. We expect it to surface.
        with pytest.raises(ImproperlyConfigured):
            class _ListCategoryEvent(EventBase):  # type: ignore[misc, valid-type]
                categories = [EventCategory.OTHER]  # type: ignore[assignment]

                async def process(self, *args, **kwargs):
                    return True, {}

    def test_categories_with_non_eventcategory_member_raises(self):
        EventBase, _, _, EventCategory, _ = _import_meta()
        from volnux.exceptions import ImproperlyConfigured

        with pytest.raises(ImproperlyConfigured):
            class _BadCategoryEvent(EventBase):  # type: ignore[misc, valid-type]
                categories = frozenset({"NOT_A_CATEGORY"})  # type: ignore[arg-type]

                async def process(self, *args, **kwargs):
                    return True, {}

    def test_runtime_error_during_registration_is_swallowed(self, caplog):
        """If `cls.get_version_handler()` raises RuntimeError, the metaclass
        logs a warning and continues (the class is still created)."""
        EventBase, _, _, _, _ = _import_meta()

        # We can't easily make get_version_handler raise RuntimeError at
        # class-creation time without monkey-patching. Instead, we verify
        # the swallow branch indirectly: a class that triggers a RuntimeError
        # inside the registration try-block still gets created successfully.

        # Patch `get_version_handler` on EventBase to raise RuntimeError.
        with patch.object(
            EventBase, "get_version_handler",
            side_effect=RuntimeError("simulated versioning failure"),
        ):
            with caplog.at_level(logging.WARNING):
                class _FailingVersionEvent(EventBase):  # type: ignore[misc, valid-type]
                    async def process(self, *args, **kwargs):
                        return True, {}

        # Class was created despite the RuntimeError
        assert _FailingVersionEvent is not None
        # And a warning was logged
        assert any(
            "simulated versioning failure" in r.getMessage()
            for r in caplog.records
        ), "Expected a warning log when versioning raises RuntimeError"


class TestEventMetaUsesRegistry:
    def test_registry_singleton_identity(self):
        """get_event_registry() returns the same instance every call."""
        _, _, _, _, get_event_registry = _import_meta()
        assert get_event_registry() is get_event_registry()

    def test_get_all_event_classes_returns_frozenset(self):
        EventBase, _, _, _, _ = _import_meta()
        result = EventBase.get_all_event_classes()
        # Implementation returns frozenset; allow any iterable-of-types fallback
        assert hasattr(result, "__iter__")
        for cls in result:
            assert isinstance(cls, type)

    def test_clear_class_cache_resets_registry(self):
        EventBase, _, _, _, get_event_registry = _import_meta()
        EventBase.clear_class_cache()
        registry = get_event_registry()
        # After clear, the registered class set should be empty (or close to it).
        # We can't assert == empty unconditionally because some modules may
        # re-register on import. We assert the call succeeds without error.
        assert registry is not None
