from unittest.mock import MagicMock, patch

import pytest

from volnux.exceptions import ImproperlyConfigured
from volnux.versioning.base import BaseVersioning
from volnux.versioning.handler import VersionHandler


class DummyVersioning(BaseVersioning):
    default_version = "2.0.0"

    def get_version_info(self, klass):
        return {
            "version": "info-version",
            "namespace": self.get_namespace(klass),
            "changelog": "info-changelog",
            "deprecated": False,
            "deprecation_info": None,
        }

    def validate_version(self, version: str) -> bool:
        return version.startswith("v") or version == "2.0.0"


class EventWithExplicitScheme:
    versioning_class = DummyVersioning
    version = "v3"
    changelog = "added feature"
    deprecated = True
    deprecation_info = {"reason": "legacy"}
    namespace = "billing"
    name = "billing-event"


class PlainEvent:
    pass


class NamedEvent:
    name = "custom-name"


def test_from_class_uses_class_versioning_scheme():
    handler = VersionHandler.from_class(EventWithExplicitScheme, "EVENT_VERSIONING")

    assert isinstance(handler.scheme, DummyVersioning)
    assert handler.version == "v3"
    assert handler.changelog == "added feature"
    assert handler.deprecated is True
    assert handler.deprecation_info == {"reason": "legacy"}
    assert handler.namespace == "billing"
    assert handler.class_name == "billing-event"


def test_from_class_uses_default_version_when_class_has_no_version():
    class EventNoVersion:
        versioning_class = DummyVersioning

    handler = VersionHandler.from_class(EventNoVersion, "EVENT_VERSIONING")

    assert handler.version == "2.0.0"


def test_from_class_uses_configured_scheme_when_class_has_no_scheme():
    with patch(
        "volnux.versioning.handler.conf.get",
        return_value={"VERSIONING_CLASS": DummyVersioning},
    ):
        handler = VersionHandler.from_class(PlainEvent, "EVENT_VERSIONING")

    assert isinstance(handler.scheme, DummyVersioning)
    assert handler.version == "2.0.0"
    assert handler.namespace == "local"
    assert handler.class_name == "PlainEvent"


def test_from_class_uses_semantic_versioning_as_fallback_default():
    class FakeSemanticVersioning(BaseVersioning):
        default_version = "1.0.0"

        def get_version_info(self, klass):
            return {
                "version": self.default_version,
                "namespace": self.get_namespace(klass),
                "changelog": None,
                "deprecated": False,
                "deprecation_info": None,
            }

        def validate_version(self, version: str) -> bool:
            return True

        def get_namespace(self, klass):
            return "local"

        def get_event_name(self, klass):
            return "PlainEvent"

    with patch("volnux.versioning.handler.conf.get", return_value={}), patch(
        "volnux.versioning.handler.SemanticVersioning",
        FakeSemanticVersioning,
    ):
        handler = VersionHandler.from_class(PlainEvent, "EVENT_VERSIONING")

    assert isinstance(handler.scheme, FakeSemanticVersioning)
    assert handler.version == "1.0.0"
    assert handler.namespace == "local"
    assert handler.class_name == "PlainEvent"


def test_from_class_raises_when_configured_scheme_is_not_base_versioning_subclass():
    class NotAScheme:
        pass

    with patch(
        "volnux.versioning.handler.conf.get",
        return_value={"VERSIONING_CLASS": NotAScheme},
    ):
        with pytest.raises(
            ImproperlyConfigured, match="is not a subclass of BaseVersioning"
        ):
            VersionHandler.from_class(PlainEvent, "EVENT_VERSIONING")


def test_get_info_delegates_to_scheme():
    scheme = MagicMock()
    scheme.get_version_info.return_value = {"version": "v1"}

    handler = VersionHandler(
        scheme=scheme,
        version="v1",
        namespace="local",
        class_name="MyEvent",
    )

    result = handler.get_info()

    scheme.get_version_info.assert_called_once_with(VersionHandler)
    assert result == {"version": "v1"}


def test_is_deprecated_returns_handler_flag():
    handler = VersionHandler(
        scheme=MagicMock(),
        version="v1",
        deprecated=True,
    )

    assert handler.is_deprecated() is True


def test_validate_delegates_to_scheme():
    scheme = MagicMock()
    scheme.validate_version.return_value = True

    handler = VersionHandler(
        scheme=scheme,
        version="v2",
    )

    result = handler.validate()

    scheme.validate_version.assert_called_once_with("v2")
    assert result is True


def test_from_class_uses_class_name_when_name_attribute_missing():
    class EventWithoutCustomName:
        versioning_class = DummyVersioning

    handler = VersionHandler.from_class(EventWithoutCustomName, "EVENT_VERSIONING")

    assert handler.class_name == "EventWithoutCustomName"


def test_from_class_uses_scheme_namespace_resolution():
    class EventWithoutNamespace:
        versioning_class = DummyVersioning

    with patch(
        "volnux.versioning.base.conf.get",
        return_value={"DEFAULT_NAMESPACE": "configured-ns"},
    ):
        handler = VersionHandler.from_class(EventWithoutNamespace, "EVENT_VERSIONING")

    assert handler.namespace == "configured-ns"
