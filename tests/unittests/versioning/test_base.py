from unittest.mock import patch

from volnux.versioning.base import BaseVersioning


class DummyVersioning(BaseVersioning):
    def get_version_info(self, klass):
        return {
            "version": "1.0.0",
            "namespace": self.get_namespace(klass),
            "changelog": None,
            "deprecated": self.is_deprecated(klass),
            "deprecation_info": self.get_deprecation_info(klass),
        }

    def validate_version(self, version):
        return version == "1.0.0"


class PlainEvent:
    pass


class NamedEvent:
    name = "custom-event"


class NamespacedEvent:
    namespace = "payments"


def test_init_stores_config_key():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    assert versioning.config_key == "EVENT_VERSIONING"


def test_get_namespace_returns_class_namespace_when_present():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    result = versioning.get_namespace(NamespacedEvent)

    assert result == "payments"


def test_get_namespace_uses_config_default_namespace_when_class_namespace_missing():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    with patch(
        "volnux.versioning.base.conf.get",
        return_value={"DEFAULT_NAMESPACE": "configured"},
    ):
        result = versioning.get_namespace(PlainEvent)

    assert result == "configured"


def test_get_namespace_falls_back_to_local_when_config_has_no_default_namespace():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    with patch("volnux.versioning.base.conf.get", return_value={}):
        result = versioning.get_namespace(PlainEvent)

    assert result == "local"


def test_get_event_name_returns_class_name_by_default():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    result = versioning.get_event_name(PlainEvent)

    assert result == "PlainEvent"


def test_get_event_name_returns_custom_name_when_present():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    result = versioning.get_event_name(NamedEvent)

    assert result == "custom-event"


def test_is_deprecated_returns_false_by_default():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    assert versioning.is_deprecated(PlainEvent) is False


def test_get_deprecation_info_returns_none_by_default():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    assert versioning.get_deprecation_info(PlainEvent) is None


def test_validate_version_uses_subclass_implementation():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    assert versioning.validate_version("1.0.0") is True
    assert versioning.validate_version("2.0.0") is False


def test_get_version_info_uses_subclass_implementation():
    versioning = DummyVersioning(config_key="EVENT_VERSIONING")

    with patch(
        "volnux.versioning.base.conf.get",
        return_value={"DEFAULT_NAMESPACE": "configured"},
    ):
        info = versioning.get_version_info(PlainEvent)

    assert info["version"] == "1.0.0"
    assert info["namespace"] == "configured"
    assert info["changelog"] is None
    assert info["deprecated"] is False
    assert info["deprecation_info"] is None


def test_default_version_class_attribute_is_exposed():
    assert DummyVersioning.default_version == "1.0.0"


def test_allowed_versions_class_attribute_defaults_to_none():
    assert DummyVersioning.allowed_versions is None
