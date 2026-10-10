from unittest.mock import patch

from volnux.versioning.simple import SimpleVersioning


class PlainEvent:
    pass


class VersionedEvent:
    version = "v2"
    changelog = "Introduced v2 behavior"
    deprecated = True
    deprecation_info = {
        "reason": "Use v3 instead",
        "replacement": "v3",
    }
    namespace = "billing"


class NamespacedEvent:
    namespace = "payments"


def test_default_version_is_v1():
    assert SimpleVersioning.default_version == "v1"


def test_allowed_versions_defaults_to_none():
    assert SimpleVersioning.allowed_versions is None


def test_get_version_info_uses_default_version_when_missing():
    versioning = SimpleVersioning(config_key="EVENT_VERSIONING")

    with patch(
        "volnux.versioning.base.conf.get",
        return_value={"DEFAULT_NAMESPACE": "configured"},
    ):
        info = versioning.get_version_info(PlainEvent)

    assert info["version"] == "v1"
    assert info["namespace"] == "configured"
    assert info["changelog"] is None
    assert info["deprecated"] is False
    assert info["deprecation_info"] is None


def test_get_version_info_uses_class_attributes_when_present():
    versioning = SimpleVersioning(config_key="EVENT_VERSIONING")

    info = versioning.get_version_info(VersionedEvent)

    assert info["version"] == "v2"
    assert info["namespace"] == "billing"
    assert info["changelog"] == "Introduced v2 behavior"
    assert info["deprecated"] is True
    assert info["deprecation_info"] == {
        "reason": "Use v3 instead",
        "replacement": "v3",
    }


def test_get_version_info_uses_class_namespace_when_present():
    versioning = SimpleVersioning(config_key="EVENT_VERSIONING")

    info = versioning.get_version_info(NamespacedEvent)

    assert info["namespace"] == "payments"


def test_get_version_info_falls_back_to_local_namespace():
    versioning = SimpleVersioning(config_key="EVENT_VERSIONING")

    with patch("volnux.versioning.base.conf.get", return_value={}):
        info = versioning.get_version_info(PlainEvent)

    assert info["namespace"] == "local"


def test_validate_version_returns_true_when_allowed_versions_is_none():
    versioning = SimpleVersioning(config_key="EVENT_VERSIONING")

    assert versioning.validate_version("v1") is True
    assert versioning.validate_version("v2") is True
    assert versioning.validate_version("latest") is True
    assert versioning.validate_version("") is True


def test_validate_version_checks_membership_when_allowed_versions_is_set():
    versioning = SimpleVersioning(config_key="EVENT_VERSIONING")
    versioning.allowed_versions = ["v1", "v2", "latest"]

    assert versioning.validate_version("v1") is True
    assert versioning.validate_version("v2") is True
    assert versioning.validate_version("latest") is True
