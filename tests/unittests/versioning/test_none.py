from unittest.mock import patch

from volnux.versioning.none import NoVersioning


class PlainEvent:
    pass


class NamespacedEvent:
    namespace = "payments"


def test_default_version_is_1_0_0():
    assert NoVersioning.default_version == "1.0.0"


def test_get_version_info_returns_default_values():
    versioning = NoVersioning(config_key="EVENT_VERSIONING")

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


def test_get_version_info_uses_class_namespace_when_present():
    versioning = NoVersioning(config_key="EVENT_VERSIONING")

    info = versioning.get_version_info(NamespacedEvent)

    assert info["version"] == "1.0.0"
    assert info["namespace"] == "payments"
    assert info["changelog"] is None
    assert info["deprecated"] is False
    assert info["deprecation_info"] is None


def test_get_version_info_falls_back_to_local_namespace():
    versioning = NoVersioning(config_key="EVENT_VERSIONING")

    with patch("volnux.versioning.base.conf.get", return_value={}):
        info = versioning.get_version_info(PlainEvent)

    assert info["namespace"] == "local"


def test_validate_version_always_returns_true_for_default_version():
    versioning = NoVersioning(config_key="EVENT_VERSIONING")

    assert versioning.validate_version("1.0.0") is True


def test_validate_version_always_returns_true_for_arbitrary_version():
    versioning = NoVersioning(config_key="EVENT_VERSIONING")

    assert versioning.validate_version("v999") is True
    assert versioning.validate_version("anything-at-all") is True
    assert versioning.validate_version("") is True
