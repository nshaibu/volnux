from unittest.mock import patch

import pytest

from volnux.versioning.semantic import SemanticVersioning


class PlainEvent:
    pass


class VersionedEvent:
    version = "2.1.0"
    changelog = "Added new feature"
    deprecated = True
    deprecation_info = {
        "reason": "Use NewVersionedEvent",
        "replacement": "NewVersionedEvent",
    }
    namespace = "payments"


class NonDeprecatedEvent:
    deprecated = False
    deprecation_info = {
        "reason": "should not be returned",
    }


def test_default_version_is_1_0_0():
    assert SemanticVersioning.default_version == "1.0.0"


def test_get_version_info_uses_default_version_when_missing():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

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


def test_get_version_info_uses_class_attributes_when_present():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    info = versioning.get_version_info(VersionedEvent)

    assert info["version"] == "2.1.0"
    assert info["namespace"] == "payments"
    assert info["changelog"] == "Added new feature"
    assert info["deprecated"] is True
    assert info["deprecation_info"] == {
        "reason": "Use NewVersionedEvent",
        "replacement": "NewVersionedEvent",
    }


def test_get_version_info_raises_value_error_when_parse_version_fails():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    class InvalidVersionEvent:
        version = "not-a-version"

    with patch(
        "volnux.versioning.semantic.parse_version",
        side_effect=Exception("bad version"),
    ):
        with pytest.raises(
            ValueError, match="Invalid semantic version 'not-a-version'"
        ):
            versioning.get_version_info(InvalidVersionEvent)


def test_validate_version_returns_true_for_valid_semantic_version():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    assert versioning.validate_version("1.0.0") is True
    assert versioning.validate_version("2.1.3") is True


def test_validate_version_returns_false_when_parse_version_fails():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    with patch(
        "volnux.versioning.semantic.parse_version",
        side_effect=Exception("bad version"),
    ):
        assert versioning.validate_version("not-a-version") is False


def test_validate_version_checks_allowed_versions_when_configured():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")
    versioning.allowed_versions = ["1.0.0", "2.0.0"]

    assert versioning.validate_version("1.0.0") is True
    assert versioning.validate_version("2.0.0") is True
    assert versioning.validate_version("3.0.0") is False


def test_is_deprecated_returns_true_when_class_is_deprecated():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    assert versioning.is_deprecated(VersionedEvent) is True


def test_is_deprecated_returns_false_when_class_is_not_deprecated():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    assert versioning.is_deprecated(PlainEvent) is False
    assert versioning.is_deprecated(NonDeprecatedEvent) is False


def test_get_deprecation_info_returns_info_when_deprecated():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    result = versioning.get_deprecation_info(VersionedEvent)

    assert result == {
        "reason": "Use NewVersionedEvent",
        "replacement": "NewVersionedEvent",
    }


def test_get_deprecation_info_returns_none_when_not_deprecated():
    versioning = SemanticVersioning(config_key="EVENT_VERSIONING")

    assert versioning.get_deprecation_info(PlainEvent) is None
    assert versioning.get_deprecation_info(NonDeprecatedEvent) is None
