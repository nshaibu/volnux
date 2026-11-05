import pytest

from volnux.versioning import SemanticVersioning
from volnux.versioning.handler import VersionHandler


class PlainEvent:
    pass


class VersionedEvent:
    version = "2.0.1"
    changelog = "notes"
    deprecated = True
    deprecation_info = {"reason": "old"}
    namespace = "trading"
    name = "Custom"


def test_the_scheme_is_always_semantic_versioning():
    handler = VersionHandler.from_class(PlainEvent)
    assert type(handler.scheme) is SemanticVersioning


def test_from_class_uses_the_default_version_when_the_class_has_none():
    handler = VersionHandler.from_class(PlainEvent)
    assert handler.version == "1.0.0"
    assert handler.namespace == "local"
    assert handler.class_name == "PlainEvent"
    assert handler.klass is PlainEvent


def test_from_class_reads_the_classes_metadata():
    handler = VersionHandler.from_class(VersionedEvent)
    assert handler.version == "2.0.1"
    assert handler.changelog == "notes"
    assert handler.deprecated is True
    assert handler.deprecation_info == {"reason": "old"}
    assert handler.namespace == "trading"
    assert handler.class_name == "Custom"


def test_get_info_delegates_to_the_scheme():
    info = VersionHandler.from_class(VersionedEvent).get_info()
    assert info["version"] == "2.0.1" and info["namespace"] == "trading"
    assert info["deprecated"] is True


def test_get_info_rejects_a_bad_version():
    bad = type("Bad", (), {"version": "v2"})
    with pytest.raises(ValueError, match="not a semantic version"):
        VersionHandler.from_class(bad).get_info()


def test_is_deprecated_is_the_handler_flag():
    assert VersionHandler.from_class(VersionedEvent).is_deprecated() is True
    assert VersionHandler.from_class(PlainEvent).is_deprecated() is False


@pytest.mark.parametrize(
    "version, valid", [("1.2.3", True), ("1.2", False), ("v1", False)]
)
def test_validate(version, valid):
    handler = VersionHandler.from_class(type("E", (), {"version": version}))
    assert handler.validate() is valid


def test_from_class_takes_no_config_key():
    # the scheme is not configurable any more
    with pytest.raises(TypeError):
        VersionHandler.from_class(PlainEvent, config_key="DEFAULT_EVENT_VERSIONING")
