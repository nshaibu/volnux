"""Event versions are semantic versions (https://semver.org), and nothing else."""

import pytest

from volnux.versioning import (
    SemVer,
    SemanticVersioning,
    is_semver,
    parse_semver,
)


class PlainEvent:
    pass


class VersionedEvent:
    version = "2.1.0"
    changelog = "Added a feature"
    deprecated = False
    namespace = "trading"


class DeprecatedEvent:
    version = "1.4.0"
    deprecated = True
    deprecation_info = {"reason": "use V2", "replacement": "EventV2"}


VALID = [
    "0.0.0",
    "1.0.0",
    "2.1.3",
    "10.20.30",
    "1.0.0-alpha",
    "1.0.0-alpha.1",
    "1.0.0-alpha.beta",
    "1.0.0-rc.1",
    "1.0.0-0.3.7",
    "1.0.0-x.7.z.92",
    "1.0.0+20130313144700",
    "1.0.0-beta+exp.sha.5114f85",
    "1.0.0+21AF26D3-117B344092BD",
]
INVALID = [
    "",
    "1",
    "1.0",
    "v1.0.0",
    "v2",
    "1.0.0.0",
    "01.0.0",
    "1.01.0",
    "1.0.01",
    "1.0.0-",
    "1.0.0-01",
    "1.0.0-alpha..1",
    "1.0.0+",
    "1.0.0 ",
    "latest",
    "not-a-version",
    "1.0.0rc1",
    "1.0.0-rc_1",
    None,
    1.0,
    ("1", "0", "0"),
]


class TestTheRule:
    @pytest.mark.parametrize("version", VALID)
    def test_valid(self, version):
        assert is_semver(version)
        assert SemanticVersioning().validate_version(version)
        assert str(parse_semver(version)) == version

    @pytest.mark.parametrize("version", INVALID)
    def test_invalid(self, version):
        assert not is_semver(version)
        assert not SemanticVersioning().validate_version(version)
        with pytest.raises(ValueError, match="not a semantic version"):
            parse_semver(version)

    def test_the_message_says_what_is_expected(self):
        with pytest.raises(ValueError, match=r"MAJOR\.MINOR\.PATCH"):
            parse_semver("2")

    def test_partial_versions_only_for_constraints(self):
        assert str(parse_semver("2", partial=True)) == "2.0.0"
        assert str(parse_semver("2.1", partial=True)) == "2.1.0"
        assert str(parse_semver("2.1.3", partial=True)) == "2.1.3"
        with pytest.raises(ValueError):
            parse_semver("v2", partial=True)


class TestOrdering:
    ORDER = [
        "0.9.9",
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-rc.1",
        "1.0.0",
        "1.0.1",
        "1.1.0",
        "2.0.0",
        "10.0.0",
    ]

    def test_the_example_of_the_specification(self):
        parsed = [parse_semver(v) for v in self.ORDER]
        assert parsed == sorted(parsed)
        assert sorted(reversed(parsed)) == parsed

    def test_numbers_compare_as_numbers(self):
        assert parse_semver("1.10.0") > parse_semver("1.9.0")
        assert parse_semver("1.0.0-beta.11") > parse_semver("1.0.0-beta.2")

    def test_numeric_identifiers_sort_below_words(self):
        assert parse_semver("1.0.0-1") < parse_semver("1.0.0-a")

    def test_build_metadata_is_ignored(self):
        assert parse_semver("1.0.0+a") == parse_semver("1.0.0+b")
        assert hash(parse_semver("1.0.0+a")) == hash(parse_semver("1.0.0"))

    def test_parts(self):
        v = parse_semver("2.3.4-rc.1+build.5")
        assert (v.major, v.minor, v.patch) == (2, 3, 4)
        assert v.prerelease == ("rc", "1") and v.build == ("build", "5")
        assert isinstance(v, SemVer)

    def test_not_equal_to_other_types(self):
        assert parse_semver("1.0.0") != "1.0.0"


class TestVersionInfo:
    def test_default_version_is_1_0_0(self):
        assert SemanticVersioning.default_version == "1.0.0"

    def test_defaults_when_the_class_says_nothing(self):
        info = SemanticVersioning().get_version_info(PlainEvent)
        assert info == {
            "version": "1.0.0",
            "namespace": "local",
            "changelog": None,
            "deprecated": False,
            "deprecation_info": None,
        }

    def test_uses_the_class_attributes(self):
        info = SemanticVersioning().get_version_info(VersionedEvent)
        assert info["version"] == "2.1.0"
        assert info["namespace"] == "trading"
        assert info["changelog"] == "Added a feature"

    @pytest.mark.parametrize("version", ["not-a-version", "1.0", "v2"])
    def test_a_class_with_a_bad_version_is_refused(self, version):
        bad = type("Bad", (), {"version": version})
        with pytest.raises(ValueError, match="not a semantic version"):
            SemanticVersioning().get_version_info(bad)

    def test_allowed_versions(self):
        class Strict(SemanticVersioning):
            allowed_versions = ["1.0.0", "2.0.0"]

        assert Strict().validate_version("1.0.0")
        assert not Strict().validate_version("3.0.0")
        assert not Strict().validate_version("not-semver")


class TestNamespace:
    def test_the_class_attribute(self):
        assert SemanticVersioning().get_namespace(VersionedEvent) == "trading"

    def test_the_setting_is_the_default(self):
        from volnux.conf import override_settings

        scheme = SemanticVersioning()
        assert scheme.get_namespace(PlainEvent) == "local"
        with override_settings(DEFAULT_EVENT_NAMESPACE="finance"):
            assert scheme.get_namespace(PlainEvent) == "finance"
            assert scheme.get_namespace(VersionedEvent) == "trading"

    def test_event_name(self):
        scheme = SemanticVersioning()
        assert scheme.get_event_name(PlainEvent) == "PlainEvent"
        named = type("Named", (), {"name": "custom"})
        assert scheme.get_event_name(named) == "custom"


class TestDeprecation:
    def test_deprecated(self):
        scheme = SemanticVersioning()
        assert scheme.is_deprecated(DeprecatedEvent) is True
        assert scheme.get_deprecation_info(DeprecatedEvent) == {
            "reason": "use V2",
            "replacement": "EventV2",
        }

    def test_not_deprecated(self):
        scheme = SemanticVersioning()
        assert scheme.is_deprecated(PlainEvent) is False
        assert scheme.get_deprecation_info(PlainEvent) is None


def test_other_schemes_are_gone():
    import volnux.versioning as versioning

    for name in ("NoVersioning", "SimpleVersioning", "BaseVersioning"):
        assert not hasattr(versioning, name)
