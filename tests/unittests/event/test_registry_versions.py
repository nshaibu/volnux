"""The event registry orders and matches versions by the rules of semver.org."""

import pytest

from volnux.event.registry import Registry
from volnux.versioning import SemanticVersioning


def registry_with(*versions, name="Thing", namespace="local"):
    registry = Registry()
    for version in versions:
        klass = type(f"{name}_{version.replace('.', '_').replace('-', '_')}", (), {})
        registry.register(
            klass,
            name=name,
            namespace=namespace,
            version=version,
            scheme_handler=SemanticVersioning(),
        )
    return registry


class TestOrdering:
    def test_newest_first(self):
        registry = registry_with("1.0.0", "1.10.0", "1.2.0", "2.0.0")
        assert registry.list_versions("Thing") == ["2.0.0", "1.10.0", "1.2.0", "1.0.0"]

    def test_a_prerelease_is_older_than_its_release(self):
        registry = registry_with("1.0.0", "1.0.0-rc.1", "1.0.0-beta.2", "1.0.0-beta.11")
        assert registry.list_versions("Thing") == [
            "1.0.0",
            "1.0.0-rc.1",
            "1.0.0-beta.11",
            "1.0.0-beta.2",
        ]

    def test_identifiers_that_the_pep_440_parser_would_refuse(self):
        registry = registry_with("1.0.0-alpha.beta", "1.0.0-alpha.1", "1.0.0")
        assert registry.list_versions("Thing")[0] == "1.0.0"
        assert registry.get_latest_version("Thing") == "1.0.0"

    def test_latest_ignores_registration_order(self):
        for order in (("1.0.0", "3.0.0", "2.0.0"), ("3.0.0", "2.0.0", "1.0.0")):
            assert registry_with(*order).get_latest_version("Thing") == "3.0.0"

    def test_a_release_beats_its_prerelease_as_the_latest(self):
        assert (
            registry_with("2.0.0-rc.1", "2.0.0").get_latest_version("Thing") == "2.0.0"
        )
        assert (
            registry_with("2.0.0-rc.1", "1.9.0").get_latest_version("Thing")
            == "2.0.0-rc.1"
        )


class TestConstraints:
    VERSIONS = ("1.0.0", "1.2.3", "1.2.9", "1.3.0", "2.0.0", "2.1.0")

    def version_of(self, registry, constraint):
        entry = registry.get_entry("Thing", "local", constraint)
        return entry.version if entry else None

    def test_exact(self):
        registry = registry_with(*self.VERSIONS)
        assert self.version_of(registry, "1.2.3") == "1.2.3"
        assert self.version_of(registry, "9.9.9") is None

    def test_caret_is_the_same_major(self):
        registry = registry_with(*self.VERSIONS)
        assert self.version_of(registry, "^1.0.0") == "1.3.0"
        assert self.version_of(registry, "^2.0.0") == "2.1.0"
        assert self.version_of(registry, "^1.2.5") == "1.3.0"
        assert self.version_of(registry, "^3.0.0") is None

    def test_tilde_is_the_same_minor(self):
        registry = registry_with(*self.VERSIONS)
        assert self.version_of(registry, "~1.2.3") == "1.2.9"
        assert self.version_of(registry, "~1.3.0") == "1.3.0"
        assert self.version_of(registry, "~1.4.0") is None

    @pytest.mark.parametrize(
        "constraint, expected", [("^2", "2.1.0"), ("~1.2", "1.2.9"), ("^1", "1.3.0")]
    )
    def test_a_constraint_may_leave_out_the_trailing_parts(self, constraint, expected):
        registry = registry_with(*self.VERSIONS)
        assert self.version_of(registry, constraint) == expected

    def test_a_prerelease_satisfies_a_constraint_only_above_its_base(self):
        registry = registry_with("2.0.0-rc.1", "1.9.0")
        # 2.0.0-rc.1 is older than 2.0.0, so it does not satisfy ^2.0.0
        assert self.version_of(registry, "^2.0.0") is None
        assert self.version_of(registry, "^1.0.0") == "1.9.0"

    def test_a_malformed_constraint_is_an_error(self):
        registry = registry_with(*self.VERSIONS)
        with pytest.raises(ValueError, match="not a semantic version"):
            registry.get_entry("Thing", "local", "^latest")


class TestRegistration:
    def test_a_version_that_is_not_semantic_is_refused(self):
        registry = Registry()
        with pytest.raises(ValueError, match="Invalid version format"):
            registry.register(
                type("Bad", (), {}),
                name="Bad",
                namespace="local",
                version="v2",
                scheme_handler=SemanticVersioning(),
            )

    def test_namespaces_are_separate(self):
        a = registry_with("1.0.0", namespace="one")
        assert a.list_versions("Thing", "one") == ["1.0.0"]
        assert a.list_versions("Thing", "two") == []
