import pytest

from volnux.parser import pointy_parser
from volnux.parser.ast import LiteralNode
from volnux.parser.sema import Sema


def lit(node):
    return node.value if isinstance(node, LiteralNode) else node


FULL = """\
@mode: "CFG"
@version: "2.1.0"

@source: hub("trading_events", "trading-events", version="2.1.0")
@source: git("notify", "https://example.com/notify.git", version="1.5.0")
@source: github("alerts", "https://github.com/x/alerts.git", branch="main")
@source: pypi("utils", "shared-utils", version="v3.0.0")
@source: local("shared", "./shared")

@descriptor: needs_review
@trigger: schedule("0 6 * * MON-FRI")

notify::FetchOrders |-> trading_events::ValidateOrder (
    success      -> trading_events::ProcessOrder |-> utils::SendConfirmation,
    needs_review -> shared::ManualReview
)
"""


def messages(source):
    result = Sema().analyse(pointy_parser(source))
    return result, [d.message for d in result.all]


class TestVersion:
    def test_script_version(self):
        assert pointy_parser('@version: "2.1.0"\nA').script_version == "2.1.0"

    def test_absent(self):
        assert pointy_parser("A").script_version is None

    def test_still_a_directive(self):
        assert lit(pointy_parser('@version: "1.0"\nA').directives["version"]) == "1.0"

    def test_known_to_sema(self):
        assert Sema().analyse(pointy_parser('@version: "2.1.0"\nA'))

    @pytest.mark.parametrize(
        "source, level, fragment",
        [
            ("@version: 1.0\nA", "errors", "string literal"),
            ('@version: ""\nA', "errors", "must not be empty"),
            ("@version: $env.V\nA", "errors", "string literal"),
            ('@version: "latest"\nA', "warnings", "does not look like a version"),
        ],
    )
    def test_problems(self, source, level, fragment):
        result = Sema().analyse(pointy_parser(source))
        assert any(fragment in d.message for d in getattr(result, level))

    @pytest.mark.parametrize("v", ["1", "1.2", "v3.0.0", "2.1.0-rc1", "1.0.0+build.5"])
    def test_accepted_forms(self, v):
        assert Sema().analyse(pointy_parser(f'@version: "{v}"\nA'))


class TestSourceDeclarations:
    def test_the_full_example(self):
        program = pointy_parser(FULL)
        assert [s.kind for s in program.sources] == [
            "hub",
            "git",
            "github",
            "pypi",
            "local",
        ]
        hub = program.sources[0]
        assert [lit(a) for a in hub.args] == ["trading_events", "trading-events"]
        assert lit(hub.kwargs["version"]) == "2.1.0"
        assert hub.line == 4 and hub.is_static
        assert program.script_version == "2.1.0"
        assert len(program.triggers) == 1 and program.chain is not None

    def test_sources_are_not_directives(self):
        assert "source" not in pointy_parser(FULL).directives

    def test_full_example_is_clean(self):
        result, msgs = messages(FULL)
        assert result, msgs

    def test_location_as_keyword(self):
        (s,) = pointy_parser('@source: local("shared", location="./x")\nA').sources
        assert [lit(a) for a in s.args] == ["shared"]
        assert lit(s.kwargs["location"]) == "./x"

    def test_env_values_make_it_dynamic(self):
        (s,) = pointy_parser('@source: git("n", "u", token=$env.TOKEN)\nA').sources
        assert not s.is_static

    def test_source_is_still_usable_as_a_name(self):
        assert pointy_parser("source -> B").chain is not None
        assert pointy_parser("@descriptor: source\nA (source -> B)").descriptors

    def test_chain_in_parentheses_right_after_a_source(self):
        program = pointy_parser('@source: local("s", "./s")\n(A -> B)')
        assert len(program.sources) == 1 and program.chain is not None

    @pytest.mark.parametrize(
        "source",
        [
            '@source: hub("a", "b"\nA',
            '@source: hub(version="1", "a")\nA',
            '@source: hub("a", "b", version="1", version="2")\nA',
            "@source: hub\nA",
            "@source:\nA",
            '@source: "hub"\nA',
        ],
    )
    def test_syntax_errors(self, source):
        with pytest.raises(SyntaxError):
            pointy_parser(source)


class TestSourceSemantics:
    @pytest.mark.parametrize(
        "source, fragment",
        [
            ('@source: nope("a", "b")\nA', "Unknown source 'nope'"),
            ('@source: hub("trading-events", "x", version="1")\nA', "plain identifier"),
            ('@source: hub("local", "x", version="1")\nA', "reserved"),
            ('@source: hub("", "x", version="1")\nA', "plain identifier"),
            ('@source: hub(5, "x", version="1")\nA', "must be a string"),
            ('@source: hub("a")\nA', "takes 2 positional"),
            ('@source: hub("a", "x")\nA', "version is required"),
            ('@source: pypi("a", "x")\nA', "version is required"),
            ('@source: hub("a", "x", version=1)\nA', "must be a str"),
            ('@source: hub("a", "x", version="")\nA', "must not be empty"),
            ('@source: hub("a", "")\nA', "must not be empty"),
            ('@source: git("a", "x", timeout=0)\nA', "must be positive"),
            ('@source: git("a", "x", timeout="5")\nA', "must be a int"),
            ('@source: git("a", "x", retries=-1)\nA', "0 or more"),
            ('@source: git("a", "x", bogus=1)\nA', "unknown argument 'bogus'"),
            ('@source: git("a", "x", events=[1])\nA', "list of strings"),
            ('@source: git("a", "x", events="p")\nA', "must be a list"),
            ('@source: local("a", "x", version="1")\nA', "unknown argument 'version'"),
            ('@source: git("a", "x", as_package=1)\nA', "must be a bool"),
            (
                '@source: hub("a", "x", version="1")\n@source: git("a", "y")\nA',
                "duplicate namespace 'a'",
            ),
            ('@source: git("a", "x", location="y")\nA', "location given twice"),
        ],
    )
    def test_errors(self, source, fragment):
        result, msgs = messages(source)
        assert any(fragment in m for m in msgs), msgs

    def test_hyphen_hint(self):
        _, msgs = messages('@source: hub("trading-events", "x", version="1")\nA')
        assert any("trading_events" in m for m in msgs)

    def test_token_literal_is_a_warning(self):
        result, msgs = messages('@source: git("a", "x", token="secret")\nA')
        assert not result.errors
        assert any("$env" in d.message for d in result.warnings)

    def test_token_from_env_is_clean(self):
        assert Sema().analyse(pointy_parser('@source: git("a", "x", token=$env.T)\nA'))

    def test_run_time_values_are_not_checked_early(self):
        assert Sema().analyse(
            pointy_parser('@source: hub("a", $env.PKG, version=$env.V)\nA')
        )

    def test_variables_are_followed(self):
        _, msgs = messages('@v = 5\n@source: hub("a", "x", version=$v)\nA')
        assert any("must be a str" in m for m in msgs)

    def test_undeclared_namespace_is_a_warning_not_an_error(self):
        result, msgs = messages('@source: local("shared", "./s")\ntrading::A')
        assert not result.errors
        assert any("Namespace 'trading' has no '@source" in m for m in msgs)

    def test_scripts_without_sources_are_not_held_to_them(self):
        # such a script registers its namespaces in the workflow configuration
        assert Sema().analyse(pointy_parser("trading::A -> pypi::B"))

    def test_declared_and_local_namespaces_are_clean(self):
        assert Sema().analyse(
            pointy_parser('@source: local("shared", "./s")\nshared::A -> B -> local::C')
        )

    def test_meta_event_template_namespace_counts(self):
        _, msgs = messages('@source: local("s", "./s")\nMAP<other::Template>')
        assert any("Namespace 'other'" in m for m in msgs)
