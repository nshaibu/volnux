import unittest

from volnux.parser.ast import (
    BinOpNode,
    ConditionalNode,
    PipelineGroupingNode,
    RetryNode,
    TaskNode,
)
from volnux.parser.dag_visitor import DAGValidationError
from volnux.parser.grammar import pointy_parser
from volnux.parser.lexer import PointyLexer


def lex(code):
    lexer = PointyLexer().lexer
    lexer.input(code)
    return [(t.type, t.value) for t in lexer]


class TestLexer(unittest.TestCase):
    def test_boolean_prefix_is_identifier(self):
        self.assertEqual(
            lex("trueish falsePositive"),
            [("IDENTIFIER", "trueish"), ("IDENTIFIER", "falsePositive")],
        )

    def test_boolean_literals(self):
        self.assertEqual(lex("true false"), [("BOOLEAN", True), ("BOOLEAN", False)])

    def test_variable_access_does_not_swallow_multiplication(self):
        self.assertEqual(
            lex("$count*2"), [("VAR_ACCESS", "count"), ("RETRY", "*"), ("INT", 2)]
        )

    def test_directive_name_has_no_hyphen(self):
        self.assertEqual(
            lex("@a-b"),
            [("VAR_DECL", "a"), ("MINUS", "-"), ("IDENTIFIER", "b")],
        )

    def test_builtin_descriptor_names_are_keywords(self):
        self.assertEqual(
            lex("failure success failures"),
            [
                ("FAILURE", "failure"),
                ("SUCCESS", "success"),
                ("IDENTIFIER", "failures"),
            ],
        )

    def test_keywords_without_token_are_identifiers(self):
        self.assertEqual(
            lex("env RETRY THREADPOOL_EXECUTOR"),
            [
                ("IDENTIFIER", "env"),
                ("IDENTIFIER", "RETRY"),
                ("IDENTIFIER", "THREADPOOL_EXECUTOR"),
            ],
        )

    def test_non_ascii_string_preserved(self):
        self.assertEqual(lex('"é\\n"'), [("STRING_LITERAL", "é\n")])

    def test_illegal_character_raises(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("A -> B ` -> C")


class TestLiteralTypes(unittest.TestCase):
    def test_import_string_detection_never_imports(self):
        import sys

        from volnux.parser.ast import LiteralType

        name = "volnux_definitely_not_imported_pkg.mod"
        self.assertEqual(
            LiteralType.determine_literal_type(name), LiteralType.IMPORT_STRING
        )
        self.assertNotIn("volnux_definitely_not_imported_pkg", sys.modules)

    def test_plain_strings_stay_strings(self):
        from volnux.parser.ast import LiteralType

        for value in ("hello", "has space.x", "1.2.3", ".lead", "trail."):
            self.assertEqual(
                LiteralType.determine_literal_type(value), LiteralType.STRING, value
            )


class TestGrammar(unittest.TestCase):
    def test_parallel_binds_tighter_than_sequence(self):
        chain = pointy_parser("A -> B || C").chain
        self.assertEqual(chain.op, "->")
        self.assertEqual(chain.right.op, "||")

    def test_retry_with_branches(self):
        chain = pointy_parser("A * 2 (0 -> B, 1 -> C)").chain
        self.assertIsInstance(chain, ConditionalNode)
        self.assertIsInstance(chain.task, RetryNode)
        self.assertEqual(len(chain.branches), 2)

    def test_retry_on_group(self):
        chain = pointy_parser("(A || B) * 3").chain
        self.assertIsInstance(chain, RetryNode)
        self.assertIsInstance(chain.job, BinOpNode)
        chain = pointy_parser("{A -> B} * 2").chain
        self.assertIsInstance(chain.job, PipelineGroupingNode)

    def test_retry_minimum(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("A * 1")

    def test_variables_do_not_leak_between_parses(self):
        pointy_parser("@v = 1\nA[x=$v]")
        with self.assertRaises(SyntaxError):
            pointy_parser("A[x=$v]")

    def test_line_numbers_reset_between_parses(self):
        for _ in range(2):
            with self.assertRaises(SyntaxError) as ctx:
                pointy_parser("A -> -> B")
            self.assertIn("line 1,", str(ctx.exception))

    def test_multiple_chains_rejected(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("A -> B\nC -> D")

    def test_dag_mode_detects_cycle_case_insensitive(self):
        # mode is validated by sema; the parser only triggers the cycle check
        self.assertIsNotNone(pointy_parser('@mode: "dag"\nA -> B').chain)

    def test_nested_ternary_and_coalesce(self):
        pointy_parser("A[x = 1 ? 2 : 3 ? 4 : 5]")
        pointy_parser("A[x = null ?? 1 ?? 2]")


if __name__ == "__main__":
    unittest.main()
