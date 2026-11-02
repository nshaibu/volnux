import unittest

from volnux.parser.grammar import pointy_parser
from volnux.parser.ast import (
    BinOpNode,
    LiteralNode,
    UnaryOpNode,
    ComparisonExprNode,
)


class ParserArithmeticTests(unittest.TestCase):
    def test_simple_addition(self):
        program = pointy_parser("@foo = 1 + 2")
        expr = program.global_variables["foo"]
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "+")
        self.assertIsInstance(expr.left, LiteralNode)
        self.assertEqual(expr.left.value, 1)
        self.assertIsInstance(expr.right, LiteralNode)
        self.assertEqual(expr.right.value, 2)

    def test_precedence_mul_over_add(self):
        program = pointy_parser("@foo = 1 + 2 * 3")
        expr = program.global_variables["foo"]
        # top-level should be addition
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "+")
        # right side should be multiplication due to precedence
        self.assertIsInstance(expr.right, BinOpNode)
        self.assertEqual(expr.right.op, "*")
        self.assertEqual(expr.right.left.value, 2)
        self.assertEqual(expr.right.right.value, 3)

    def test_unary_minus_and_add(self):
        program = pointy_parser("@foo = -5 + 2")
        expr = program.global_variables["foo"]

        self.assertIsInstance(expr, BinOpNode)
        # left is unary minus applied to 5
        self.assertIsInstance(expr.left, UnaryOpNode)
        self.assertEqual(expr.left.op, "-")
        self.assertIsInstance(expr.left.right, LiteralNode)
        self.assertEqual(expr.left.right.value, 5)
        # right is literal 2
        self.assertIsInstance(expr.right, LiteralNode)
        self.assertEqual(expr.right.value, 2)

    def test_parentheses_change_precedence(self):
        program = pointy_parser("@foo = (1 + 2) * 3")
        expr = program.global_variables["foo"]

        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "*")
        self.assertIsInstance(expr.left, BinOpNode)
        self.assertEqual(expr.left.op, "+")
        self.assertEqual(expr.left.left.value, 1)
        self.assertEqual(expr.left.right.value, 2)
        self.assertEqual(expr.right.value, 3)

    def test_complex_add_sub_mul_div(self):
        program = pointy_parser("@foo = 1 + 2 * 3 - 4 / 2")
        expr = program.global_variables["foo"]

        # ((1 + (2 * 3)) - (4 / 2))
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "-")

        left = expr.left
        self.assertIsInstance(left, BinOpNode)
        self.assertEqual(left.op, "+")
        self.assertEqual(left.left.value, 1)
        self.assertIsInstance(left.right, BinOpNode)
        self.assertEqual(left.right.op, "*")
        self.assertEqual(left.right.left.value, 2)
        self.assertEqual(left.right.right.value, 3)

        right = expr.right
        self.assertIsInstance(right, BinOpNode)
        self.assertEqual(right.op, "/")
        self.assertEqual(right.left.value, 4)
        self.assertEqual(right.right.value, 2)

    def test_shifts_left_right_associative(self):
        program = pointy_parser("@foo = 1 << 2 >> 3")
        expr = program.global_variables["foo"]
        # ((1 << 2) >> 3)
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, ">>")
        self.assertIsInstance(expr.left, BinOpNode)
        self.assertEqual(expr.left.op, "<<")
        self.assertEqual(expr.left.left.value, 1)
        self.assertEqual(expr.left.right.value, 2)
        self.assertEqual(expr.right.value, 3)

    def test_unary_parentheses_bitwise_not(self):
        program = pointy_parser("@foo = -(1 + 2) * ~3")
        expr = program.global_variables["foo"]

        # left is unary minus applied to (1 + 2), multiplied by bitwise not 3
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "*")

        self.assertIsInstance(expr.left, UnaryOpNode)
        self.assertEqual(expr.left.op, "-")
        self.assertIsInstance(expr.left.right, BinOpNode)
        self.assertEqual(expr.left.right.op, "+")
        self.assertEqual(expr.left.right.left.value, 1)
        self.assertEqual(expr.left.right.right.value, 2)

        self.assertIsInstance(expr.right, UnaryOpNode)
        self.assertEqual(expr.right.op, "~")
        self.assertIsInstance(expr.right.right, LiteralNode)
        self.assertEqual(expr.right.right.value, 3)

    def test_comparison_ge(self):
        program = pointy_parser("@foo = 10 >= 5")
        expr = program.global_variables["foo"]

        self.assertIsInstance(expr, ComparisonExprNode)
        self.assertEqual(expr.operator, ">=")
        self.assertIsInstance(expr.left, LiteralNode)
        self.assertEqual(expr.left.value, 10)
        self.assertIsInstance(expr.right, LiteralNode)
        self.assertEqual(expr.right.value, 5)

    def test_logical_and_or(self):
        program = pointy_parser("@foo = 1 && 0 || 2")
        expr = program.global_variables["foo"]

        # Expect top-level OR (||) with left being AND (&&)
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "||")
        self.assertIsInstance(expr.left, BinOpNode)
        self.assertEqual(expr.left.op, "&&")
        self.assertEqual(expr.left.left.value, 1)
        self.assertEqual(expr.left.right.value, 0)
        self.assertIsInstance(expr.right, LiteralNode)
        self.assertEqual(expr.right.value, 2)

    def test_bitwise_ops_combination(self):
        program = pointy_parser("@foo = 1 & 2 | 4 ^ 8")
        expr = program.global_variables["foo"]

        # Top-level should be '|'
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "|")
        # Left of '|' should be '&' combining 1 and 2
        left = expr.left
        self.assertIsInstance(left, BinOpNode)
        self.assertEqual(left.op, "&")
        self.assertEqual(left.left.value, 1)
        self.assertEqual(left.right.value, 2)
        # Right of '|' is '^' combining 4 and 8
        right = expr.right
        self.assertIsInstance(right, BinOpNode)
        self.assertEqual(right.op, "^")
        self.assertEqual(right.left.value, 4)
        self.assertEqual(right.right.value, 8)

    def test_bitwise_not_unary_and(self):
        program = pointy_parser("@foo = ~1 & 2")
        expr = program.global_variables["foo"]

        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "&")
        self.assertIsInstance(expr.left, UnaryOpNode)
        self.assertEqual(expr.left.op, "~")
        self.assertIsInstance(expr.left.right, LiteralNode)
        self.assertEqual(expr.left.right.value, 1)
        self.assertIsInstance(expr.right, LiteralNode)
        self.assertEqual(expr.right.value, 2)

    def test_deeply_nested_expression(self):
        program = pointy_parser("@foo = ((1 + 2) * (3 << (4 - 1))) & ~(6 | 7)")

        expr = program.global_variables["foo"]
        # Top-level &: left is multiplication, right is unary ~ over (6 | 7)
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "&")

        left = expr.left
        self.assertIsInstance(left, BinOpNode)
        self.assertEqual(left.op, "*")
        # left-left: (1 + 2)
        self.assertIsInstance(left.left, BinOpNode)
        self.assertEqual(left.left.op, "+")
        self.assertEqual(left.left.left.value, 1)
        self.assertEqual(left.left.right.value, 2)
        # left-right: (3 << (4 - 1))
        self.assertIsInstance(left.right, BinOpNode)
        self.assertEqual(left.right.op, "<<")
        self.assertEqual(left.right.left.value, 3)
        self.assertIsInstance(left.right.right, BinOpNode)
        self.assertEqual(left.right.right.op, "-")
        self.assertEqual(left.right.right.left.value, 4)
        self.assertEqual(left.right.right.right.value, 1)

        # Right: unary ~ applied to (6 | 7)
        right = expr.right
        self.assertIsInstance(right, UnaryOpNode)
        self.assertEqual(right.op, "~")
        self.assertIsInstance(right.right, BinOpNode)
        self.assertEqual(right.right.op, "|")
        self.assertEqual(right.right.left.value, 6)
        self.assertEqual(right.right.right.value, 7)

    def test_comparison_with_arithmetic(self):
        program = pointy_parser("@foo = 1 + 2 * 3 <= 7")
        expr = program.global_variables["foo"]

        self.assertIsInstance(expr, ComparisonExprNode)
        self.assertEqual(expr.operator, "<=")

        # Left side is arithmetic (1 + (2 * 3))
        left = expr.left
        self.assertIsInstance(left, BinOpNode)
        self.assertEqual(left.op, "+")
        self.assertEqual(left.left.value, 1)
        self.assertIsInstance(left.right, BinOpNode)
        self.assertEqual(left.right.op, "*")
        self.assertEqual(left.right.left.value, 2)
        self.assertEqual(left.right.right.value, 3)
        self.assertEqual(expr.right.value, 7)

        right = expr.right
        self.assertIsInstance(right, LiteralNode)
        self.assertEqual(right.value, 7)

    def test_logical_and_with_comparisons(self):
        program = pointy_parser("@foo = 1 < 2 && 3 >= 2")
        expr = program.global_variables["foo"]

        # Top-level should be logical AND combining two ComparisonExprNode
        self.assertIsInstance(expr, BinOpNode)
        self.assertEqual(expr.op, "&&")
        self.assertIsInstance(expr.left, ComparisonExprNode)
        self.assertEqual(expr.left.operator, "<")
        self.assertEqual(expr.left.left.value, 1)
        self.assertEqual(expr.left.right.value, 2)
        self.assertIsInstance(expr.right, ComparisonExprNode)
        self.assertEqual(expr.right.operator, ">=")
        self.assertEqual(expr.right.left.value, 3)
        self.assertEqual(expr.right.right.value, 2)
