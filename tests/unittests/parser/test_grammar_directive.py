import unittest

from volnux.parser.ast import LiteralNode
from volnux.parser.grammar import pointy_parser


class TestDirectiveStatement(unittest.TestCase):
    def test_simple_directive(self):
        program = pointy_parser('@mode:"CFG"')

        self.assertIn("mode", program.directives)
        self.assertIsInstance(program.directives["mode"], LiteralNode)
        self.assertEqual(program.directives["mode"].value, "CFG")

    def test_multiple_directives(self):
        program = pointy_parser('@mode:"CFG" @version:1.0')
        self.assertIn("mode", program.directives)
        self.assertIn("version", program.directives)
        self.assertEqual(program.directives["mode"].value, "CFG")
        self.assertEqual(program.directives["version"].value, 1.0)

    def test_author_and_description_retained(self):
        program = pointy_parser('@author: "Jane"\n@description: "Moves money"\nA -> B')
        self.assertEqual(program.author, "Jane")
        self.assertEqual(program.description, "Moves money")
        self.assertEqual(program.directives["author"].value, "Jane")
        self.assertEqual(program.directives["description"].value, "Moves money")

    def test_author_description_default_to_none(self):
        program = pointy_parser("A")
        self.assertIsNone(program.author)
        self.assertIsNone(program.description)
        self.assertEqual(program.descriptors, {})

    def test_descriptor_directive_assigns_values(self):
        program = pointy_parser(
            "@descriptor: out_of_balance, foreign_bank_not_supported"
        )
        self.assertEqual(
            program.descriptors, {"out_of_balance": 2, "foreign_bank_not_supported": 3}
        )
        self.assertEqual(
            [i.value for i in program.directives["descriptor"].value],
            ["out_of_balance", "foreign_bank_not_supported"],
        )

    def test_multiple_descriptor_directives_accumulate(self):
        program = pointy_parser("@descriptor: a, b\n@descriptor: c")
        self.assertEqual(program.descriptors, {"a": 2, "b": 3, "c": 4})

    def test_duplicate_descriptor_label(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("@descriptor: a, a")
        with self.assertRaises(SyntaxError):
            pointy_parser("@descriptor: a\n@descriptor: a")

    def test_descriptor_directive_requires_names(self):
        for source in ('@descriptor: "a"', "@descriptor: 1", "@descriptor: []"):
            with self.assertRaises(SyntaxError, msg=source):
                pointy_parser(source)

    def test_bare_names_only_valid_for_descriptor(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("@author: jane")

    def test_builtin_names_cannot_be_redefined(self):
        with self.assertRaises(SyntaxError):
            pointy_parser("@descriptor: failure")


if __name__ == "__main__":
    unittest.main()
