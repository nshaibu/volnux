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


if __name__ == "__main__":
    unittest.main()
