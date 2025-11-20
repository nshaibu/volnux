import logging

from ply.lex import lex

from volnux.utils import _extend_recursion_depth

logger = logging.getLogger(__name__)


class PointyLexer(object):
    directives = ("recursive-depth",)

    reserved = {"true": "BOOLEAN", "false": "BOOLEAN"}

    tokens = (
        "SEPARATOR",
        "POINTER",
        "PPOINTER",
        "PARALLEL",
        "RETRY",
        "ASSIGN",
        "IDENTIFIER",
        "COMMENT",
        "LPAREN",
        "RPAREN",
        "LBRACKET",
        "RBRACKET",
        "LCURLY_BRACKET",
        "RCURLY_BRACKET",
        "DIRECTIVE",
        "STRING_LITERAL",
        "INT",
        "FLOAT",
        "BOOLEAN",
    )

    t_ignore = " \t"
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_LBRACKET = r"\["
    t_RBRACKET = r"\]"
    t_LCURLY_BRACKET = r"\{"
    t_RCURLY_BRACKET = r"\}"
    t_IDENTIFIER = r"[a-zA-Z_][a-zA-Z0-9_]*"
    t_POINTER = r"\-\>"
    t_PPOINTER = r"\|\-\>"
    t_RETRY = r"\*"
    t_PARALLEL = r"\|\|"
    t_ASSIGN = r"\="
    t_SEPARATOR = r","
    t_ignore_COMMENT = r"\#.*"

    def t_FLOAT(self, t):
        r"[+-]?([0-9]+\.[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?"
        t.value = float(t.value)
        return t

    def t_INT(self, t):
        r"[+-]?[0-9]+"
        t.value = int(t.value)
        return t

    def t_STRING_LITERAL(self, t):
        r'"([^"\\]|\\[ntr"\'\\])*"'  # Matches double-quoted string with escape sequences
        t.value = bytes(t.value[1:-1], "utf-8").decode(
            "unicode_escape"
        )  # Unescape the string
        return t

    def t_BOOLEAN(self, t):
        r"(true|false)"
        t.value = t.value == "true"
        return t

    def t_DIRECTIVE(self, t):
        r"\@[a-zA-Z0-9-]+:{1}[a-zA-Z0-9]+"
        from volnux.utils import _extend_recursion_depth

        value = str(t.value).lstrip("@")
        directive, value = value.split(":")
        if directive in self.directives:
            if value.isnumeric():
                if directive == "recursive-depth":
                    limit = int(value)
                    ret = _extend_recursion_depth(limit)
                    if isinstance(ret, Exception):
                        logger.warning(str(ret))
        t.lexer.skip(1)

    def t_newline(self, t):
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_error(self, t):
        print(f"Illegal character '{t.value[0]}' on line '{t.lexer.lineno}'")
        t.lexer.skip(1)

    def __init__(self, **kwargs):
        self.lexer = lex(module=self, **kwargs)
