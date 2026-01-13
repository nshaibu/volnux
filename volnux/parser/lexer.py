import logging

from ply.lex import lex

logger = logging.getLogger(__name__)


class PointyLexer(object):
    directives = ("recursive-depth", "mode")

    reserved = {
        "true": "BOOLEAN",
        "false": "BOOLEAN",
        "RETRY": "RETRY",
        "MAP": "MAP",
        "FILTER": "FILTER",
        "REDUCE": "REDUCE",
        "FOREACH": "FOREACH",
        "FLATMAP": "FLATMAP",
        "FANOUT": "FANOUT",
        "env": "env",
        "null": "NULL",
    }

    builtin_event_token = (
        # Meta events
        "MAP",
        "FILTER",
        "REDUCE",
        "FOREACH",
        "FLATMAP",
        "FANOUT",
    )

    builtins = ("NULL",)

    tokens = (
        (
            "LANGLE",  # <
            "RANGLE",  # >
            "SEPARATOR",
            "COLON",
            "DOT",
            "DOUBLE_COLON",
            "POINTER",
            "PPOINTER",
            "PARALLEL",
            "RETRY",
            "ASSIGN",
            "IDENTIFIER",
            "VAR_DECL",  # @ prefix for declaration
            "VAR_ACCESS",  # $ prefix for variable value access
            "COMMENT",
            "LPAREN",
            "RPAREN",
            "LBRACKET",
            "RBRACKET",
            "LCURLY_BRACKET",
            "RCURLY_BRACKET",
            "STRING_LITERAL",
            "INT",
            "FLOAT",
            "BOOLEAN",
            "COMMA",
            # Operators
            "NULLCOALESCE",  # ??
            "EQ",  # ==
            "NE",  # !=
            "LT",  #
            "GT",  # >
            "LE",  # <=
            "GE",  # >=
            "QUESTION",  # ?
        )
        + builtin_event_token
        + builtins
    )

    t_QUESTION = r"\?"
    t_ignore = " \t"
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_LBRACKET = r"\["
    t_RBRACKET = r"\]"
    t_LCURLY_BRACKET = r"\{"
    t_RCURLY_BRACKET = r"\}"
    t_POINTER = r"\-\>"
    t_PPOINTER = r"\|\-\>"
    t_RETRY = r"\*"
    t_PARALLEL = r"\|\|"
    t_ASSIGN = r"\="
    t_SEPARATOR = r","
    t_DOT = r"\."
    t_COLON = r":"
    t_DOUBLE_COLON = r"::"
    t_LANGLE = r"<"
    t_RANGLE = r">"
    t_COMMA = r","
    t_ignore_COMMENT = r"\#.*"

    def t_LE(self, t):
        r"<="
        return t

    def t_GE(self, t):
        r">="
        return t

    def t_EQ(self, t):
        r"=="
        return t

    def t_NE(self, t):
        r"!="
        return t

    def t_LT(self, t):
        r"<"
        return t

    def t_GT(self, t):
        r">"
        return t

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

    def t_VAR_DECL(self, t):
        r"@[a-zA-Z_*][a-zA-Z0-9_*]*"
        t.value = t.value[1:]  # Remove @ prefix
        return t

    def t_VAR_ACCESS(self, t):
        r"\$[a-zA-Z_*][a-zA-Z0-9_*]*"
        t.value = t.value[1:]  # Remove $ prefix
        return t

    def t_IDENTIFIER(self, t):
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        # Check for reserved keywords
        t.type = self.reserved.get(t.value, "IDENTIFIER")  # type: ignore
        if t.type == "BOOLEAN":
            t.value = t.value == "true"
        return t

    def t_NULLCOALESCE(self, t):
        r"\?\?"
        return t

    def t_newline(self, t):
        r"\n+"
        t.lexer.lineno += len(t.value)

    def t_error(self, t):
        print(f"Illegal character '{t.value[0]}' on line '{t.lexer.lineno}'")
        t.lexer.skip(1)

    def __init__(self, **kwargs):
        self.lexer = lex(module=self, **kwargs)
