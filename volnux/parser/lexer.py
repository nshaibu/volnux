import logging
import re

from ply.lex import lex

logger = logging.getLogger(__name__)

_STRING_ESCAPES = {
    "n": "\n",
    "t": "\t",
    "r": "\r",
    '"': '"',
    "'": "'",
    "\\": "\\",
}


def find_column(data: str, lexpos: int) -> int:
    """Return the 1-based column of ``lexpos`` within its line."""
    return lexpos - data.rfind("\n", 0, lexpos)


class PointyLexer(object):
    directives = (
        "mode",
        "trigger",
        "descriptor",
        "source",
        "version",
        "author",
        "description",
    )

    reserved = {
        "true": "BOOLEAN",
        "false": "BOOLEAN",
        # Meta event
        "MAP": "MAP",
        "FILTER": "FILTER",
        "REDUCE": "REDUCE",
        "FOREACH": "FOREACH",
        "FLATMAP": "FLATMAP",
        "FANOUT": "FANOUT",
        # Built-in descriptors (0 and 1)
        "failure": "FAILURE",
        "success": "SUCCESS",
        # Other
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
            "LANGLE",  # < will be used for comparison as well
            "RANGLE",  # > will be used for comparison
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
            "TRIGGER_DECL",  # @trigger (declares when the workflow starts)
            "SOURCE_DECL",  # @source (where a namespace's events come from)
            "COMPONENT_DECL",  # @component (a named, reusable sub-chain)
            "WHEN",  # ``when`` right after ``@trigger:``
            "VAR_ACCESS",  # $ prefix for variable value access
            "LPAREN",
            "RPAREN",
            "LBRACKET",
            "RBRACKET",
            "LCURLY_BRACKET",
            "RCURLY_BRACKET",
            # Type
            "STRING_LITERAL",
            "INT",
            "FLOAT",
            "BOOLEAN",
            "FAILURE",
            "SUCCESS",
            # Operators
            "NULLCOALESCE",  # ??
            "EQ",  # ==
            "NE",  # !=
            "LE",  # <=
            "GE",  # >=
            "QUESTION",  # ?
            "PLUS",
            "MINUS",
            "DIV",
            "MOD",
            "LOGICAL_NOT",
            "LOGICAL_AND",
            "BITWISE_NOT",
            "LSHL",  # Logical shift left
            "LSHR",  # Logical shift right
            "ASHR",  # Arithmetic shift right
            "BITWISE_AND",
            "BITWISE_OR",
            "BITWISE_XOR",
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
    t_PLUS = r"\+"
    t_MINUS = r"-"
    t_DIV = r"/"
    t_MOD = r"%"
    t_LOGICAL_NOT = r"!"
    t_LOGICAL_AND = r"&&"
    t_BITWISE_AND = r"&"
    t_BITWISE_OR = r"\|"
    t_BITWISE_XOR = r"\^"
    t_BITWISE_NOT = r"~"
    t_LSHL = r"<<"
    t_LSHR = r">>"
    t_ASHR = r">>>"
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

    def t_FLOAT(self, t):
        r"([0-9]+\.[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?"
        t.value = float(t.value)
        return t

    def t_INT(self, t):
        r"[0-9]+"
        t.value = int(t.value)
        return t

    def t_STRING_LITERAL(self, t):
        r'"([^"\\]|\\[ntr"\'\\])*"'  # double-quoted string with escape sequences
        raw = t.value[1:-1]
        t.lexer.lineno += raw.count("\n")
        t.value = re.sub(r"\\(.)", lambda m: _STRING_ESCAPES[m.group(1)], raw)
        return t

    def t_VAR_DECL(self, t):
        r"@[a-zA-Z_][a-zA-Z0-9_]*"
        t.value = t.value[1:]  # Remove @ prefix
        self._trigger_head = t.value == "trigger"
        if t.value == "component":
            t.type = "COMPONENT_DECL"
        if t.value == "source":
            # Own token for the same reason as ``@trigger``: ``name(...)`` after
            # a plain directive is ambiguous with a parenthesised chain.
            t.type = "SOURCE_DECL"
        if t.value == "trigger":
            # Its own token: the value of a trigger (``schedule(...)``,
            # ``when ...``) is not an expression, and sharing VAR_DECL's rules
            # made ``name(`` ambiguous with a parenthesised chain that follows.
            t.type = "TRIGGER_DECL"
        return t

    def t_VAR_ACCESS(self, t):
        r"\$[a-zA-Z_][a-zA-Z0-9_]*"
        t.value = t.value[1:]  # Remove $ prefix
        return t

    def t_IDENTIFIER(self, t):
        r"[a-zA-Z_][a-zA-Z0-9_]*"
        # Check for reserved keywords
        t.type = self.reserved.get(t.value, "IDENTIFIER")  # type: ignore
        if self._trigger_head:
            # ``when`` is only a keyword as the first word of a trigger, so it
            # stays usable as a task or descriptor name everywhere else.
            self._trigger_head = False
            if t.value == "when":
                t.type = "WHEN"
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
        raise SyntaxError(
            f"Illegal character {t.value[0]!r} at line {t.lexer.lineno}, "
            f"column {find_column(t.lexer.lexdata, t.lexpos)}"
        )

    def __init__(self, **kwargs):
        self._trigger_head = False
        self.lexer = lex(module=self, **kwargs)

    def reset(self) -> None:
        """Forget the state of a previous (possibly failed) scan."""
        self._trigger_head = False
        self.lexer.lineno = 1
