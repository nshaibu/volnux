__all__ = ["pointy_parser"]
import logging
import threading
import typing

from ply.yacc import YaccError, yacc

from . import lexer
from .lexer import find_column
from .ast import (
    BinOpNode,
    ConditionalNode,
    DescriptorNode,
    PipelineGroupingNode,
    LiteralNode,
    LiteralType,
    ProgramNode,
    TaskNode,
    VariableDeclNode,
    VariableAccessNode,
    EnvironmentVariableAccessNode,
    DirectiveNode,
    MetaTaskNode,
    ListNode,
    MapNode,
    UnaryOpNode,
    TernaryExprNode,
    ComparisonExprNode,
    NullCoalesceExprNode,
    AttributeNode,
    BranchNode,
    BUILTIN_DESCRIPTORS,
    IndexExprNode,
    RetryNode,
)
from .parser_mode import ParserMode
from .dag_visitor import CycleDetectionVisitor, DAGValidationError, format_cycle_error

logger = logging.getLogger("volnux.parser")

pointy_lexer = lexer.PointyLexer()
tokens = pointy_lexer.tokens

# Name accepted after ``$`` for environment variable access: ``$env.NAME``
ENV_ACCESSOR = "env"

# Variables declared in the program currently being parsed. Reset at the start
# of every ``pointy_parser`` call; access is serialised by ``_parse_lock``.
variables: typing.Dict[str, typing.Any] = {}
# Descriptors written as labels (``A(out_of_balance -> B)``); their values are
# only known once every ``@descriptor`` directive has been seen.
_pending_descriptors: typing.List[typing.Tuple[DescriptorNode, int, int]] = []
_parse_lock = threading.RLock()

# Chain operators are layered in the grammar (``->``/``|->`` bind looser than
# ``||``) and expressions are layered too, so no precedence table is needed.
precedence = ()


def _position(p, index: int) -> typing.Tuple[int, int]:
    """Return the (line, column) of the token at ``p[index]``."""
    lexdata = getattr(p.lexer, "lexdata", "")
    return p.lineno(index), find_column(lexdata, p.lexpos(index))


def p_program(p):
    """
    program : declaration_list
            | declaration_list chain_declaration declaration_list
    """
    chain_expression = p[2] if len(p) == 4 else None
    statements = p[1] + (p[3] if len(p) == 4 else [])

    variable_declarations = {}
    directives = {}
    descriptors: typing.Dict[str, int] = {}

    for statement in statements:
        if isinstance(statement, VariableDeclNode):
            variable_declarations[statement.name] = statement.value
        elif statement.name == "descriptor":
            # Several @descriptor directives accumulate; values are assigned
            # sequentially starting after the built-ins.
            _define_descriptors(statement, descriptors)
            directives["descriptor"] = ListNode(
                [LiteralNode(label, type=LiteralType.STRING) for label in descriptors]
            )
        else:
            # Check for duplicate directives
            if statement.name in directives:
                logger.warning(
                    f"Duplicate directive '@{statement.name}', "
                    f"using last value: {statement.value}"
                )
            directives[statement.name] = statement.value

    _resolve_descriptors(descriptors)

    # Validate DAG mode if specified (invalid values are reported by sema)
    mode = directives.get("mode")
    mode_value = getattr(mode, "value", mode)
    is_dag = isinstance(mode_value, str) and mode_value.upper() == ParserMode.DAG.value
    if is_dag and chain_expression is not None:
        cycle_detector = CycleDetectionVisitor()
        cycle_path = cycle_detector.has_cycle(chain_expression)

        if cycle_path:
            raise DAGValidationError(format_cycle_error(cycle_path))

    p[0] = ProgramNode(
        global_variables=variable_declarations,
        directives=directives,
        chain=chain_expression,
        descriptors=descriptors,
        author=_string_directive(directives.get("author")),
        description=_string_directive(directives.get("description")),
    )


def _string_directive(value) -> typing.Optional[str]:
    """Plain string of a directive value; non-string values are left to sema."""
    if isinstance(value, LiteralNode) and isinstance(value.value, str):
        return value.value
    return None


def _define_descriptors(directive: DirectiveNode, descriptors: typing.Dict[str, int]):
    next_value = len(BUILTIN_DESCRIPTORS) + len(descriptors)
    for item in directive.value.value:
        label = item.value
        if label in descriptors:
            raise YaccError(f"Duplicate descriptor '{label}' in '@descriptor'")
        descriptors[label] = next_value
        next_value += 1


def _resolve_descriptors(descriptors: typing.Dict[str, int]) -> None:
    for node, line, column in _pending_descriptors:
        if node.label not in descriptors:
            raise YaccError(
                f"Unknown descriptor '{node.label}'. Declare it with "
                f"'@descriptor: {node.label}'. Line: {line}, Column: {column}"
            )
        node.value = descriptors[node.label]


def p_declaration_list(p):
    """
    declaration_list : empty
                     | declaration_list declaration
    """
    if len(p) == 2:
        p[0] = []
    else:
        p[0] = p[1] + [p[2]]


def p_declaration(p):
    """
    declaration : variable_declaration
                | directive
    """
    p[0] = p[1]


def p_chain_declaration(p):
    """
    chain_declaration : chain
    """
    p[0] = p[1]


def p_conditional(p):
    """
    conditional : retry LPAREN branch_list RPAREN
    """
    p[0] = ConditionalNode(task=p[1], branches=p[3])


def p_branch_list(p):
    """
    branch_list : branch
                | branch_list SEPARATOR branch
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]


def p_branch(p):
    """
    branch : descriptor POINTER chain
           | descriptor PPOINTER chain
    """
    p[0] = BranchNode(condition=p[1], operator=p[2], task=p[3])


def p_headless_branch_list(p):
    """
    headless_branch_list : builtin_descriptor POINTER chain
                         | builtin_descriptor PPOINTER chain
                         | headless_branch_list SEPARATOR branch
    """
    if len(p) == 4 and isinstance(p[1], DescriptorNode):
        p[0] = [BranchNode(condition=p[1], operator=p[2], task=p[3])]
    else:
        p[0] = p[1] + [p[3]]


def p_chain(p):
    """
    chain : parallel_chain
          | chain POINTER parallel_chain
          | chain PPOINTER parallel_chain
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = BinOpNode(left=p[1], op=p[2], right=p[3])


def p_parallel_chain(p):
    """
    parallel_chain : retry
                   | parallel_chain PARALLEL retry
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = BinOpNode(left=p[1], op=p[2], right=p[3])


def p_retry(p):
    """
    retry : unit
          | unit RETRY INT
          | conditional
    """
    if len(p) != 4:
        p[0] = p[1]
        return

    retry_count = p[3]
    if retry_count < 2:
        line, column = _position(p, 3)
        raise YaccError(
            f"Task cannot be retried less than 2 times. "
            f"Line: {line}, Column: {column}, Offending Token: {retry_count}"
        )
    p[0] = RetryNode(
        job=p[1], attempts=LiteralNode(retry_count, type=LiteralType.NUMBER)
    )


def p_unit(p):
    """
    unit : task
         | meta_task
         | grouped
         | LPAREN chain RPAREN
         | LPAREN headless_branch_list RPAREN
    """
    if len(p) == 2:
        p[0] = p[1]
    elif isinstance(p[2], list):
        # Branches without a subject task; reported by semantic analysis.
        p[0] = ConditionalNode(task=None, branches=p[2])
    else:
        p[0] = p[2]


def p_task(p):
    """
    task : IDENTIFIER attribute_list
         | IDENTIFIER DOUBLE_COLON IDENTIFIER attribute_list
    """
    if len(p) == 3:
        p[0] = TaskNode(task=p[1], options=p[2])
    else:
        p[0] = TaskNode(task=p[3], namespace=p[1], options=p[4])


def p_attribute_list(p):
    """
    attribute_list : LBRACKET attribute_list_items RBRACKET
                   | empty
    """
    if len(p) == 2:
        p[0] = []
    else:
        p[0] = p[2]


def p_attribute_list_items(p):
    """
    attribute_list_items : attribute
                         | attribute_list_items SEPARATOR attribute
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]


def p_attribute(p):
    """
    attribute : IDENTIFIER ASSIGN expression
    """
    p[0] = AttributeNode(attr=p[1], value=p[3])


def p_meta_task(p):
    """
    meta_task : mode LANGLE IDENTIFIER RANGLE attribute_list
              | mode LANGLE IDENTIFIER DOUBLE_COLON IDENTIFIER RANGLE attribute_list

    """
    mode = p[1]

    if len(p) == 6:
        template_event = p[3]
        options = p[5]

        p[0] = MetaTaskNode(mode=mode, template_task=template_event, options=options)
    elif len(p) == 8:
        namespace = p[3]
        template_event = p[5]
        options = p[7]

        p[0] = MetaTaskNode(
            mode=mode,
            template_task=template_event,
            template_event_namespace=namespace,
            options=options,
        )


def p_mode(p):
    """
    mode : MAP
         | FILTER
         | REDUCE
         | FOREACH
         | FLATMAP
         | FANOUT
    """
    p[0] = p[1]


def p_grouped(p):
    """
    grouped : LCURLY_BRACKET chain_list RCURLY_BRACKET attribute_list
    """
    # ``{ A -> B }`` holds one chain; ``{ A, B -> C, D }`` holds several, which the
    # engine runs concurrently and cooperatively (one sub-engine per chain).
    p[0] = PipelineGroupingNode(p[2], options=p[4] or None)


def p_chain_list(p):
    """
    chain_list : chain
               | chain_list SEPARATOR chain
    """
    p[0] = [p[1]] if len(p) == 2 else p[1] + [p[3]]


def p_directive(p):
    """
    directive : VAR_DECL COLON expression
              | VAR_DECL COLON identifier_list
    """
    name, value = p[1], p[3]
    if name == "descriptor":
        if not isinstance(value, ListNode) or not value.value:
            raise YaccError(
                "Directive '@descriptor' expects comma-separated names, "
                "e.g. '@descriptor: out_of_balance, foreign_bank_not_supported'"
            )
    elif isinstance(value, ListNode) and getattr(value, "_identifiers", False):
        raise YaccError(f"Directive '@{name}' expects an expression, not bare names")
    p[0] = DirectiveNode(name=name, value=value)


class _IdentifierList(ListNode):
    """List of bare names; only valid as the value of ``@descriptor``."""

    _identifiers = True


def p_identifier_list(p):
    """
    identifier_list : IDENTIFIER
                    | identifier_list SEPARATOR IDENTIFIER
    """
    label = LiteralNode(p[len(p) - 1], type=LiteralType.STRING)
    p[0] = (
        _IdentifierList([label])
        if len(p) == 2
        else _IdentifierList(p[1].value + [label])
    )


def p_variable_declaration(p):
    """
    variable_declaration : VAR_DECL ASSIGN expression
    """
    var_name = p[1]
    var_value = p[3]

    variables[var_name] = var_value

    p[0] = VariableDeclNode(var_name, var_value)


def p_expression(p):
    """
    expression : coalesce_expression
               | ternary_expression
    """
    p[0] = p[1]


def p_expression_ternary(p):
    """ternary_expression : arithmetic_expr QUESTION expression COLON expression"""
    p[0] = TernaryExprNode(condition=p[1], true_expr=p[3], false_expr=p[5])


def p_coalesce_expression(p):
    """
    coalesce_expression : coalesce_expression NULLCOALESCE operand
                        | operand
    """
    if len(p) == 4:
        p[0] = NullCoalesceExprNode(left=p[1], right=p[3])
    else:
        p[0] = p[1]


def p_operand(p):
    """
    operand : list
            | map
            | arithmetic_expr
            | null_value
    """
    p[0] = p[1]


def p_map(p):
    """
    map : LCURLY_BRACKET map_entries RCURLY_BRACKET
    """
    p[0] = MapNode(p[2])


def p_map_entries(p):
    """
    map_entries : empty
                | map_entry
                | map_entries SEPARATOR map_entry
    """
    if p[1] is None:
        p[0] = {}
    elif len(p) == 2:
        p[0] = p[1]
    elif len(p) == 4:
        p[1].update(p[3])
        p[0] = p[1]


def p_map_entry(p):
    """
    map_entry : STRING_LITERAL COLON expression
    """
    p[0] = {p[1]: p[3]}


def p_list(p):
    """
    list : LBRACKET list_elements RBRACKET
    """
    p[0] = ListNode(p[2])


def p_list_elements(p):
    """
    list_elements : expression
                  | list_elements SEPARATOR expression
                  | empty
    """
    is_empty = p[1] is None
    is_single_item = len(p) == 2
    is_multi_items = len(p) == 4

    if is_empty:
        p[0] = []
    elif is_single_item:
        p[0] = [p[1]]
    elif is_multi_items:
        p[0] = p[1] + [p[3]]


def p_empty(p):
    "empty :"
    pass


def p_null_value(p):
    """
    null_value : NULL
    """
    p[0] = LiteralNode(p[1], type=LiteralType.determine_literal_type(p[1]))


def p_variable_reference(p):
    """
    variable_reference : VAR_ACCESS
                        | VAR_ACCESS DOT IDENTIFIER
                        | VAR_ACCESS LBRACKET expression RBRACKET
    """
    if len(p) == 4:
        accessor = p[1]
        if accessor != ENV_ACCESSOR:
            raise YaccError(f"Unknown variable accessor '{accessor}'")
        p[0] = EnvironmentVariableAccessNode(name=p[3])
    elif len(p) == 5:
        var_name = p[1]
        index_expr = p[3]

        try:
            value = variables[var_name]
        except KeyError:
            raise YaccError(f"Undefined variable '${var_name}'")

        p[0] = IndexExprNode(collection=value, index=index_expr)
    else:
        var_name = p[1]

        try:
            value = variables[var_name]
        except KeyError:
            raise YaccError(f"Undefined variable '${var_name}'")

        p[0] = VariableAccessNode(name=var_name, value=value)


def p_descriptor(p):
    """
    descriptor : builtin_descriptor
               | IDENTIFIER
    """
    if isinstance(p[1], DescriptorNode):
        p[0] = p[1]
        return
    # Custom label: its value is assigned once all directives are parsed.
    node = DescriptorNode(value=None, label=p[1])
    _pending_descriptors.append((node, *_position(p, 1)))
    p[0] = node


def p_builtin_descriptor(p):
    """
    builtin_descriptor : INT
                       | FAILURE
                       | SUCCESS
    """
    if isinstance(p[1], int):
        p[0] = DescriptorNode(p[1])
    else:
        p[0] = DescriptorNode(BUILTIN_DESCRIPTORS[p[1]], label=p[1])


def p_arithmetic_expr(p):
    """
    arithmetic_expr : logical_or_expression
    """
    p[0] = p[1]


def p_logical_or_expression(p):
    """
    logical_or_expression : logical_or_expression PARALLEL logical_and_expression
                          | logical_and_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_logical_and_expression(p):
    """
    logical_and_expression : logical_and_expression LOGICAL_AND bitwise_or_expression
                           | bitwise_or_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_bitwise_or_expression(p):
    """
    bitwise_or_expression : bitwise_or_expression BITWISE_OR bitwise_xor_expression
                          | bitwise_xor_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_bitwise_xor_expression(p):
    """
    bitwise_xor_expression : bitwise_xor_expression BITWISE_XOR bitwise_and_expression
                           | bitwise_and_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_bitwise_and_expression(p):
    """
    bitwise_and_expression : bitwise_and_expression BITWISE_AND arith_comparison_expression
                           | arith_comparison_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_arith_comparison_expression(p):
    """
    arith_comparison_expression : arith_comparison_expression EQ shift_expression
                          | arith_comparison_expression NE shift_expression
                          | arith_comparison_expression LANGLE shift_expression
                          | arith_comparison_expression RANGLE shift_expression
                          | arith_comparison_expression LE shift_expression
                          | arith_comparison_expression GE shift_expression
                          | shift_expression
    """
    if len(p) == 4:
        p[0] = ComparisonExprNode(p[2], p[1], p[3])
    else:
        p[0] = p[1]


def p_shift_expression(p):
    """
    shift_expression : shift_expression LSHL additive_expression
                     | shift_expression LSHR additive_expression
                     | shift_expression ASHR additive_expression
                     | additive_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_additive_expression(p):
    """
    additive_expression : additive_expression PLUS multiplicative_expression
                        | additive_expression MINUS multiplicative_expression
                        | multiplicative_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_multiplicative_expression(p):
    """
    multiplicative_expression : multiplicative_expression DIV unary_expression
                              | multiplicative_expression MOD unary_expression
                              | multiplicative_expression RETRY unary_expression
                              | unary_expression
    """
    if len(p) == 4:
        p[0] = BinOpNode(p[1], p[2], p[3])
    else:
        p[0] = p[1]


def p_unary_expression(p):
    """
    unary_expression : LOGICAL_NOT unary_expression
                     | BITWISE_NOT unary_expression
                     | MINUS unary_expression
                     | primary_expression
    """
    if len(p) == 3:
        p[0] = UnaryOpNode(p[1], p[2])
    else:
        p[0] = p[1]


def p_primary_expression(p):
    """
    primary_expression : LPAREN expression RPAREN
                       | arithmetic_factor
    """
    if len(p) == 4:
        p[0] = p[2]
    else:
        p[0] = p[1]


def p_arithmetic_factor(p):
    """
    arithmetic_factor : INT
                      | FLOAT
                      | BOOLEAN
                      | STRING_LITERAL
                      | variable_reference
    """
    factor = p[1]
    if (
        isinstance(factor, VariableAccessNode)
        or isinstance(factor, EnvironmentVariableAccessNode)
        or isinstance(factor, IndexExprNode)
    ):
        p[0] = factor
    else:
        p[0] = LiteralNode(factor, type=LiteralType.determine_literal_type(factor))


def p_error(p):
    """
    PLY error handler with better error reporting and context.
    """
    if p is None:
        raise SyntaxError("Syntax error: Unexpected end of input!")

    lexdata = getattr(getattr(p, "lexer", None), "lexdata", "")
    line = p.lineno
    column = find_column(lexdata, p.lexpos)

    error_message = (
        f"Syntax error at line {line}, column {column}\n"
        f"Unexpected token: '{p.value}' (type: {p.type})"
    )

    # Add context if lexer is available
    if lexdata:
        try:
            context = get_error_context(lexdata, p.lexpos)
            if context:
                error_message += f"\nContext: {context}"
        except Exception:
            # If context extraction fails, continue without it
            pass

    raise SyntaxError(error_message)


def get_error_context(input_data, error_pos, context_size=50):
    """
    Extract context around the error position for better error reporting.

    Args:
        input_data: The complete input string
        error_pos: Position where error occurred
        context_size: Number of characters to show on each side of error

    Returns:
        String showing context around the error position
    """
    if not input_data or error_pos is None:
        return None

    error_pos = max(0, min(error_pos, len(input_data) - 1))

    start = max(0, error_pos - context_size)
    end = min(len(input_data), error_pos + context_size)

    # Extract context
    context = input_data[start:end]

    # Calculate relative position of error within context
    relative_pos = error_pos - start

    # Create visual indicator
    if relative_pos < len(context):
        context_with_marker = context[:relative_pos] + ">>>" + context[relative_pos:]
    else:
        context_with_marker = context + ">>>"

    # Clean up whitespace for display
    context_lines = context_with_marker.split("\n")
    if len(context_lines) > 3:
        # Show only a few lines around the error
        mid = len(context_lines) // 2
        context_lines = context_lines[max(0, mid - 1) : mid + 2]

    return " ".join(line.strip() for line in context_lines if line.strip())


# Tables are rebuilt in memory on import: nothing is written next to the source.
parser = yacc(debug=False, write_tables=False)


def pointy_parser(code: str) -> ProgramNode:
    # The lexer, parser and ``variables`` are module-level singletons, so
    # parses are serialised and per-parse state is reset on entry.
    with _parse_lock:
        variables.clear()
        _pending_descriptors.clear()
        pointy_lexer.lexer.lineno = 1
        try:
            return parser.parse(code, lexer=pointy_lexer.lexer)
        except YaccError as e:
            raise SyntaxError(f"Parsing error: {str(e)}")
