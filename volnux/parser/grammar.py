__all__ = ["pointy_parser"]
import logging, typing
from ply.yacc import YaccError, yacc

from . import lexer
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
    IndexExprNode,
    RetryNode,
)
from .parser_mode import ParserMode
from .dag_visitor import CycleDetectionVisitor, DAGValidationError, format_cycle_error

logger = logging.getLogger("volnux.parser")

pointy_lexer = lexer.PointyLexer()
tokens = pointy_lexer.tokens

variables = {}

precedence = (
    ("left", "RETRY", "POINTER", "PPOINTER", "PARALLEL"),
    ("right", "TERNARY"),
    ("left", "NULLCOALESCE"),  # ??
)


def p_program(p):
    """program : statement_list"""
    variable_declarations = {}
    directives = {}
    chain_expression = None

    for statement in p[1]:
        if isinstance(statement, VariableDeclNode):
            variable_declarations[statement.name] = statement.value
        elif isinstance(statement, DirectiveNode):
            # Check for duplicate directives
            if statement.name in directives:
                logger.warning(
                    f"Duplicate directive '@{statement.name}', "
                    f"using last value: {statement.value}"
                )
            directives[statement.name] = statement.value
        else:
            # Only one chain expression allowed
            if chain_expression is not None:
                raise YaccError("Multiple workflow expressions not allowed")
            chain_expression = statement

    # Validate DAG mode if specified
    mode = directives.get("mode", ParserMode.CFG)
    if mode == ParserMode.DAG and chain_expression is not None:
        cycle_detector = CycleDetectionVisitor()
        cycle_path = cycle_detector.has_cycle(chain_expression)

        if cycle_path:
            raise DAGValidationError(format_cycle_error(cycle_path))

    p[0] = ProgramNode(
        global_variables=variable_declarations,
        directives=directives,
        chain=chain_expression,
    )


def p_statement_list(p):
    """
    statement_list : statement
                   | statement_list statement
                   | empty
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[2]]


def p_statement(p):
    """
    statement : variable_declaration
              | chain_declaration
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
    conditional : meta LPAREN branch_list RPAREN
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = ConditionalNode(task=p[1], branches=p[3])


def p_branch_list(p):
    """
    branch_list : branch
                | branch_list SEPARATOR branch
    """
    if len(p) == 2:
        p[0] = [p[1]]
    elif len(p) == 4:
        if isinstance(p[1], list):
            p[0] = p[1] + [p[3]]
        else:
            p[0] = [p[1], p[3]]
    else:
        p[0] = []  # No branches


def p_branch(p):
    """
    branch : descriptor POINTER chain
           | descriptor PPOINTER chain
    """
    p[0] = BranchNode(condition=p[1], operator=p[2], task=p[3])


def p_chain(p):
    """
    chain : retry
          | chain POINTER retry
          | chain PPOINTER retry
          | chain PARALLEL retry
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = BinOpNode(left=p[1], op=p[2], right=p[3])


def p_meta(p):
    """
    meta : task
         | meta_task
         | grouped
         | conditional
         | LPAREN chain RPAREN
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = p[2]


def p_meta(p):
    """
    meta : task
         | meta_task
         | grouped
         | conditional
         | LPAREN chain RPAREN
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = p[2]


def p_retry(p):
    """
    retry : meta
          | task RETRY INT
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        retry_count = p[3]
        if retry_count < 2:
            line = p.lineno(3) if hasattr(p, "lineno") else "unknown line"
            column = p.lexpos(3) if hasattr(p, "lexpos") else "unknown column"
            raise YaccError(
                f"Task cannot be retried less than 2 times. "
                f"Line: {line}, Column: {column}, Offending Token: {p[3]}"
            )
        p[0] = RetryNode(
            job=p[1], attempts=LiteralNode(retry_count, type=LiteralType.NUMBER)
        )


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
    grouped : LCURLY_BRACKET chain RCURLY_BRACKET
            | LCURLY_BRACKET chain RCURLY_BRACKET attribute_list
    """
    if len(p) == 4:
        p[0] = PipelineGroupingNode([p[2]])
    else:
        p[0] = PipelineGroupingNode([p[2]], options=p[4])


def p_expression_ternary(p):
    """ternary_expression : arithmetic_expr QUESTION expression COLON expression %prec TERNARY"""
    p[0] = TernaryExprNode(condition=p[1], true_expr=p[3], false_expr=p[5])


def p_expression_null_coalesce(p):
    """null_coalesce_expression : expression NULLCOALESCE expression"""
    p[0] = NullCoalesceExprNode(left=p[1], right=p[3])


def p_directive(p):
    """directive : VAR_DECL COLON expression"""
    if p[1] == "mode":
        # validate cfg and dag text
        if p[3] in ParserMode.DAG.modes:
            raise YaccError("Unknown parser mode '%s'" % p[3])

    p[0] = DirectiveNode(name=p[1], value=p[3])


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
    expression : list
               | map
               | arithmetic_expr
               | null_value
               | ternary_expression
               | null_coalesce_expression
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
        if accessor != pointy_lexer.reserved["env"]:
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
    descriptor : INT
    """
    if 0 <= p[1] < 10:
        p[0] = DescriptorNode(p[1])
    else:
        line = p.lineno(1) if hasattr(p, "lineno") else "unknown line"
        column = p.lexpos(1) if hasattr(p, "lexpos") else "unknown column"
        raise YaccError(
            f"Descriptors cannot be either greater 9 or less than 0. "
            f"Line: {line}, Column: {column}, Offending token: {p[1]}"
        )


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

    # Extract error position information
    line = getattr(p, "lineno", "unknown")
    column = getattr(p, "lexpos", "unknown")
    token_value = getattr(p, "value", "unknown token")
    token_type = getattr(p, "type", "unknown type")

    # Build basic error message
    error_message = (
        f"Syntax error at line {line}, column {column}\n"
        f"Unexpected token: '{token_value}' (type: {token_type})"
    )

    # Add context if lexer is available
    if hasattr(p, "lexer") and p.lexer and hasattr(p.lexer, "lexdata"):
        try:
            context = get_error_context(p.lexer.lexdata, p.lexpos)
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


parser = yacc()


def pointy_parser(code: str) -> ProgramNode:
    try:
        return parser.parse(code, lexer=pointy_lexer.lexer)
    except YaccError as e:
        raise SyntaxError(f"Parsing error: {str(e)}")
