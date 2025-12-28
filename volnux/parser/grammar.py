__all__ = ["pointy_parser"]
import logging
import typing
from ply.yacc import YaccError, yacc

from . import lexer
from .ast import (
    AssignmentNode,
    BinOpNode,
    BlockNode,
    BlockType,
    ConditionalNode,
    DescriptorNode,
    ExpressionGroupingNode,
    LiteralNode,
    LiteralType,
    ProgramNode,
    TaskNode,
    VariableDeclNode,
    VariableAccessNode,
    EnvironmentVariableAccessNode,
    DirectiveNode,
    MetaEventNode,
    ListNode,
    MapNode,
    UnaryOpNode,
    ExpressionNode,
    TernaryExprNode,
    ComparisonExprNode,
    NullCoalesceExprNode,
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
    ('left', 'LOGICAL_AND'),
    ('left', 'BITWISE_OR'),
    ('left', 'BITWISE_XOR'),
    ('left', 'BITWISE_AND'),
    ("nonassoc", "EQ", "NE"),  # == !=
    ('nonassoc', 'LANGLE', 'LE', 'RANGLE', 'GE'),              # relational operators
    ('left', 'LSHL', 'LSHR', 'ASHR'),                              # shift operators
    ('left', 'PLUS', 'MINUS'),                                     # additive operators
    ('left', 'MULT', 'DIV', 'MOD'),                                # multiplicative operators
    ('right', 'UMINUS', 'UPLUS', 'LOGICAL_NOT', 'BITWISE_NOT'),    # Unary operators
    ('left', 'LPAREN'),
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
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[2]]


def p_statement(p):
    """
    statement : variable_declaration
                 | expression
                 | directive
    """
    p[0] = p[1]


def p_expression(p):
    """
    expression :  expression POINTER expression
                | expression PPOINTER expression
                | expression PARALLEL expression
                | descriptor POINTER expression
                | descriptor PPOINTER expression
                | factor RETRY task
                | task RETRY factor
                | expression_groupings RETRY factor
                | factor RETRY expression_groupings
                | variable_declaration
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = BinOpNode(left=p[1], op=p[2], right=p[3])


def p_expression_ternary(p):
    """ternary_expression : comparison_expression QUESTION expression COLON expression %prec TERNARY"""
    p[0] = TernaryExprNode(condition=p[1], true_expr=p[3], false_expr=p[5])


def p_expression_comparison(p):
    """
    comparison_expression : expression EQ expression
                  | expression NE expression
                  | expression LANGLE expression
                  | expression RANGLE expression
                  | expression LE expression
                  | expression GE expression
    """
    p[0] = ComparisonExprNode(left=p[1], operator=p[2], right=p[3])


def p_expression_null_coalesce(p):
    """null_coalesce_expression : expression NULLCOALESCE expression"""
    p[0] = NullCoalesceExprNode(left=p[1], right=p[3])


def p_expression_term(p):
    """
    expression : term
    """
    p[0] = p[1]


def p_task(p):
    """
    term : task
        | expression_groupings
        | value
        | variable_reference
    """
    p[0] = p[1]


def p_task_meta_event(p):
    """
    task : meta_event
         | meta_event LBRACKET assigment_expression_group RBRACKET
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        meta_event = p[1]
        meta_event.options = p[3]
        p[0] = meta_event


def p_meta_event(p):
    """
    meta_event : meta_mode LANGLE IDENTIFIER RANGLE
                | meta_mode LANGLE IDENTIFIER DOUBLE_COLON IDENTIFIER RANGLE
    """
    mode = p[1]

    if len(p) == 5:
        template_event = p[3]

        p[0] = MetaEventNode(mode=mode, template_event=template_event)
    else:
        namespace = p[3]
        template_event = p[5]
        p[0] = MetaEventNode(
            mode=mode, template_event=template_event, template_event_namespace=namespace
        )


def p_meta_mode(p):
    """
    meta_mode : MAP
              | FILTER
              | REDUCE
              | FOREACH
              | FLATMAP
              | FANOUT
    """
    p[0] = p[1]


def p_directive(p):
    """directive : VAR_DECL COLON scalar_value"""
    if p[1] == "mode":
        # validate cfg and dag text
        if p[3] in ParserMode.DAG.modes:
            raise YaccError("Unknown parser mode '%s'" % p[3])

    p[0] = DirectiveNode(name=p[1], value=p[3])


def p_variable_declaration(p):
    """
    variable_declaration : VAR_DECL ASSIGN value
    """
    var_name = p[1]
    var_value = p[3]

    variables[var_name] = var_value

    p[0] = VariableDeclNode(var_name, var_value)


def p_value(p):
    """
    value : scalar_value
            | variable_reference
            | list
            | map
            | arithmetic_expression
            | null_value
            | comparison_expression
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
    map_entry : STRING_LITERAL COLON value
    """
    p[0] = {p[1]: p[3]}


def p_list(p):
    """
    list : LBRACKET list_elements RBRACKET
    """
    p[0] = ListNode(p[2])


def p_list_elements(p):
    """
    list_elements : value
                  | list_elements SEPARATOR value
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
    'empty :'
    pass


def p_scalar_value(p):
    """
    scalar_value : INT
                | FLOAT
                | BOOLEAN
                | STRING_LITERAL
    """
    p[0] = LiteralNode(p[1], type=LiteralType.determine_literal_type(p[1]))


def p_null_value(p):
    """
    null_value : NULL
    """
    p[0] = LiteralNode(p[1], type=LiteralType.determine_literal_type(p[1]))


def p_variable_reference(p):
    """
    variable_reference : VAR_ACCESS
                        | VAR_ACCESS DOT IDENTIFIER
    """
    if len(p) == 4:
        accessor = p[1]
        if accessor != pointy_lexer.reserved["env"]:
            raise YaccError(f"Unknown variable accessor '{accessor}'")
        p[0] = EnvironmentVariableAccessNode(name=p[3])
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


def p_factor(p):
    """
    factor : INT
            | FLOAT
            | variable_reference
    """
    factor = p[1]

    if isinstance(factor, VariableAccessNode):
        factor = factor.resolve()
        if not isinstance(factor, (int, float)):
            raise YaccError("Factor cannot be nonnumerical type")

    if p[1] < 2:
        line = p.lineno(1) if hasattr(p, "lineno") else "unknown line"
        column = p.lexpos(1) if hasattr(p, "lexpos") else "unknown column"
        raise YaccError(
            f"Task cannot be retried less than 2 times. "
            f"Line: {line}, Column: {column}, Offending Token: {p[1]}"
        )

    p[0] = LiteralNode(factor, type=LiteralType.determine_literal_type(factor))


def p_scoped_task(p):
    """
    task : task_name
        | IDENTIFIER DOUBLE_COLON task_name
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        task_instance = typing.cast(TaskNode, p[3])
        p[0] = TaskNode(
            task=task_instance.task, options=task_instance.options, namespace=p[1]
        )


def p_task_name(p):
    """
    task_name : IDENTIFIER
        | IDENTIFIER LBRACKET assigment_expression_group RBRACKET
    """
    if len(p) == 2:
        p[0] = TaskNode(task=p[1])
    else:
        p[0] = TaskNode(task=p[1], options=p[3])


def p_conditional_group(p):
    """
    conditional_group : expression SEPARATOR expression
                | conditional_group SEPARATOR expression
    """
    statements = [p[3]]
    if isinstance(p[1], BlockNode):
        statements.extend(p[1].statements)
    else:
        statements.append(p[1])
    p[0] = BlockNode(statements, type=BlockType.CONDITIONAL)


def p_task_conditional_statement(p):
    """
    task :  task LPAREN conditional_group RPAREN
    """
    p[0] = ConditionalNode(p[1], p[3])


def p_assignment_expression(p):
    """
    assignment_expression : IDENTIFIER ASSIGN value
    """
    p[0] = AssignmentNode(p[1], p[3])


def p_assignment_expression_group(p):
    """
    assigment_expression_group : assignment_expression
                                | assignment_expression SEPARATOR assignment_expression
                                | assigment_expression_group SEPARATOR assignment_expression
    """
    if len(p) == 2:
        p[0] = BlockNode([p[1]], type=BlockType.ASSIGNMENT)
    else:
        statements = [p[3]]

        if isinstance(p[1], BlockNode):
            statements.extend(p[1].statements)
        else:
            statements.append(p[1])

        p[0] = BlockNode(statements, type=BlockType.ASSIGNMENT)


def p_expression_groupings(p):
    """
    expression_groupings : LCURLY_BRACKET expression RCURLY_BRACKET
                            | LCURLY_BRACKET expression RCURLY_BRACKET LBRACKET assigment_expression_group RBRACKET
    """
    if len(p) == 4:
        p[0] = ExpressionGroupingNode([p[2]])
    else:
        p[0] = ExpressionGroupingNode([p[2]], options=p[5])

def p_arithmetic_expression(p):
    """
    arithmetic_expression : arithmetic_expression PARALLEL arithmetic_expression
                     | arithmetic_expression LOGICAL_AND arithmetic_expression
                     | arithmetic_expression BITWISE_OR arithmetic_expression
                     | arithmetic_expression BITWISE_XOR arithmetic_expression
                     | arithmetic_expression BITWISE_AND arithmetic_expression
                     | arithmetic_expression EQ arithmetic_expression
                     | arithmetic_expression NE arithmetic_expression
                     | arithmetic_expression LANGLE arithmetic_expression
                     | arithmetic_expression LE arithmetic_expression
                     | arithmetic_expression RANGLE arithmetic_expression
                     | arithmetic_expression GE arithmetic_expression
                     | arithmetic_expression LSHL arithmetic_expression
                     | arithmetic_expression LSHR arithmetic_expression
                     | arithmetic_expression ASHR arithmetic_expression
                     | arithmetic_expression PLUS arithmetic_expression
                     | arithmetic_expression MINUS arithmetic_expression
                     | arithmetic_expression RETRY arithmetic_expression %prec MULT
                     | arithmetic_expression DIV arithmetic_expression
                     | arithmetic_expression MOD arithmetic_expression
                     | LOGICAL_NOT arithmetic_expression
                     | BITWISE_NOT arithmetic_expression
                     | PLUS arithmetic_expression %prec UPLUS
                     | MINUS arithmetic_expression %prec UMINUS
                     | LPAREN arithmetic_expression RPAREN
                     | arithmetic_factor
    """
    if len(p) == 4 and isinstance(p[1], ExpressionNode):
        p[0] = BinOpNode(p[1], p[2], p[3])
    elif len(p) == 3:
        p[0] = UnaryOpNode(p[1], p[2])
    elif len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = p[2]

def p_arithmetic_factor(p):
    """
    arithmetic_factor : INT
                      | FLOAT
                      | variable_reference
    """
    factor = p[1]
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


def pointy_parser(code: str):
    try:
        return parser.parse(code, lexer=pointy_lexer.lexer)
    except YaccError as e:
        raise SyntaxError(f"Parsing error: {str(e)}")
