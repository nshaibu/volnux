__all__ = ["pointy_parser"]

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
)

pointy_lexer = lexer.PointyLexer()
tokens = pointy_lexer.tokens

precedence = (("left", "RETRY", "POINTER", "PPOINTER", "PARALLEL"),)


"""
| expression_groupings POINTER expression
                | expression POINTER expression_groupings
                | descriptor POINTER expression_groupings
                | expression_groupings PPOINTER expression
                | expression PPOINTER expression_groupings
                | descriptor PPOINTER expression_groupings
                | expression_groupings PARALLEL expression_groupings
                | expression_groupings PARALLEL expression
                | expression PARALLEL expression_groupings
"""


def p_program(p):
    """
    program : expression
    """
    p[0] = ProgramNode(p[1])


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
    """
    p[0] = BinOpNode(left=p[1], op=p[2], right=p[3])


def p_expression_term(p):
    """
    expression : term
    """
    p[0] = p[1]


def p_task(p):
    """
    term : task
        | expression_groupings
    """
    p[0] = p[1]


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
    """
    if p[1] < 2:
        line = p.lineno(1) if hasattr(p, "lineno") else "unknown line"
        column = p.lexpos(1) if hasattr(p, "lexpos") else "unknown column"
        raise YaccError(
            f"Task cannot be retried less than 2 times. "
            f"Line: {line}, Column: {column}, Offending Token: {p[1]}"
        )

    p[0] = LiteralNode(p[1], type=LiteralType.determine_literal_type(p[1]))


def p_task_taskname(p):
    """
    task : IDENTIFIER
        | IDENTIFIER LBRACKET assigment_expression_group RBRACKET
    """
    if len(p) == 2:
        p[0] = TaskNode(p[1])
    else:
        p[0] = TaskNode(p[1], p[3])


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
    assignment_expression : IDENTIFIER ASSIGN STRING_LITERAL
                            | IDENTIFIER ASSIGN INT
                            | IDENTIFIER ASSIGN FLOAT
                            | IDENTIFIER ASSIGN BOOLEAN
    """
    p[0] = AssignmentNode(
        p[1], LiteralNode(p[3], type=LiteralType.determine_literal_type(p[3]))
    )


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

    # Ensure error_pos is within bounds
    error_pos = max(0, min(error_pos, len(input_data) - 1))

    # Calculate context boundaries
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
