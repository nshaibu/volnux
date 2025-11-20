from volnux.parser import pointy_parser
from volnux.parser.code_gen import ExecutableASTGenerator

from .group import PipelineTaskGrouping
from .task import PipelineTask


def build_pipeline_flow_from_pointy_code(code: str):
    """
    Build a pipeline flow from Pointy code.
    Args:
        code (str): The Pointy code as a string.
    Returns:
        The constructed pipeline flow.
    """
    ast = pointy_parser(code)
    code_generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    code_generator.visit_program(ast)
    return code_generator.generate()
