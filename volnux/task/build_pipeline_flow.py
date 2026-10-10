from typing import Tuple, Optional
from volnux.parser import pointy_parser
from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.exceptions import PointyNotExecutable
from volnux.parser.ast import ProgramNode
from volnux.parser.protocols import TaskType

from .group import PipelineTaskGrouping
from .task import PipelineTask


def is_workflow_executable(code: str) -> bool:
    """
    Check if a pointy code contains executable instructions
    NOTE: it can contain comments and directive, but those are not considered executable
    :param code: The pointy code to check
    :return: bool
    """
    executables = []
    for line in code.splitlines():
        stripped_line = line.strip()
        if stripped_line.startswith("#") or stripped_line.startswith("@"):
            continue
        if line:
            executables.append(line)

    return len(executables) != 0


def build_pipeline_flow_from_pointy_code(
    code: str,
) -> Tuple[Optional[TaskType], ProgramNode]:
    """
    Build a pipeline flow from Pointy code.
    Args:
        code (str): The Pointy code as a string.
    Returns:
        The constructed pipeline flows.
    """
    if not is_workflow_executable(code):
        raise PointyNotExecutable(
            "Does not contain executable pointy script: {}".format(code)
        )

    ast = pointy_parser(code)
    code_generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    code_generator.visit_program(ast)
    return code_generator.generate(), ast
