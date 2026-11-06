from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.parser.grammar import pointy_parser
from volnux.task import PipelineTask, PipelineTaskGrouping


def build(code):
    generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    generator.visit_program(pointy_parser(code))
    return generator.generate()


def walk(task, out=None):
    """Every task and group reachable from ``task``, once each, in chain order."""
    out = out if out is not None else []
    while task is not None and not any(task is seen for seen in out):
        out.append(task)
        if task.is_grouping:
            for chain in task.chains:
                walk(chain, out)
        cond = task.condition_node
        for descriptor in (0, 1):
            config = cond.get_descriptor_config(descriptor)
            if config is not None and config.task is not None:
                walk(config.task, out)
        task = cond.on_success_event
    return out


def extras(task):
    return dict(task.options.extras) if task.options else None


def by_name(task, name):
    return [t for t in walk(task) if t.get_event_name() == name]
