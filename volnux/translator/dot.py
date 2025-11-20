import typing
from functools import lru_cache
from io import StringIO

from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskType
from volnux.task import PipelineTask, PipelineTaskGrouping


@lru_cache(maxsize=None)
def str_to_number(s: str) -> int:
    """
    Convert a string to a numeric hash by summing digit values and character codes.

    Args:
        s: Input string to convert

    Returns:
        Integer hash value
    """
    return sum(int(ch) if ch.isdigit() else ord(ch) for ch in s)


def process_parallel_nodes(
    parallel_nodes: typing.Deque[TaskType], nodes_list: typing.List[str]
) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    """
    Process a group of parallel execution nodes and return node ID and label.

    Args:
        parallel_nodes: Queue of parallel task nodes
        nodes_list: List to append the generated node definition

    Returns:
        Tuple of (node_id, node_label) or (None, None) if no nodes to process
    """
    if not parallel_nodes:
        return None, None

    node_id = parallel_nodes[0].get_id()
    event_names = [n.get_event_name() for n in parallel_nodes]
    node_label = "{" + "|".join(event_names) + "}"
    node_text = (
        f'\t"{node_id}" [label="{node_label}", '
        f'shape=record, style="filled,rounded", fillcolor=lightblue]\n'
    )

    if node_text not in nodes_list:
        nodes_list.append(node_text)

    return node_id, node_label


def draw_subgraph_from_task_state(task_state: PipelineTaskGrouping) -> str:
    """
    Recursively draw a DOT subgraph from a task grouping state.

    Args:
        task_state: Pipeline task grouping containing chains of tasks

    Returns:
        DOT format string representing the subgraph
    """
    lines = [
        f"subgraph cluster_{str_to_number(task_state.id)} {{\n",
        "\tstyle=filled;\n",
    ]

    for chain in task_state.chains:
        if isinstance(chain, PipelineTask):
            lines.append(generate_dot_from_task_state(chain, is_subgraph=True))
        elif isinstance(chain, PipelineTaskGrouping):
            lines.append(draw_subgraph_from_task_state(chain))

    lines.append("}\n")
    return "".join(lines)


def _create_edge(
    from_id: str, to_id: str, descriptor: typing.Optional[str] = None
) -> str:
    """
    Create a DOT edge string with optional label.

    Args:
        from_id: Source node ID
        to_id: Target node ID
        descriptor: Optional edge label

    Returns:
        Formatted DOT edge string
    """
    edge = f'\t"{from_id}" -> "{to_id}"'
    if descriptor is not None:
        edge += f' [taillabel="{descriptor}"]'
    return edge


def _should_skip_node(node: TaskType) -> bool:
    """
    Determine if a node should be skipped during traversal.

    A node is skipped if its parent is a parallel execution node but the node itself isn't.
    This prevents duplicate processing of nodes already handled by parallel processing.

    Args:
        node: Task node to check

    Returns:
        True if the node should be skipped
    """
    parent = node.parent_node
    return (
        parent
        and parent.condition_node.on_success_pipe == PipeType.PARALLELISM
        and node.condition_node.on_success_pipe != PipeType.PARALLELISM
    )


def _process_parallel_children(
    node: TaskType,
    last_parallel_node: TaskType,
    nodes: typing.List[str],
    edges: typing.List[str],
    parallel_node_id: str,
) -> None:
    """
    Process children of the last parallel node and create edges.

    Args:
        node: Current node being processed
        last_parallel_node: Last node in the parallel execution queue
        nodes: List of node definitions
        edges: List of edge definitions
        parallel_node_id: ID of the parallel node group
    """
    for child in last_parallel_node.get_children():
        edge = _create_edge(parallel_node_id, child.get_id(), child.descriptor)
        if edge not in edges:
            edges.append(edge)


def _process_regular_children(
    node: TaskType, nodes: typing.List[str], edges: typing.List[str]
) -> None:
    """
    Process children of a regular (non-parallel) node and create edges.

    Args:
        node: Current node being processed
        nodes: List of node definitions
        edges: List of edge definitions
    """
    for child in node.get_children():
        if child.is_parallel_execution_node:
            parallel_nodes = child.get_parallel_nodes()
            if not parallel_nodes:
                continue

            node_id, _ = process_parallel_nodes(parallel_nodes, nodes)
            if not node_id:
                continue

            first_node = parallel_nodes[0]
            edge = _create_edge(node.get_id(), node_id, first_node.descriptor)
        else:
            edge = _create_edge(node.get_id(), child.get_id(), child.descriptor)

        if edge not in edges:
            edges.append(edge)


def generate_dot_from_task_state(
    task_state: TaskType, is_subgraph: bool = False
) -> str:
    """
    Generate DOT graph representation from a task state tree.

    Performs breadth-first traversal of the task tree and generates
    a GraphViz DOT format string with proper handling of parallel execution nodes.

    Args:
        task_state: Root task state to visualize
        is_subgraph: Whether this is being rendered as a subgraph

    Returns:
        DOT format string representing the task graph
    """
    root = task_state.get_root()
    nodes: typing.List[str] = []
    edges: typing.List[str] = []
    output = StringIO()

    # Write graph header
    if not is_subgraph:
        output.write("digraph G {\n")
        output.write('\tnode [fontname="Helvetica", fontsize=11]\n')
        output.write('\tedge [fontname="Helvetica", fontsize=10]\n')

    iterator = task_state.bf_traversal(root)

    # Process all nodes via breadth-first traversal
    while True:
        try:
            node = next(iterator)
        except StopIteration:
            break

        # Skip nodes already processed as part of parallel groups
        if _should_skip_node(node):
            continue

        # Add node definition
        text = node.get_dot_node_data()
        if text and text not in nodes:
            nodes.append(text)

        # Handle parallel execution nodes
        if node.is_parallel_execution_node:
            parallel_nodes = node.get_parallel_nodes()
            node_id, _ = process_parallel_nodes(parallel_nodes, nodes)
            if node_id is None:
                continue

            last_node = parallel_nodes[-1]
            _process_parallel_children(node, last_node, nodes, edges, node_id)

            # Reset iterator to continue from last parallel node
            iterator = task_state.bf_traversal(last_node)
        else:
            _process_regular_children(node, nodes, edges)

    # Write nodes and edges
    for node in nodes:
        output.write(node)

    for edge in edges:
        output.write(f"{edge}\n")

    output.write("}")
    return output.getvalue()
