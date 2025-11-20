from unittest.mock import MagicMock

import pytest

from volnux.translator.dot import PipeType, generate_dot_from_task_state


def test_generate_dot_from_task_state_simple():
    # Mock PipelineTask and its methods
    mock_task = MagicMock()
    mock_task.get_root.return_value = mock_task
    mock_task.bf_traversal.return_value = iter([mock_task])
    mock_task.get_dot_node_data.return_value = '\t"node1" [label="Task1"]\n'
    mock_task.get_children.return_value = []
    mock_task.parent_node = None
    mock_task.is_parallel_execution_node = False

    # Call the function
    result = generate_dot_from_task_state(mock_task)

    # Assert the DOT output
    expected_output = (
        "digraph G {\n"
        '\tnode [fontname="Helvetica", fontsize=11]\n'
        '\tedge [fontname="Helvetica", fontsize=10]\n'
        '\t"node1" [label="Task1"]\n'
        "}"
    )
    assert result == expected_output


def test_generate_dot_from_task_state_with_children():
    # Mock PipelineTask and its methods
    mock_task = MagicMock()
    child_task = MagicMock()

    mock_task.get_root.return_value = mock_task
    mock_task.bf_traversal.return_value = iter([mock_task, child_task])
    mock_task.get_dot_node_data.return_value = '\t"node1" [label="Task1"]\n'
    mock_task.get_children.return_value = [child_task]
    mock_task.parent_node = None
    mock_task.is_parallel_execution_node = False
    mock_task.id = "node1"
    mock_task.get_id.return_value = "node1"
    mock_task.get_event_name.return_value = "node1"
    mock_task.descriptor = None

    child_task.get_dot_node_data.return_value = '\t"node2" [label="Task2"]\n'
    child_task.get_children.return_value = []
    child_task.parent_node = mock_task
    child_task.is_parallel_execution_node = False
    child_task.id = "node2"
    child_task.get_id.return_value = "node2"
    child_task.get_event_name.return_value = "node2"
    child_task.descriptor = 1

    # Call the function
    result = generate_dot_from_task_state(mock_task)

    # Assert the DOT output
    expected_output = (
        "digraph G {\n"
        '\tnode [fontname="Helvetica", fontsize=11]\n'
        '\tedge [fontname="Helvetica", fontsize=10]\n'
        '\t"node1" [label="Task1"]\n'
        '\t"node2" [label="Task2"]\n'
        '\t"node1" -> "node2" [taillabel="1"]\n'
        "}"
    )
    assert result == expected_output


def test_generate_dot_from_task_state_with_parallel_nodes_as_child():
    # Mock PipelineTask and its methods
    mock_task = MagicMock()
    parallel_task1 = MagicMock()
    parallel_task2 = MagicMock()
    child_task = MagicMock()

    mock_task.get_root.return_value = mock_task
    mock_task.bf_traversal.return_value = iter(
        [mock_task, parallel_task1, parallel_task2, child_task]
    )
    mock_task.get_dot_node_data.return_value = '\t"node1" [label="Task1"]\n'
    mock_task.get_children.return_value = [parallel_task1]
    mock_task.parent_node = None
    mock_task.is_parallel_execution_node = False
    mock_task.id = "node1"
    mock_task.get_id.return_value = "node1"
    mock_task.get_event_name.return_value = "node1"
    mock_task.descriptor = None
    mock_task.condition_node.on_success_event = parallel_task1
    mock_task.condition_node.on_success_pipe = PipeType.PIPE_POINTER

    parallel_task1.get_dot_node_data.return_value = '\t"parallel1" [label="{parallel1|parallel2}", shape=record, style="filled,rounded", fillcolor=lightblue]\n'
    parallel_task1.get_children.return_value = [parallel_task2]
    parallel_task1.parent_node = mock_task
    parallel_task1.is_parallel_execution_node = True
    parallel_task1.id = "parallel1"
    parallel_task1.get_id.return_value = "parallel1"
    parallel_task1.event = "parallel1"
    parallel_task1.get_event_name.return_value = "parallel1"
    parallel_task1.descriptor = None
    parallel_task1.get_parallel_nodes.return_value = [parallel_task1, parallel_task2]
    parallel_task1.condition_node.on_success_event = parallel_task2
    parallel_task1.condition_node.on_success_pipe = PipeType.PARALLELISM

    parallel_task2.get_dot_node_data.return_value = '\t"parallel2" [label="{parallel1|parallel2}", shape=record, style="filled,rounded", fillcolor=lightblue]\n'
    parallel_task2.get_children.return_value = [child_task]
    parallel_task2.parent_node = parallel_task1
    parallel_task2.is_parallel_execution_node = True
    parallel_task2.id = "parallel2"
    parallel_task2.get_id.return_value = "parallel2"
    parallel_task2.event = "parallel2"
    parallel_task2.get_event_name.return_value = "parallel2"
    parallel_task2.descriptor = None
    parallel_task2.get_parallel_nodes.return_value = [parallel_task1, parallel_task2]
    parallel_task2.condition_node.on_success_event = child_task
    parallel_task2.condition_node.on_success_pipe = PipeType.PIPE_POINTER

    child_task.get_dot_node_data.return_value = '\t"node2" [label="Task2"]\n'
    child_task.get_children.return_value = []
    child_task.parent_node = parallel_task2
    child_task.is_parallel_execution_node = False
    child_task.id = "node2"
    child_task.get_id.return_value = "node2"
    child_task.get_event_name.return_value = "node2"
    child_task.descriptor = None

    # Call the function
    result = generate_dot_from_task_state(mock_task)

    # Assert the DOT output
    expected_output = (
        "digraph G {\n"
        '\tnode [fontname="Helvetica", fontsize=11]\n'
        '\tedge [fontname="Helvetica", fontsize=10]\n'
        '\t"node1" [label="Task1"]\n'
        '\t"parallel1" [label="{parallel1|parallel2}", shape=record, style="filled,rounded", fillcolor=lightblue]\n'
        '\t"node2" [label="Task2"]\n'
        '\t"node1" -> "parallel1"\n'
        '\t"parallel1" -> "node2"\n'
        "}"
    )
    assert result == expected_output


@pytest.mark.skip(reason="formatting errors within the dot code generator")
def test_generate_dot_from_task_state_with_parallel_nodes():
    # Mock PipelineTask and its methods
    parallel_task1 = MagicMock()
    parallel_task2 = MagicMock()
    child_task = MagicMock()

    parallel_task1.get_root.return_value = parallel_task1
    parallel_task1.bf_traversal.return_value = iter(
        [parallel_task1, parallel_task2, child_task]
    )
    parallel_task1.get_dot_node_data.return_value = '\t"parallel1" [label="{Parallel1|Parallel2}", shape=record, style="filled,rounded", fillcolor=lightblue]\n'
    parallel_task1.parent_node = None
    parallel_task1.is_parallel_execution_node = True
    parallel_task1.get_parallel_nodes.return_value = [parallel_task1, parallel_task2]
    parallel_task1.descriptor = None
    parallel_task1.event = "Parallel1"
    parallel_task1.id = "parallel1"
    parallel_task1.get_id.return_value = "parallel1"
    parallel_task1.get_event_name.return_value = "parallel1"
    parallel_task1.condition_node.on_success_event = parallel_task2
    parallel_task1.condition_node.on_success_pipe = PipeType.PARALLELISM
    parallel_task1.get_children.return_value = [parallel_task2]

    parallel_task2.event = "Parallel2"
    parallel_task2.id = "parallel2"
    parallel_task2.get_id.return_value = "parallel2"
    parallel_task2.get_event_name.return_value = "parallel2"
    parallel_task2.get_dot_node_data.return_value = '\t"parallel1" [label="{Parallel1|Parallel2}", shape=record, style="filled,rounded", fillcolor=lightblue]\n'
    parallel_task2.condition_node.on_success_event = child_task
    parallel_task2.condition_node.on_success_pipe = PipeType.PIPE_POINTER
    parallel_task2.bf_traversal.return_value = iter([parallel_task1, child_task])
    parallel_task2.parent_node = parallel_task1
    parallel_task2.get_parallel_nodes.return_value = [parallel_task1, parallel_task2]
    parallel_task2.is_parallel_execution_node = True
    parallel_task2.get_children.return_value = [child_task]

    child_task.get_dot_node_data.return_value = '\t"node3" [label="Task3"]\n'
    child_task.get_children.return_value = []
    child_task.parent_node = parallel_task2
    child_task.is_parallel_execution_node = False
    child_task.id = "node3"
    child_task.get_id.return_value = "node3"
    child_task.get_event_name.return_value = "node3"
    child_task.descriptor = None

    # Call the function
    result = generate_dot_from_task_state(parallel_task1)

    # Assert the DOT output
    expected_output = (
        "digraph G {\n"
        '\tnode [fontname="Helvetica", fontsize=11]\n'
        '\tedge [fontname="Helvetica", fontsize=10]\n'
        '\t"parallel1" [label="{Parallel1|Parallel2}", shape=record, style="filled,rounded", fillcolor=lightblue]\n'
        '\t"node3" [label="Task3"]\n'
        '\t"parallel1" -> "node3"\n'
        "}"
    )
    assert result == expected_output


def test_generate_dot_from_task_state_empty():
    # Mock PipelineTask and its methods
    mock_task = MagicMock()
    mock_task.get_root.return_value = None
    mock_task.bf_traversal.return_value = iter([])

    # Call the function
    result = generate_dot_from_task_state(mock_task)

    # Assert the DOT output
    expected_output = (
        "digraph G {\n"
        '\tnode [fontname="Helvetica", fontsize=11]\n'
        '\tedge [fontname="Helvetica", fontsize=10]\n'
        "}"
    )
    assert result == expected_output
