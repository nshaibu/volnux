from typing import Set, List, Dict, Optional
from ply.yacc import YaccError

from .ast import ASTNode, BinOpNode, ConditionalNode, ExpressionGroupingNode, TaskNode


class DAGValidationError(YaccError):
    """Exception raised when cycle detected in DAG mode"""

    pass


def format_cycle_error(cycle_path: List[str]) -> str:
    """cycle error message"""
    cycle_str = " -> ".join(cycle_path)

    error_msg = f"""
╔═══════════════════════════════════════════════════════════════╗
║              CYCLE DETECTED IN DAG MODE                       ║
╚═══════════════════════════════════════════════════════════════╝

Cycle Path: {cycle_str}

DAG (Directed Acyclic Graph) mode does not allow circular 
dependencies. A cycle means a task can eventually reach itself 
through a chain of dependencies.

Possible Solutions:
  1. Remove the cyclic dependency by restructuring your workflow
  2. Switch to CFG mode if cycles are intentional: @mode:CFG
  3. Break the cycle by removing one of the connections

Example of breaking the cycle:
  Before: {cycle_path[0]} -> ... -> {cycle_path[-2]} -> {cycle_path[-1]}
  After:  {cycle_path[0]} -> ... -> {cycle_path[-2]}
"""
    return error_msg.strip()


class CycleDetectionVisitor:
    """Detects cycles in the workflow graph for DAG mode validation"""

    def __init__(self):
        self.visited: Set[str] = set()
        self.rec_stack: Set[str] = set()
        self.graph: Dict[str, List[str]] = {}
        self.cycle_path: List[str] = []

    def build_graph(self, node: ASTNode, parent: Optional[str] = None):
        """Build adjacency list representation of the workflow graph"""
        if isinstance(node, TaskNode):
            task_name = node.fully_qualified_name

            if task_name not in self.graph:
                self.graph[task_name] = []

            if parent:
                if parent not in self.graph:
                    self.graph[parent] = []
                if task_name not in self.graph[parent]:
                    self.graph[parent].append(task_name)

            return task_name

        elif isinstance(node, BinOpNode):
            left_task = self.build_graph(node.left, parent)

            if node.op == "||":  # PARALLEL
                right_task = self.build_graph(node.right, parent)
                return right_task
            else:  # POINTER (->) or PPOINTER (|->)
                right_task = self.build_graph(node.right, left_task)
                return right_task

        elif isinstance(node, ExpressionGroupingNode):
            last_task = parent
            for expr in node.expressions:
                last_task = self.build_graph(expr, last_task)
            return last_task

        elif isinstance(node, ConditionalNode):
            task_name = self.build_graph(node.task, parent)
            for statement in node.branches.statements:
                self.build_graph(statement, task_name)
            return task_name

        return parent

    def detect_cycle_dfs(self, node: str, path: List[str]) -> bool:
        """
        Detect cycle using DFS with path tracking
        Returns True if cycle found, False otherwise
        """
        self.visited.add(node)
        self.rec_stack.add(node)
        path.append(node)

        # Check all neighbors
        for neighbor in self.graph.get(node, []):
            if neighbor not in self.visited:
                if self.detect_cycle_dfs(neighbor, path.copy()):
                    return True
            elif neighbor in self.rec_stack:
                # Cycle detected! Build the cycle path
                cycle_start = path.index(neighbor)
                self.cycle_path = path[cycle_start:] + [neighbor]
                return True

        self.rec_stack.remove(node)
        return False

    def has_cycle(self, root: ASTNode) -> Optional[List[str]]:
        """
        Check if the workflow graph has cycles
        Returns the cycle path if found, None otherwise
        """
        self.build_graph(root)

        # Run DFS from all unvisited nodes
        for node in self.graph.keys():
            if node not in self.visited:
                if self.detect_cycle_dfs(node, []):
                    return self.cycle_path

        return None
