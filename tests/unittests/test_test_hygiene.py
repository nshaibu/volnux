"""Rules the test suite keeps, because breaking them fails silently.

* An ``async def test_`` that is not marked ``asyncio`` is skipped by pytest (a
  warning, not a failure), so it never checks anything.
* Events are resolved by class name, and every test module registers its events
  globally: two modules defining an event with the same name make one of them run
  the other's code when both are collected in one session.
"""

import ast
from pathlib import Path

import pytest

TESTS = Path(__file__).resolve().parents[1]
FILES = sorted(p for p in TESTS.rglob("*.py") if "__pycache__" not in p.parts)


def _is_asyncio_mark(node):
    if isinstance(node, ast.Call):
        node = node.func
    return isinstance(node, ast.Attribute) and node.attr == "asyncio"


def _marks(value):
    if isinstance(value, (ast.List, ast.Tuple)):
        return any(_is_asyncio_mark(e) for e in value.elts)
    return _is_asyncio_mark(value)


def _sets_pytestmark(body):
    return any(
        isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "pytestmark" for t in n.targets)
        and _marks(n.value)
        for n in body
    )


def _base_names(cls):
    names = []
    for base in cls.bases:
        if isinstance(base, ast.Attribute):
            names.append(base.attr)
        elif isinstance(base, ast.Name):
            names.append(base.id)
    return names


def unmarked_async_tests(path):
    tree = ast.parse(path.read_text())
    module_marked = _sets_pytestmark(tree.body)
    found = []

    def visit(body, covered):
        for node in body:
            if isinstance(node, ast.ClassDef):
                runs_async_natively = "IsolatedAsyncioTestCase" in _base_names(node)
                marked = (
                    covered
                    or runs_async_natively
                    or any(_is_asyncio_mark(d) for d in node.decorator_list)
                    or _sets_pytestmark(node.body)
                )
                visit(node.body, marked)
            elif isinstance(node, ast.AsyncFunctionDef) and node.name.startswith(
                "test"
            ):
                if not (
                    covered or any(_is_asyncio_mark(d) for d in node.decorator_list)
                ):
                    found.append(
                        f"{path.relative_to(TESTS.parent)}:{node.lineno} {node.name}"
                    )

    visit(tree.body, module_marked)
    return found


def test_every_async_test_is_marked():
    unmarked = [item for path in FILES for item in unmarked_async_tests(path)]
    assert not unmarked, (
        "async tests without @pytest.mark.asyncio are skipped, not run:\n  "
        + "\n  ".join(unmarked)
    )


def test_the_scan_recognises_the_ways_of_marking():
    assert _sets_pytestmark(ast.parse("pytestmark = pytest.mark.asyncio").body)
    assert _sets_pytestmark(ast.parse("pytestmark = [pytest.mark.asyncio]").body)
    assert not _sets_pytestmark(ast.parse("x = 1").body)
    assert _is_asyncio_mark(ast.parse("pytest.mark.asyncio", mode="eval").body)
    assert _is_asyncio_mark(ast.parse("pytest.mark.asyncio()", mode="eval").body)
    assert not _is_asyncio_mark(ast.parse("pytest.mark.slow", mode="eval").body)


def _event_subclasses(path):
    tree = ast.parse(path.read_text())
    return [
        node.name
        for node in tree.body
        if isinstance(node, ast.ClassDef) and "EventBase" in _base_names(node)
    ]


def test_event_class_names_are_unique_across_test_modules():
    seen = {}
    for path in FILES:
        for name in _event_subclasses(path):
            seen.setdefault(name, []).append(str(path.relative_to(TESTS.parent)))
    duplicates = {n: files for n, files in seen.items() if len(set(files)) > 1}
    assert (
        not duplicates
    ), "events resolve by class name; give each its own name:\n  " + "\n  ".join(
        f"{n}: {', '.join(f)}" for n, f in duplicates.items()
    )
