"""Helpers for the Pipeline tests: classes built from inline Pointy, unique names."""

import uuid
from typing import Any, Callable, Dict

import pytest

from volnux import EventBase, Pipeline
from volnux.execution.pipeline.fields import InputDataField


def unique(name: str) -> str:
    return f"{name}{uuid.uuid4().hex[:8]}"


@pytest.fixture
def event_class() -> Callable[..., type]:
    def factory(process: Callable = None, name: str = "Ev", **attrs: Any) -> type:
        async def default(self, *args, **kwargs):
            return True, "done"

        return type(
            unique(name), (EventBase,), {"process": process or default, **attrs}
        )

    return factory


@pytest.fixture
def pipeline_class() -> Callable[..., type]:
    """``pipeline_class(pointy, **fields_and_attrs)`` -> a Pipeline subclass."""

    def factory(pointy: str, name: str = "Pipe", **attrs: Any) -> type:
        namespace: Dict[str, Any] = {
            "Meta": type("Meta", (), {"pointy": pointy}),
            **attrs,
        }
        return type(unique(name), (Pipeline,), namespace)

    return factory
