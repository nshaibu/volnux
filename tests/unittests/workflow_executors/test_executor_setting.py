"""Which executor runs a workflow is a setting; the old executors are gone."""

import importlib

import pytest

from volnux.conf import global_settings, override_settings
from volnux.config import setting
from volnux.engine.workflows.trigger.executors.asyncio_executor import (
    AsyncTaskWorkflowExecutor,
)
from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor
from volnux.import_utils import import_string


def test_the_default_executor_is_the_async_one():
    path = global_settings.DEFAULT_WORKFLOW_EXECUTOR
    assert import_string(path) is AsyncTaskWorkflowExecutor
    assert setting("DEFAULT_WORKFLOW_EXECUTOR") == path


def test_the_setting_can_name_another_executor():
    with override_settings(
        DEFAULT_WORKFLOW_EXECUTOR="volnux.engine.workflows.trigger.executors.celery.CeleryWorkflowExecutor"
    ):
        path = setting("DEFAULT_WORKFLOW_EXECUTOR")
    assert issubclass(import_string(path), WorkflowExecutor)


def test_the_old_classes_are_gone():
    engine = importlib.import_module("volnux.engine.workflows.trigger.engine")
    assert not hasattr(engine, "WorkflowConfigExecutor")
    assert not hasattr(engine, "BaseWorkflowConfigExecutor")
    base = importlib.import_module("volnux.engine.workflows.trigger.executors.base")
    assert not hasattr(base, "BaseWorkflowConfigExecutor")


def test_the_executors_share_one_base():
    for module, name in (
        ("asyncio_executor", "AsyncTaskWorkflowExecutor"),
        ("celery", "CeleryWorkflowExecutor"),
    ):
        cls = getattr(
            importlib.import_module(
                f"volnux.engine.workflows.trigger.executors.{module}"
            ),
            name,
        )
        assert cls.__mro__[1] is WorkflowExecutor


def test_the_async_module_name_is_a_valid_import():
    # (``async.py`` could not be imported with an import statement)
    importlib.import_module(
        "volnux.engine.workflows.trigger.executors.asyncio_executor"
    )
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("volnux.engine.workflows.trigger.executors.async")


class TestInitialiseWorkflows:
    @pytest.mark.asyncio
    async def test_the_engine_gets_the_configured_executor(self, monkeypatch, tmp_path):
        from unittest.mock import AsyncMock, MagicMock

        from volnux.engine.workflows.trigger import get_trigger_engine
        import volnux.setup as setup

        engine = get_trigger_engine()
        monkeypatch.setattr(engine, "workflow_executor", None)
        registry = MagicMock()
        registry.workflow_count = 0
        registry.is_ready.return_value = True
        registry.load_workflow_configs = AsyncMock()
        monkeypatch.setattr(
            "volnux.engine.workflows.get_workflow_registry", lambda *a, **k: registry
        )
        monkeypatch.setattr(
            "volnux.reporting.ensure_pipeline_reporters_installed", lambda *a, **k: None
        )
        # (the project's logging configuration would disable other tests' loggers)
        monkeypatch.setattr("logging.config.dictConfig", lambda *a, **k: None)

        returned = await setup.initialise_workflows(tmp_path)

        assert returned is engine
        assert isinstance(engine.workflow_executor, AsyncTaskWorkflowExecutor)
        assert engine.get_workflow_registry() is registry

    @pytest.mark.asyncio
    async def test_a_setting_that_is_not_an_executor_is_refused(
        self, monkeypatch, tmp_path
    ):
        from unittest.mock import AsyncMock, MagicMock

        from volnux.engine.workflows.trigger import get_trigger_engine
        import volnux.setup as setup

        monkeypatch.setattr(get_trigger_engine(), "workflow_executor", None)
        registry = MagicMock()
        registry.workflow_count = 0
        registry.is_ready.return_value = True
        registry.load_workflow_configs = AsyncMock()
        monkeypatch.setattr(
            "volnux.engine.workflows.get_workflow_registry", lambda *a, **k: registry
        )
        monkeypatch.setattr(
            "volnux.reporting.ensure_pipeline_reporters_installed", lambda *a, **k: None
        )
        # (the project's logging configuration would disable other tests' loggers)
        monkeypatch.setattr("logging.config.dictConfig", lambda *a, **k: None)
        with override_settings(DEFAULT_WORKFLOW_EXECUTOR="builtins.dict"):
            with pytest.raises(TypeError, match="WorkflowExecutor"):
                await setup.initialise_workflows(tmp_path)
