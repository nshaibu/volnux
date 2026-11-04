from unittest.mock import MagicMock

import pytest

from volnux.executors import BaseExecutor, DefaultExecutor, ThreadPoolExecutor
from volnux.parser.executor_config import ExecutorInitializerConfig


@pytest.fixture
def registry(monkeypatch):
    fake = MagicMock(name="registry")
    fake.get.return_value = DefaultExecutor
    monkeypatch.setattr("volnux.mixins.event.executor._GLOBAL_EXECUTOR_REGISTRY", fake)
    return fake


class WorkerPool(ThreadPoolExecutor):
    """An executor class that exposes a multiprocessing-style context."""

    @classmethod
    def get_context(cls, method):
        return "ctx:%s" % method


class TestGetTaskExecutor:
    def test_default_label_goes_through_the_registry(self, make_event, registry):
        event_class = make_event()
        assert event_class.executor == "default"
        assert event_class.get_task_executor() is DefaultExecutor
        registry.get.assert_called_once_with("default")

    def test_custom_string_label_goes_through_the_registry(self, make_event, registry):
        registry.get.return_value = WorkerPool
        assert make_event(executor="pool").get_task_executor() is WorkerPool
        registry.get.assert_called_once_with("pool")

    def test_executor_class_is_used_directly(self, make_event, registry):
        """Regression: a class used to reach ``label.lower()`` and raise AttributeError."""
        assert make_event(executor=ThreadPoolExecutor).get_task_executor() is (
            ThreadPoolExecutor
        )
        registry.get.assert_not_called()

    def test_executor_instance_is_used_directly(self, make_event, registry):
        instance = ThreadPoolExecutor(max_workers=1)
        try:
            assert make_event(executor=instance).get_task_executor() is instance
        finally:
            instance.shutdown()
        registry.get.assert_not_called()

    def test_unknown_label_yields_registry_answer(self, make_event, registry):
        registry.get.return_value = None
        assert make_event(executor="missing").get_task_executor() is None

    def test_is_available_on_the_class_without_an_instance(self, make_event, registry):
        assert callable(make_event().get_task_executor)

    def test_subclasses_can_override_the_label(self, make_event, registry):
        parent = make_event(executor="parent")
        child = type("Child%d" % id(parent), (parent,), {"executor": "child"})
        child.get_task_executor()
        registry.get.assert_called_once_with("child")


class TestExecutorInitializerConfig:
    def test_default_config_is_created_and_cached(self, build):
        event = build()
        config = event.get_executor_initializer_config()
        assert isinstance(config, ExecutorInitializerConfig)
        assert event.get_executor_initializer_config() is config

    def test_dict_config_is_converted(self, build):
        event = build({"executor_config": {"max_workers": 3}})
        config = event.get_executor_initializer_config()
        assert isinstance(config, ExecutorInitializerConfig)
        assert config.max_workers == 3

    def test_config_instance_is_returned_as_is(self, build):
        config = ExecutorInitializerConfig(max_workers=2)
        event = build({"executor_config": config})
        assert event.get_executor_initializer_config() is config


class TestExecutorContext:
    def test_non_class_executor_has_no_context(self, build, registry):
        registry.get.return_value = ThreadPoolExecutor(max_workers=1)
        try:
            assert build().get_executor_context() is None
        finally:
            registry.get.return_value.shutdown()

    def test_context_carries_mp_context_and_constructor_params(self, build, registry):
        registry.get.return_value = WorkerPool
        event = build({"executor_config": ExecutorInitializerConfig(max_workers=4)})
        context = event.get_executor_context()
        assert context["mp_context"] == "ctx:spawn"
        assert context["max_workers"] == 4

    def test_extra_context_overrides_computed_values(self, build, registry):
        registry.get.return_value = WorkerPool
        event = build({"executor_config": ExecutorInitializerConfig(max_workers=4)})
        context = event.get_executor_context({"max_workers": 9, "extra": 1})
        assert context["max_workers"] == 9 and context["extra"] == 1

    def test_non_dict_extra_context_is_ignored(self, build, registry):
        registry.get.return_value = WorkerPool
        context = build().get_executor_context(["not", "a", "dict"])
        assert "mp_context" in context

    def test_executor_without_get_context_has_no_mp_context(self, build, registry):
        registry.get.return_value = DefaultExecutor
        context = build().get_executor_context()
        assert "mp_context" not in context
